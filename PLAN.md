# js-udx — porting plan

Port the UDX protocol (wire **v3**) to TypeScript for Node.js, plus a js-libp2p
transport. Reference implementations: `../go-udx` (wire authority) and
`../dart-udx` (feature-parity reference, closest concurrency model to JS).

## Decisions

| Topic | Decision |
|---|---|
| Runtime | Node ≥ 20 (`node:dgram`). No browser support — browsers have no raw UDP. |
| Language / tooling | TypeScript 5.9 (typescript-eslint doesn't support TS 7 yet), ESM, vitest 4 (vitest 5 needs Node ≥ 22.12), eslint 10. Node 20 is past EOL; raise the floor to 22 once the dev machine moves. |
| Layout | npm workspaces: `packages/udx` (protocol, no libp2p deps) and `packages/libp2p-udx` (transport). |
| js-libp2p target | Latest stable at scaffold time: `libp2p@3.x`, `@libp2p/interface@3.x`, `@multiformats/multiaddr@13.x`. |
| Scope | **Full Dart parity**: PMTUD, path migration, anti-amplification, connection flow-control enforcement, version negotiation, stateless reset — in addition to everything go-udx wires. Parity features must never break interop with Go (see "Parity vs Go" below). |
| Wire authority | **go-udx**. Frame type numbers follow Go's table (Dart's enum is off by one from 0x0c up — Dart bug, to be fixed upstream). |
| u64 fields | Plain JS `number` (safe to 2^53), encoded as two u32 halves; no BigInt on the hot path. |

## Wire format (v3) — summary

All integers big-endian. No checksum, no encryption (Noise sits above).

Header: `version u32 | dcidLen u8 | dcid (0..20) | scidLen u8 | scid | seq u32 | dstStreamId u32 | srcStreamId u32 | frames…`
(min 18 bytes; 34 with default 8-byte CIDs). Drop any packet whose version ≠ 3.
Parse frames to end of datagram; an unknown type or truncation rejects the whole packet.

| Type | Frame | Body |
|---|---|---|
| 0x00 | PADDING | — |
| 0x01 | PING | — |
| 0x02 | ACK | largest u32, delayMs u16, rangeCount u8, firstRange u32, n × {gap u8, len u32} |
| 0x03 | STREAM | flags u8 (FIN=0x01, SYN=0x02), offset u64, len u16, data |
| 0x04 | WINDOW_UPDATE | u32 absolute stream offset limit, mod 2^32 |
| 0x05 | MAX_DATA | u64 |
| 0x06 | RESET_STREAM | errorCode u32 |
| 0x07 | MAX_STREAMS | u32 |
| 0x08 | MTU_PROBE | zero padding to end of datagram |
| 0x09 / 0x0a | PATH_CHALLENGE / PATH_RESPONSE | 8 bytes |
| 0x0b | CONNECTION_CLOSE | code u32, frameType u32, reasonLen u16, reason |
| 0x0c | *(unused — parse error)* | |
| 0x0d | STOP_SENDING | streamId u32, code u32 |
| 0x0e | DATA_BLOCKED | u64 |
| 0x0f | STREAM_DATA_BLOCKED | streamId u32, limit u64 |
| 0x10 | NEW_CONNECTION_ID | seq u64, retirePriorTo u64, cidLen u8, cid, resetToken[16] |
| 0x11 | RETIRE_CONNECTION_ID | seq u64 |

ACK ranges are raw counts (not QUIC minus-one): `cursor = L − first`; per range
`end = cursor − gap`, ack `end … end−len+1`, `cursor = end − len`. At most 5 extra ranges.

### Protocol rules that bite
- Only data-bearing packets (STREAM with data, SYN or FIN) consume a sequence number,
  are tracked, retransmitted and ACKed. All control packets go out with seq 0 and are never ACKed.
- Retransmit = same frames under a **fresh** seq; tracking re-keyed, sentTime reset, no
  inflight re-charge, **no retry cap**. Idle timeout (max(30 s, 3×RTO), silent close, code 6) ends dead paths.
- ACK immediately on out-of-order, SYN/FIN, or ≥2 pending; else after clamp(SRTT/4, 1 ms, 25 ms).
- Loss: gap seq is lost if `largest − seq ≥ 3` or age ≥ max(SRTT, latestRTT)×9/8; skip if resent within last RTO.
- RTO = SRTT + 4·RTTVAR clamped [200, 5000] ms (initial 300); backoff min(rto·2^(k−1), max(2 s, rto)).
- Connection setup: dialer picks both 8-byte CIDs, sends STREAM{SYN} on stream ids 0/0, seq 0.
  Acceptor creates the connection on an unknown DCID carrying SYN (localCid = pkt.dcid). No-SYN
  packets for unknown CIDs are buffered (256 CIDs × 32 packets).
- Stream ids are local: dialer odd, acceptor even. Opener never learns the peer's id → sends dst=0
  forever. Resolve by `streams[dst]`, then by `remoteId == src`. Data (or SYN) with src≠0 opens a
  stream; bare FIN/RESET does not. First Write carries SYN.
- FIN offset = final size; EOF only once delivered bytes reach it. Out-of-order backlog > 8 MB → reset (code 3).
- Stream flow control: windows start 64 KiB, receiver doubles up to 4 MiB when credit < window/2,
  anchored to bytes **consumed**. Sender reconstructs the 64-bit limit nearest its current one mod 2^32.
  Blocked sender sends STREAM_DATA_BLOCKED every 500 ms (WINDOW_UPDATE is not retransmitted).
- CUBIC: initial cwnd 14720, min 2944, ssthresh 65535, β 0.7, C 0.4, MSS 1472. Pacing 2.88·cwnd/minRTT.
- Payload per STREAM frame: 1372 bytes (Go). Receive buffer ≥ 1600.

### Parity vs Go (features Go declares but doesn't wire)
Implement them, but keep Go interop intact:
- **Version negotiation**: reply to an unsupported version pre-handshake (Go drops version-0 packets, harmless).
- **Anti-amplification**: server side only, until the address is validated (Dart behaviour).
- **Connection flow control**: send MAX_DATA as Dart does; enforce on send only if the peer
  advertises MAX_DATA (Go does — 1 MiB, replies to DATA_BLOCKED). Verify against Go before enabling by default.
- **PMTUD**: binary search 1280–1500; must not raise payload beyond what Go's 1600-byte receive buffer accepts.
- **Migration / stateless reset / NEW_CONNECTION_ID**: implement per Dart; Go ignores these frames.

## Phases

0. **Scaffold** — workspaces, tsconfig, vitest, lint, CI script.
1. **Codec** — packet header + all frames. Golden vectors produced by a small Go program
   (`tools/gen-vectors`) checked byte-for-byte. Port Dart `frame_test`, `udx_test`, `cid_test`.
2. **Reliability engine (socket-free, injectable clock)** — ACK tracker, packet manager,
   RTT estimator, CUBIC, pacer. One loss/RTO timer per connection rather than per packet.
   Port Go `loss_detection_test`, `retransmit_recovery_test`, `ack_policy_test`, `congestion_test`.
3. **Connection / stream / multiplexer** — dgram socket, CID routing, stream id resolution,
   flow control, half-close, reset, idle watchdog. Stream API: async-iterable source + write/drain
   with backpressure counted at consumption. Port Dart `half_close`, `flow_control`,
   `stream_initiation`, `multiplexer_handshake`, `sequence_number_desync` tests.
4. **Parity features** — version negotiation, anti-amplification, connection flow control,
   PMTUD, migration, stateless reset.
5. **UDX interop** — `bin/bulk-peer` matching Dart's `bulk_peer.dart` CLI/stderr protocol
   (`READY`/`PROGRESS`/`RESULT`/`CORRUPT`/`WINDOW`) so `go-udx/interop` tests can drive JS;
   JS-side tests that spawn Go peers. Later: netem Docker matrix.
6. **libp2p transport** — register multiaddr `udx` (0x0300, size 0); Transport/Listener;
   MultiaddrConnection over the dialer's first UDX stream, handed to the upgrader
   (Noise + Yamux). One shared dial socket per address family (as Go). Note: Dart listener
   de-dups sessions by remote ip:port.
7. **libp2p interop** — Go `dart-libp2p/interop/go-peer --transport=udx`, Dart
   `interop_echo_server` / `interop_echo_client` (`READY <port> <peerid>` on stderr):
   identify, echo, ping.

## Upstream issues found (not in scope here)
- Dart frame enum numbering diverges from Go for 0x0c–0x11.
- `go-udx/cmd/interop-server` stamps version 2 on replies (dropped by v3 peers); Dart raw interop test asserts v2.
- `go-udx/doc/PENDING_WORK.md` §5 says Dart uses UDX streams as the libp2p muxer; dart-libp2p's swarm actually always upgrades over one stream.
- Dart ACK frame declares ECN fields in its length but never encodes them; u8 ACK gaps > 255 truncate.
