# js-udx — porting plan

Port the UDX protocol (wire **v3**) to TypeScript for Node.js, plus a js-libp2p
transport. Reference implementations: `../go-udx` (wire authority) and
`../dart-udx` (feature-parity reference, closest concurrency model to JS).

## Decisions

| Topic | Decision |
|---|---|
| Runtime | `udx`: Node ≥ 20 (`node:dgram`). `libp2p-udx`: Node ≥ 22, because js-libp2p v3 (`@libp2p/utils`) uses `Promise.withResolvers`. `.nvmrc` pins 22 for development. No browser support — browsers have no raw UDP. |
| Language / tooling | TypeScript 5.9 (typescript-eslint doesn't support TS 7 yet), ESM, vitest 4 (vitest 5 needs Node ≥ 22.12), eslint 10. |
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

### Parity features (Phase 4): what was built and why it differs from Dart

dart-udx implements all six, but several don't work there, so js-udx follows
the intent and keeps both peers' wire behaviour working:

| Feature | js-udx | dart-udx | go-udx |
|---|---|---|---|
| Version negotiation | Answers an unknown version for an unknown CID (never larger than the request, ≤10/s), advertising only v3. A dialer answered without v3 before hearing from the peer closes. | Answers pre-handshake, advertises [3,2,1] | Format only |
| Anti-amplification | Acceptor sends ≤3× received until a PATH_CHALLENGE it sent is answered (Go and Dart both answer). Retried a few times; a late answer still validates. Option `antiAmplification`. | Validates on the 2nd packet or 1000 bytes, which proves nothing | Disabled |
| Path migration | Packets from a new address → PATH_CHALLENGE there; move (and restart PMTUD) when it answers. `migrate` event. | Same | Never follows the peer |
| PMTUD | Binary search from go-udx's size (1418 with 8-byte CIDs) up to 1472 (v4) / 1452 (v6). Probes are PING + MTU_PROBE under a fresh, nonzero, untracked sequence. | Probes are MTU_PROBE only, which Dart never ACKs, so it never rises | Not wired |
| Connection flow control | MAX_DATA caps bytes **in flight** (Dart's meaning), 1 MiB until the peer says more. We advertise 16 MiB in the SYN (reliable) or the acceptor's first packet, send DATA_BLOCKED at most once per RTO, and answer it with our MAX_DATA. | In-flight cap, fixed 1 MiB | Tracked, not enforced |
| Stateless reset | Opt-in `statelessResetSecret`. Tokens sent in NEW_CONNECTION_ID; packets for a CID unclaimed for 3 s get a reset (smaller than the trigger, rate-limited). | API only, tokens never exchanged | None |

PING rule: a packet with a PING under a nonzero sequence is acknowledged at
once (Dart's `ping()` expects that; it makes probes and our own `ping()` work).
go-udx sends PINGs as sequence 0, and those stay unacknowledged. go-udx
applies the same rule since `fix/ack-pings` (jsudx-4ge): it records numbered
PINGs and probes and ACKs them at once, so our pings succeed, PMTUD rises to
1472 against Go, and probe sequences leave no holes in Go's ACK ranges.

## Deliberate deviations from go-udx (sender-side only, nothing on the wire)

- **RTO** = SRTT + max(4·RTTVAR, 1 ms) + 25 ms (RFC 9002 PTO), initial RTT 333 ms.
  go-udx uses SRTT + 4·RTTVAR from an initial 100 ms. That resends the whole first
  flight on paths with RTT ≥ 300 ms. On steady paths with RTT above the 200 ms
  floor it also resends every delayed-ACK packet, because RTTVAR decays to 0 and
  the RTO collapses onto the RTT. Found by the simulated-link tests (Phase 2).
- **One retransmission timer per connection**, armed at the earliest packet
  deadline, rather than one Go timer per packet.
- **A collapsed RTO keeps its timer.** When a packet's timer fires within an RTO
  of a SACK-driven resend, go-udx drops that packet's timer, leaving it to SACK
  alone. js-udx re-arms it at lastRetransmit + RTO.
- **Pacing granularity 1 ms.** Packets due within 1 ms go out at once; Node
  timers can't fire sooner, so pacing to µs intervals would cap throughput.
- **Received CONNECTION_CLOSE is not answered** (RFC 9000 §10.2.2). go-udx sends
  one back.
- **An empty stream's FIN carries SYN**, so the peer opens it and sees EOF.
  go-udx sends a bare FIN, which the peer drops.
- **Closed streams stay as tombstones** in the routing maps, so a late
  retransmission can't reopen them. The stream limit counts only active streams;
  go-udx counts every stream it ever had, capping a connection at 100 for life.
  Incoming streams are capped at 1024 active (reset with code 2 beyond that).
- **Early packets** (arriving before their connection's SYN) expire after 10 s.
  go-udx keeps them forever.
- **Not ported (dead in go-udx):** the congestion controller's PTO probe timer
  (nil callback) and duplicate-ACK fast retransmit. Per-packet RTO and SACK loss
  detection cover both.

## Status and next steps (2026-10-02)

Done: all phases, 0–7. `npm run ci` (on Node 22) = typecheck + build + lint +
327 tests, including UDX interop over real UDP with go-udx and dart-udx,
js-libp2p's transport compliance suite, and libp2p interop with go-libp2p and
dart-libp2p. `npm run interop:go-udx` runs go-udx's bulk and multi-stream
suites against JS through `tools/bulk-peer`. dart-udx 3.1.0 (all the dart-udx
fixes below) is on pub.dev and GitHub.

Phase 7 (`packages/libp2p-udx/test/interop.test.ts`): js-libp2p ↔ go-libp2p
(`tools/go-libp2p-peer`) and js-libp2p ↔ dart-libp2p (`tools/dart-libp2p-peer`),
both directions: Noise + Yamux, ping, identify (protocols, agent, UDX listen
address) and a 1 MiB echo. dart-libp2p's random stream ids for its first stream
work against js-udx's acceptor. Idle connections to go-libp2p outlive the UDX
idle timeout (`UDX_SLOW_TESTS=1`). It found three bugs outside js-udx:
- **jsudx-aof** (dart-libp2p): its Yamux ignores FIN on WINDOW_UPDATE, which
  is how go-yamux and js-libp2p half-close. Dart never sees Go or JS end a
  stream, so echoes never end (the peers and tests read exact lengths instead),
  and js-libp2p's connection monitor aborts idle connections to Dart after
  ~20 s, its ping streams left half-open. Both are `it.fails`.
- **jsudx-94k** (dart-libp2p): a peer resetting a UDX connection crashes the
  Dart host with an unhandled `UDXTransportException`.
- **jsudx-bh8** (js-libp2p `@libp2p/utils` 7.4.1): a paused stream's async
  iterator ends at the remote's FIN with data still buffered, which is lost;
  `echo()` hits it under backpressure. Reproduced without UDX. The tests use
  an echo that never pauses. Not yet reported upstream.

Phase 5 found and fixed two js-udx bugs that Go interop had hidden:
- The multiplexer announced a new connection only after handling its first
  datagram. dart-udx has no connection SYN (its first datagram opens a stream),
  so `'stream'` listeners attached in the `'connection'` handler missed it.
- A stream's SYN rode its first data or FIN, so a stream opened only to read
  never reached the peer. It now goes out on open, as in Go and Dart; a write
  in the same tick still shares the packet.

dartudx-4u8 is fixed in dart-udx 3.1.0: it registered a stream a peer opened
under the destination id the peer used, which go-udx and js-udx always send as
0, so every stream after the first on a connection was merged into the first
and lost. `dart-interop.test.ts` covers 8 concurrent and back-to-back streams.

Next (backlog):
1. dart-libp2p: jsudx-aof (Yamux FIN on WINDOW_UPDATE) and jsudx-94k (crash
   on reset); then flip the two Dart `it.fails` in `interop.test.ts`.
2. Report jsudx-bh8 to js-libp2p.
3. netem interop matrix (jsudx-5hp); dartudx-by0 (Dart ACKs without SACK
   history); go-udx leftovers (jsudx-6u6).

Practical notes: `dart test … | tail` hides the exit code — check with
`-r json` or `set -o pipefail`. Running go-udx's interop suite rewrites
`interop/dartpeer/pubspec.lock`; restore it.

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
5. **UDX interop** *(done)* — `tools/bulk-peer` matching Dart's `bulk_peer.dart` CLI/stderr protocol
   (`READY`/`PROGRESS`/`RESULT`/`CORRUPT`/`WINDOW`) so `go-udx/interop` tests can drive JS;
   JS-side tests that spawn Go peers. Later: netem Docker matrix.
6. **libp2p transport** *(done)* — register multiaddr `udx` (0x0300, size 0); Transport/Listener;
   MultiaddrConnection over the dialer's first UDX stream, handed to the upgrader
   (Noise + Yamux). One shared dial socket per address family (as Go). Note: Dart listener
   de-dups sessions by remote ip:port.
7. **libp2p interop** *(done)* — Go `dart-libp2p/interop/go-peer --transport=udx`, Dart
   `interop_echo_server` / `interop_echo_client` (`READY <port> <peerid>` on stderr):
   identify, echo, ping.

## Upstream issues found (not in scope here)

Status 2026-10-02: fixed on branches, not yet merged —
dart-udx `fix/go-js-interop` (frame numbering + STREAM_DATA_BLOCKED answer,
ACK gap/range overflow, same-address connections) and go-udx
`fix/dual-stack-batch-writes` (udx.Dial batched writes). The remaining
dart-udx items are in dart-udx's beads backlog (`bd ready` there), including
two found while fixing: streams opened by go-udx/js-udx collide on Dart's
local id 0 (dartudx-4u8), and Dart ACKs carry no SACK history (dartudx-by0).

- dart-udx PMTUD probes carry only MTU_PROBE, which dart-udx itself never acknowledges, so its PMTUD never raises the MTU.
- ~~dart-udx `ping()` against go-udx always fails: go-udx never acknowledges PINGs.~~ Fixed in go-udx `fix/ack-pings`.
- dart-udx anti-amplification validates on the second packet (or 1000 bytes), which proves nothing about the address.
- dart-udx sends STOP_SENDING as 0x0c, which go-udx and js-udx reject as an unknown frame (the whole packet is dropped).
- go-udx RTO omits max_ack_delay and starts from a 100 ms RTT (see deviations above).
- go-udx drops a packet's retransmit timer when it collapses into a recent resend.
- `udx.Dial` binds a dual-stack socket, and go-udx's batched send path
  (`ipv4.NewPacketConn` + `WriteBatch`) silently drops every multi-datagram write
  from it to an IPv4 peer; only single sends (SYN, retransmits) arrive. Go→JS
  1 MiB took 15 s instead of 23 ms. tools/go-peer binds `udp4` explicitly to avoid it.
- go-udx's initial ssthresh of 65535 ends slow start at 64 KiB. In the simulation a
  1 MiB transfer over a clean 300 ms path tops out near 85 KB of cwnd. Worth
  revisiting for high-BDP links; js-udx keeps Go's value for now.
- Dart frame enum numbering diverges from Go for 0x0c–0x11.
- `go-udx/cmd/interop-server` stamps version 2 on replies (dropped by v3 peers); Dart raw interop test asserts v2.
- `go-udx/doc/PENDING_WORK.md` §5 says Dart uses UDX streams as the libp2p muxer; dart-libp2p's swarm actually always upgrades over one stream.
- Dart ACK frame declares ECN fields in its length but never encodes them; u8 ACK gaps > 255 truncate.
