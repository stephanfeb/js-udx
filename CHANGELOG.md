# Changelog

All notable changes to js-udx (`@go-ricochet/udx` and `@go-ricochet/libp2p-udx`,
formerly `@stephanfeb/udx` and `@stephanfeb/libp2p-udx`).

## [Unreleased]

### Changed

- **New npm scope.** The packages are now `@go-ricochet/udx` (0.1.0) and
  `@go-ricochet/libp2p-udx` (0.1.1), the same code as the `@stephanfeb` releases.
  They publish from GitHub Actions with npm provenance (`.github/workflows/publish.yml`).
  The `@stephanfeb` packages are deprecated in favour of these.

## [0.1.1] - 2026-10-23

`@stephanfeb/libp2p-udx` only; `@stephanfeb/udx` is unchanged at 0.1.0.

### Added

- **A browser entry, `@stephanfeb/libp2p-udx/multiaddr`.** It registers the `udx`
  multiaddr codec and exports `UDX`, `parseUdxMultiaddr` and `toUdxMultiaddr`, with no
  Node.js built-ins, so it bundles for the browser. js-libp2p web apps need the codec to
  use relays that advertise `/udx` addresses (go-ricochet, dart-libp2p): without it
  js-libp2p rejects their reservations with `Protocol 768 was unknown`. The package root
  imports the transport and `node:dgram`, so browser bundles of it fail. A test bundles
  both for the browser to keep it that way.

## [0.1.0] - 2026-10-23

First tagged release: the UDX v3 protocol, interoperable with go-udx v0.3.0
and dart-udx 4.3.0, and a js-libp2p transport on it. Not yet on npm.

### Changed

- **Datagrams start at 1200 bytes, and path MTU discovery raises them.** Connections sent
  about 1472-byte datagrams from the first packet and only searched upwards, so a path
  that carries less (seen on a mobile carrier) dropped every full packet. Streams now send
  1200-byte datagrams, which every path carries, and `MtuDiscovery` (RFC 8899, as go-udx
  v0.3.0) probes up to 1472: the first probe is the maximum, three losses rule a size out,
  and the search ends within 16 bytes. Probes are numbered and tracked so ACKs and loss
  detection report on them, but never re-sent and never in flight. A black hole (three
  full-size data packets lost with none acknowledged, or three probe timeouts in a row)
  takes the size back to 1200 at once, re-sends are re-chunked to fit, and the search
  restarts after a minute. Replaces `PmtuSearch`.
- **Loss recovery is RFC 9002's.** Loss is declared from acknowledgements over every
  outstanding packet below the largest acknowledged (three behind, or older than 9/8 of
  the RTT, with a loss timer for the rest). Lost packets leave flight, cut the window once
  per epoch, and are re-sent under fresh sequences through the window and the pacer before
  new data. One probe timeout per connection (SRTT + max(4·RTTVAR, 1 ms) + 25 ms, doubling
  until an ACK) re-sends the two oldest outstanding packets. A loss spanning three probe
  timeouts with nothing acknowledged is persistent congestion: the window falls to the
  minimum. This replaces one retransmission deadline per packet, which after a radio gap
  re-sent the whole window at once, and again on every backoff.
- **Slow start runs until the first loss.** The threshold started at 65535 bytes, so
  slow start ended at 64 KB; a clean 1 MiB transfer over a 300 ms path took 6.3 s in the
  simulation and now takes 2.0 s.
- **RTT estimation follows RFC 9002 §5.3**, and the probe timeout has no 200 ms floor or
  5 s cap. Min RTT is unset until the first sample.
- **Pacing by the window and the smoothed RTT.** A budget pacer at gain × cwnd / SRTT
  (2 in slow start, 1.25 after), recomputed on every ACK, with bursts of up to ten
  datagrams. It was 2.88 × cwnd / min RTT, recomputed only on loss or a new minimum.

### Fixed

- **go-udx v0.3.0's path MTU probes were never acknowledged**, so go-udx could not raise
  its datagram size towards a js-udx peer. Its probes carry MTU_PROBE without a PING; a
  numbered packet with either is now acknowledged at once.

### Removed

- The `STREAM_CHUNK_SIZE` and `PmtuSearch` exports. `MtuDiscovery` and the `SentPacketInfo`
  type are exported instead; a connection's `datagramSize` gives the current size.

### Known

- Under heavy reordering the loss timer declares late packets lost before their ACK
  arrives (in the simulation, 25% of packets 10 ms late on a 100 ms path: 35 window cuts
  instead of 14). go-udx and dart-udx behave the same; see PLAN.md.
