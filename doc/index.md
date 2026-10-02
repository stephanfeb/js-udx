# js-udx

js-udx is UDX for Node.js, written in TypeScript. UDX is a reliable transport
over UDP, modelled on QUIC. js-udx speaks wire version 3, the same as
[go-udx](../../go-udx/README.md) and [dart_udx](../../dart-udx/README.md), and
connects to both. It also includes a transport for js-libp2p, so a js-libp2p
node can connect to go-libp2p and dart-libp2p nodes over UDX.

js-udx runs on Node.js only: browsers can't send raw UDP.

## Packages

| Package | What it is | Node.js |
|---|---|---|
| `@stephanfeb/udx` | The protocol: connections, streams, reliability, congestion and flow control. No libp2p dependency. | ≥ 20 |
| `@stephanfeb/libp2p-udx` | A js-libp2p transport for `/ip4/<ip>/udp/<port>/udx` and `/ip6/…` addresses. | ≥ 22 (js-libp2p v3 needs it) |

The packages aren't on npm yet. [Getting started](getting-started.md) shows
how to build them from source and use them in your project.

## What you get

| Feature | Notes |
|---|---|
| Multiplexed streams | Many ordered byte streams per connection, each reassembled on its own, so a lost packet only holds up its own stream |
| Backpressure | `write` returns `false` when the send queue is full. A paused reader stops the sender, because only consumed bytes reopen the peer's window. |
| Congestion control | CUBIC, with RFC 9002 RTT estimation, pacing and delayed ACKs |
| Flow control | Per stream (auto-tuned, up to 4 MiB) and per connection |
| One socket, many connections | A `UdxMultiplexer` accepts and dials on the same UDP socket, routing by connection ID |
| Path MTU discovery | Probes up to 1472-byte datagrams (RFC 8899) |
| Anti-amplification | An accepted connection sends at most 3× what it has received until the peer proves its address |
| Path migration | Follows a peer whose address changes (a NAT rebinding, say) once the new address answers a challenge |
| Keep-alive and ping | Optional PINGs that keep an idle connection open, and an acknowledged `ping()` |
| Version negotiation, stateless reset | A peer on another wire version, or one whose connection we lost, finds out at once instead of timing out |

## Guides

- [Getting started](getting-started.md): build the packages and run an echo server and client.
- [Streams](streams.md): writing with backpressure, reading, half-close, resets.
- [Connections](connections.md): dialing and listening, multiplexers, options, closing, errors.
- [js-libp2p transport](libp2p.md): using UDX in a js-libp2p node.
- [Interoperability](interop.md): which versions of go-udx, dart_udx, go-libp2p and dart-libp2p js-udx works with, and how it's tested.

Every code sample in these guides comes from a program in
[`examples/`](../examples), and the test suite runs each of them.
