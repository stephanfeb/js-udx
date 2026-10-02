# Interoperability

js-udx speaks UDX wire version 3. go-udx is the reference for the wire
format, and js-udx's codec tests check its encoding byte for byte against
vectors generated from go-udx.

## Versions

| Peer | Works with | Notes |
|---|---|---|
| [go-udx](../../go-udx/README.md) | v0.1.3 and later | Earlier v3 releases work, but don't acknowledge PINGs, so `conn.ping()` always resolves `false` |
| [dart_udx](../../dart-udx/README.md) | 3.1.0 and later | Before 3.1.0, a second stream opened by the Dart side, or a stream opened right after another closed, can get mixed up |
| [go-libp2p-udx-transport](../../go-libp2p-udx-transport/README.md) | v0.1.4 and later | Built on go-udx v0.1.3 |
| [dart-libp2p](../../dart-libp2p/doc/index.md) | 3.0.0 and later | Earlier releases miss the end of a stream that js-libp2p ends with a FIN on a Yamux window update, and can crash when js-libp2p aborts a connection |

UDX v2 and earlier don't interoperate with v3. js-udx answers a packet on
another version with a version negotiation packet, and a js-udx dialer that
gets one before the peer has answered gives up at once instead of timing out.

js-udx has some features go-udx lacks, and does some of them differently
from dart_udx. Each is built so it still works with both:

| Feature | With go-udx | With dart_udx |
|---|---|---|
| Path MTU discovery | Rises to 1472-byte datagrams (v0.1.3 and later) | Rises to 1472-byte datagrams |
| Anti-amplification | Works: go-udx answers the address challenge | Works: dart_udx answers the address challenge |
| Path migration | js-udx follows a go-udx peer that moves; go-udx doesn't follow js-udx | Both follow each other |
| Stateless reset | No effect: go-udx ignores reset tokens | No effect: dart_udx doesn't use reset tokens |

Stateless reset works between js-udx peers only. Turning it on is safe with
go-udx and dart_udx peers: they ignore or drop the packet that carries the
token, and that packet carries nothing else.

## How it's tested

The test suite runs against real peers over real UDP, when their source is
checked out next to js-udx (`../go-udx`, `../dart-udx`,
`../go-libp2p-udx-transport`, `../dart-libp2p`) and the Go and Dart
toolchains are installed. It skips what's missing.

| Test | Covers |
|---|---|
| `packages/udx/test/go-interop.test.ts` | js-udx with go-udx, both directions: handshake, bulk transfer, many streams, ping, path MTU discovery |
| `packages/udx/test/dart-interop.test.ts` | The same with dart_udx, plus streams opened back to back and concurrently from each side |
| `npm run interop:go-udx` | go-udx's own bulk and multi-stream interop suite, with js-udx in place of its Dart peer |
| `packages/libp2p-udx/test/compliance.test.ts` | js-libp2p's transport compliance suite |
| `packages/libp2p-udx/test/interop.test.ts` | js-libp2p with go-libp2p and with dart-libp2p, each side dialing the other: Noise, Yamux, ping, identify and a 1 MiB echo |
| `examples/examples.test.ts` | Every program in [`examples/`](../examples), which the code samples in these guides come from |

`UDX_SLOW_TESTS=1` adds tests that leave connections idle for 45 seconds, to
check that keep-alives hold them open at both ends.
