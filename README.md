# js-udx

TypeScript port of the UDX protocol (wire v3), a QUIC-inspired reliable UDP
transport, plus a js-libp2p transport built on it. Node.js only. Browsers have
no raw UDP.

| Package | Purpose |
|---|---|
| [`packages/udx`](packages/udx) | The UDX protocol: packets, reliability, congestion and flow control, connections and streams. No libp2p dependency. |
| [`packages/libp2p-udx`](packages/libp2p-udx) | js-libp2p transport for `/ip4\|ip6/<ip>/udp/<port>/udx`. |

Interoperates with [go-udx](../go-udx) and [dart-udx](../dart-udx), and the
transport with go-libp2p-udx-transport and dart-libp2p. go-udx is the wire
authority. See [PLAN.md](PLAN.md) for the porting plan and the protocol rules.

## libp2p

```ts
import { createLibp2p } from 'libp2p'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { udx } from '@stephanfeb/libp2p-udx'

const node = await createLibp2p({
  addresses: { listen: ['/ip4/0.0.0.0/udp/0/udx'] },
  transports: [udx()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()]
})
```

Each libp2p connection is one UDX connection. Its first stream is upgraded
like a TCP socket (multistream-select, then Noise, then Yamux), as
go-libp2p-udx-transport and dart-libp2p do. Dials share one UDP socket per
address family. The transport pings a connection after 10 s without hearing
from the peer (`keepAliveInterval`), so idle connections outlive UDX's 30 s
idle timeout at both ends. `UDX` is a multiaddr matcher for these addresses.
Needs Node ≥ 22, as js-libp2p v3 does; `@stephanfeb/udx` alone runs on Node 20.

## UDX

```ts
import { listen, dial } from '@stephanfeb/udx'

// Server: echo every stream.
const server = await listen({ host: '127.0.0.1', port: 9000 })
server.on('connection', conn => {
  conn.on('stream', stream => {
    stream.on('data', chunk => stream.write(chunk))
    stream.on('end', () => stream.end())
  })
})

// Client.
const conn = await dial(9000, '127.0.0.1')
const stream = conn.openStream()
stream.write(new TextEncoder().encode('hello'))
stream.end()
for await (const chunk of stream) console.log(new TextDecoder().decode(chunk))
conn.close()
```

`write` returns false when the send queue is full; wait for `'drain'`. A
stream starts paused and flows once it has a `'data'` listener, is iterated, or
`resume()` is called. Only consumed bytes reopen the peer's window, so a paused
stream pushes back on the sender. One `UdxMultiplexer` socket can both accept
and dial any number of connections, routed by connection ID.

Beyond go-udx's feature set, connections also do path MTU discovery,
anti-amplification on accepted connections, path migration (following a
peer's NAT rebinding), connection-level flow control and acknowledged pings.
Multiplexers do version negotiation and, given a `statelessResetSecret`,
stateless reset. Each can be turned off in the multiplexer options. See
PLAN.md for how they interoperate with go-udx and dart-udx.

## Development

```sh
npm install
npm test           # vitest, runs against sources
npm run build      # tsc -b, both packages
npm run ci         # typecheck + build + lint + test
```

Issues are tracked with beads (`bd ready`).

### Wire conformance vectors

`packages/udx/test/vectors/go-v3.json` is generated from go-udx (expected at
`../go-udx`) by `tools/gen-vectors`. The codec tests require byte-identical
encoding and the same accept/reject verdict on truncated and fuzzed input.
Regenerate after a go-udx wire change with `npm run vectors` (needs Go).

### Interop tests

`packages/udx/test/go-interop.test.ts` builds `tools/go-peer` against
`../go-udx`, and `dart-interop.test.ts` compiles `tools/dart-peer` against
`../dart-udx`; both exchange data in both directions over real UDP and are
skipped when the toolchain or the sibling repo is missing.
`npm run interop:go-udx` runs go-udx's own bulk and multi-stream interop
suites against js-udx through `tools/bulk-peer`.

`packages/libp2p-udx/test/compliance.test.ts` runs js-libp2p's transport
compliance suite. `UDX_SLOW_TESTS=1` adds a 45 s idle keep-alive test.
