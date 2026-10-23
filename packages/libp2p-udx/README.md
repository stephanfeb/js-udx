# @stephanfeb/libp2p-udx

A [js-libp2p](https://github.com/libp2p/js-libp2p) transport for
`/ip4|ip6/<ip>/udp/<port>/udx` addresses, built on
[@stephanfeb/udx](https://www.npmjs.com/package/@stephanfeb/udx).
Interoperates with go-libp2p-udx-transport and dart-libp2p. Node.js ≥ 22, as
js-libp2p v3 needs.

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
like a TCP socket (multistream-select, then Noise, then Yamux). Dials share one
UDP socket per address family. The transport pings a connection after 10 s
without hearing from the peer, so idle connections outlive UDX's 30 s idle
timeout at both ends. `UDX` is a multiaddr matcher for these addresses.

In a browser, where the transport can't run, import
`@stephanfeb/libp2p-udx/multiaddr` instead: it only registers the `udx`
multiaddr codec, so js-libp2p web apps can use relays that advertise `/udx`
addresses. See [In a browser](https://cryptopeer.net/udx/js/libp2p/#in-a-browser).

Documentation: [cryptopeer.net/udx/js](https://cryptopeer.net/udx/js/).
Source and changelog: [github.com/stephanfeb/js-udx](https://github.com/stephanfeb/js-udx).
