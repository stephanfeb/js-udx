# @go-ricochet/udx

UDX for Node.js: a QUIC-inspired reliable transport over UDP, wire version 3.
Many ordered byte streams per connection, RFC 9002 loss recovery and pacing,
CUBIC congestion control, per-stream and per-connection flow control, and path
MTU discovery from 1200-byte datagrams. Interoperates with
[go-udx](https://github.com/stephanfeb/go-udx) v0.3.0 and
[dart-udx](https://pub.dev/packages/dart_udx) 4.3.0. Node.js ≥ 20; browsers
have no raw UDP.

```ts
import { listen, dial } from '@go-ricochet/udx'

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

`write` returns false when the send queue is full; wait for `'drain'`.

Documentation: [cryptopeer.net/udx/js](https://cryptopeer.net/udx/js/).
Source and changelog: [github.com/stephanfeb/js-udx](https://github.com/stephanfeb/js-udx).
For js-libp2p, see [@go-ricochet/libp2p-udx](https://www.npmjs.com/package/@go-ricochet/libp2p-udx).
