# Getting started

## Install

Both packages are on npm. For UDX on its own:

```sh
npm install @go-ricochet/udx
```

For js-libp2p, install the transport with libp2p, Noise and Yamux. The
transport brings `@go-ricochet/udx` with it:

```sh
npm install @go-ricochet/libp2p-udx libp2p @chainsafe/libp2p-noise @chainsafe/libp2p-yamux
```

Both packages are ES modules with TypeScript types. `@go-ricochet/udx` needs
Node.js 20 or later, and `@go-ricochet/libp2p-udx` needs Node.js 22, as
js-libp2p v3 does.

### From source

To try changes that aren't released yet, build and pack the packages from a
checkout, and install the tarballs:

```sh
git clone https://github.com/stephanfeb/js-udx.git
cd js-udx
npm install
npm run build
npm pack -w packages/udx -w packages/libp2p-udx --pack-destination ~/js-udx-packages
cd your-project
npm install ~/js-udx-packages/stephanfeb-udx-*.tgz ~/js-udx-packages/stephanfeb-libp2p-udx-*.tgz
```

Install the tarballs, not the package directories. npm links a directory
instead of copying it, so the transport would load its own copy of
`@multiformats/multiaddr` from the js-udx checkout. js-libp2p then can't
parse `/udx` addresses (`Protocol udx was unknown`).

## An echo server and client

This program starts a UDX server that sends every stream's bytes back, then
connects to it, sends a message and prints the reply:

<!-- example: examples/echo.ts -->
```ts
// A UDX echo server and a client that talks to it, in one process.
// Run: node examples/echo.ts
import { dial, listen } from '@go-ricochet/udx'

const server = await listen({ host: '127.0.0.1', port: 0 })
server.on('connection', conn => {
  conn.on('stream', stream => {
    // Send every chunk back, and end our side when the client ends theirs.
    stream.on('data', chunk => stream.write(chunk))
    stream.on('end', () => stream.end())
  })
})
const { port } = server.address()

const conn = await dial(port, '127.0.0.1')
const stream = conn.openStream()
stream.write(new TextEncoder().encode('hello, udx'))
stream.end()

// Iteration ends when the server ends its side.
const decoder = new TextDecoder()
let reply = ''
for await (const chunk of stream) reply += decoder.decode(chunk, { stream: true })
console.log(reply)

conn.close()
await server.close()
```
<!-- /example -->

It prints `hello, udx`. Save it as `echo.mjs` in a project where you
installed `@go-ricochet/udx` and run `node echo.mjs`; it is plain JavaScript as
well as TypeScript. On Node.js 22.18 or later you can also run it from a
js-udx checkout as `node examples/echo.ts`.

What happens:

1. `listen` binds a UDP socket and returns a `UdxMultiplexer`. It emits
   `'connection'` for every peer that connects. Port `0` picks a free port;
   `server.address()` tells you which.
2. `dial` binds a socket of its own, sends the connection handshake and
   returns the connection straight away. You can open streams and write
   before the peer has answered; the data goes out as soon as it can.
   `await conn.established` if you need to know the peer is there.
3. `openStream()` opens a stream. The server's connection emits `'stream'`
   for it.
4. `end()` half-closes: our side is done sending, but we can still read. The
   server sees `'end'` once it has read everything, and ends its own side.
5. A stream is an async iterable of `Uint8Array` chunks, and iteration stops
   when the peer ends its side.
6. `conn.close()` closes the connection, and the socket `dial` made with it.
   `server.close()` closes every connection the server holds, then its socket.

Chunk boundaries aren't preserved. A stream is a byte stream, like TCP: one
`write` can arrive as several chunks, and several writes as one.

## Next

- [Streams](streams.md) covers sending more than a few bytes: backpressure,
  pausing, and closing without losing data.
- [Connections](connections.md) covers options, multiplexers and errors.
- [js-libp2p transport](libp2p.md) if you're building on libp2p.
