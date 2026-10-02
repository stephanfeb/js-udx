# js-libp2p transport

`@stephanfeb/libp2p-udx` lets a js-libp2p node dial and listen on UDX
addresses:

```text
/ip4/<ip>/udp/<port>/udx
/ip6/<ip>/udp/<port>/udx
```

It connects to go-libp2p nodes running go-libp2p-udx-transport and to
dart-libp2p nodes with their UDX transport. It needs Node.js 22 and
js-libp2p v3.

## Setting up a node

Add `udx()` to `transports`, with Noise and Yamux. UDX secures and
multiplexes a connection the way TCP does, through libp2p's upgrader, so it
needs a connection encrypter and a stream muxer:

<!-- example: examples/libp2p-echo.ts#node -->
```ts
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { UDX, udx } from '@stephanfeb/libp2p-udx'
import { createLibp2p } from 'libp2p'
import type { Stream } from '@libp2p/interface'

async function createNode (listen: string[]) {
  return await createLibp2p({
    addresses: { listen },
    transports: [udx()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: { ping: ping(), identify: identify() }
  })
}
```
<!-- /example -->

Noise and Yamux are what go-libp2p-udx-transport and dart-libp2p use, so use
them if you want to reach those nodes. Ping and identify are optional.

## Talking to another node

Once the node is set up, everything else is ordinary js-libp2p:

<!-- example: examples/libp2p-echo.ts#echo -->
```ts
const ECHO = '/echo/1.0.0'

/** Sends back whatever arrives, then ends our side once the dialer ends theirs. */
async function echo (stream: Stream): Promise<void> {
  for await (const chunk of stream) stream.send(chunk)
  await stream.close()
}
```
<!-- /example -->

<!-- example: examples/libp2p-echo.ts#dial -->
```ts
const server = await createNode(['/ip4/127.0.0.1/udp/0/udx'])
await server.handle(ECHO, echo)
const address = server.getMultiaddrs().find(ma => UDX.exactMatch(ma))
if (address === undefined) throw new Error('not listening on UDX')
console.log(`server listening on ${address.toString().split('/p2p/')[0]}`)

const client = await createNode([])
const rtt = await client.services.ping.ping(address)
console.log(`ping: ${rtt >= 0 ? 'ok' : 'failed'}`)

const stream = await client.dialProtocol(address, ECHO)
stream.send(new TextEncoder().encode('hello over libp2p'))
await stream.close()
let reply = ''
for await (const chunk of stream) reply += new TextDecoder().decode(chunk.subarray())
console.log(`echo: ${reply}`)
```
<!-- /example -->

The whole program is [`examples/libp2p-echo.ts`](../examples/libp2p-echo.ts).
It prints:

```text
server listening on /ip4/127.0.0.1/udp/50123/udx
ping: ok
echo: hello over libp2p
```

The echo handler reads the stream with `for await` and never pauses it. In
`@libp2p/utils` 7.4.1, a stream paused for backpressure can stop at the
remote's end before its buffered data has been read, and that data is lost
([libp2p/js-libp2p#3646](https://github.com/libp2p/js-libp2p/issues/3646)).
This affects every transport, not only UDX. Until it's fixed, avoid pausing
streams in handlers that read to the end, including the `echo()` helper from
`@libp2p/utils`.

## Addresses

Listen on `/ip4/0.0.0.0/udp/0/udx` (or a fixed port) to accept connections on
every interface. The node then announces one address per interface.
`/ip6/::/udp/0/udx` listens on IPv6. Each listen address gets a socket of its
own.

Outgoing connections share one socket per address family, whatever the
number of peers.

Only IP addresses are supported. The transport doesn't dial `/dns4`,
`/dns6` or `/dnsaddr` addresses itself.

The package registers the `udx` multiaddr protocol (code `0x0300`, as
go-libp2p-udx-transport and dart-libp2p do) when it's imported, so
`multiaddr('/ip4/1.2.3.4/udp/4001/udx')` parses. It also exports:

| Export | |
|---|---|
| `UDX` | A [multiaddr matcher](https://github.com/multiformats/js-multiaddr-matcher): `UDX.exactMatch(ma)` is true for a UDX address, with or without `/p2p/<peer>` |
| `parseUdxMultiaddr(ma)` | `{ host, port, family }` for a UDX address, else `undefined` |
| `toUdxMultiaddr(host, port)` | The UDX multiaddr for a UDP endpoint |

## Options

`udx(init)` takes:

| Option | Default | |
|---|---|---|
| `keepAliveInterval` | `10_000` | Ping a connection after this many ms without hearing from the peer, so it outlives UDX's 30 s idle timeout at both ends. `0` turns it off. |
| `closeTimeout` | `5_000` | How long closing a connection waits for the peer to acknowledge what we sent. |
| `inactivityTimeout` | js-libp2p's | Passed to each connection; see js-libp2p's `MessageStream`. |
| `udx` | | [UDX options](connections.md#options) for every socket the transport creates, such as `statelessResetSecret` or `pmtud`. |

## How it works

Each libp2p connection is one UDX connection. Its first UDX stream carries
the connection like a TCP socket: multistream-select, then Noise, then Yamux,
whose streams carry your protocols. This is how go-libp2p-udx-transport and
dart-libp2p use UDX too, which is what lets the three talk to each other.

Closing a libp2p connection ends that stream, waits up to `closeTimeout` for
the peer to acknowledge everything, then closes the UDX connection.
Aborting it resets the stream and closes the connection with an error.
