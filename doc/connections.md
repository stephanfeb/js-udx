# Connections

A `UdxConnection` is a connection to one peer, carrying any number of streams.
It does the reliability, congestion control and flow control for all of them.

## Dialing and listening

The simplest way in has two functions:

| Function | Returns |
|---|---|
| `listen({ host, port, ...options })` | A `UdxMultiplexer` bound to that address, emitting `'connection'` for each peer that connects. Also `await mux.accept()`. |
| `dial(port, host, options?)` | A `UdxConnection` from a new socket of its own. The socket closes when the connection does. |

`dial` sends the handshake and returns without waiting for an answer. Streams
you open and write to straight away are sent as soon as the handshake allows.
`conn.established` resolves when the peer first answers. A peer that never
answers isn't an error until the connection's idle timeout (below) closes it.

## One socket for everything

A `UdxMultiplexer` owns one UDP socket and routes its datagrams to connections
by connection ID. A multiplexer can accept and dial at the same time, and hold
any number of connections. Use one when you want a single port for incoming
and outgoing connections (for NAT traversal, say) or many connections without
a socket each:

<!-- example: examples/one-socket.ts#peers -->
```ts
const alice = await UdxMultiplexer.create({ host: '127.0.0.1', port: 0 })
const bob = await UdxMultiplexer.create({ host: '127.0.0.1', port: 0 })

// Bob answers each stream with its byte count.
void (async () => {
  const conn = await bob.accept()
  while (!conn.closed) {
    const stream = await conn.acceptStream().catch(() => undefined)
    if (stream === undefined) break
    void (async () => {
      let n = 0
      for await (const chunk of stream) n += chunk.length
      stream.write(new TextEncoder().encode(String(n)))
      stream.end()
    })()
  }
})()

// Alice dials Bob from the same socket she would accept on.
const conn = alice.dial(bob.address().port, '127.0.0.1')
await conn.established
```
<!-- /example -->

`mux.dial(port, host)` is the multiplexer's dial; it returns the connection
synchronously. Streams on one connection are independent:

<!-- example: examples/one-socket.ts#streams -->
```ts
async function count (stream: UdxStream, bytes: number): Promise<string> {
  stream.write(new Uint8Array(bytes))
  stream.end()
  let reply = ''
  for await (const chunk of stream) reply += new TextDecoder().decode(chunk)
  return reply
}

// Streams are independent: a lost packet on one doesn't stall the others.
const replies = await Promise.all([1_000, 100_000, 1_000_000].map(n => count(conn.openStream(), n)))
console.log(`bob counted ${replies.join(', ')}`)
```
<!-- /example -->

## Options

`listen`, `dial` and `UdxMultiplexer.create` all take these. Every connection
the multiplexer makes or accepts uses them.

| Option | Default | |
|---|---|---|
| `host`, `port` | all interfaces, `0` | Where to bind. Port `0` picks a free one. (`listen` and `create` only.) |
| `type` | from `host` | `'udp4'` or `'udp6'`. |
| `ipv6Only` | `false` | Don't accept IPv4-mapped addresses on a `'udp6'` socket. |
| `recvBufferSize`, `sendBufferSize` | 4 MiB | Socket buffer sizes to ask the OS for. |
| `streamOptions.writeHighWaterMark` | 256 KiB | When a stream's `write` starts returning `false`. |
| `keepAliveInterval` | `0` (off) | Send a PING after this many ms without hearing from the peer. See below. |
| `pmtud` | `true` | Probe for datagrams larger than the default (about 1,400 bytes), up to 1472 bytes. |
| `antiAmplification` | `true` | Limit an accepted connection to 3× the bytes it has received until the peer's address is proven. |
| `migration` | `true` | Follow a peer to a new address, once it answers a challenge there. |
| `versionNegotiation` | `true` | Tell a peer speaking another UDX version which one we speak, so it fails fast. |
| `statelessResetSecret` | unset | 32 or more secret bytes. With it, a peer that sends to a connection we no longer have is told so at once, instead of timing out. Keep the secret the same across restarts. |

## Idle connections

A connection closes after 30 seconds without hearing from its peer, with
code 6 (`ErrorCode.ConnectionTimeout`). UDX sends nothing on an idle
connection by itself, so a connection that nobody writes to times out at both
ends.

To keep one open, set `keepAliveInterval`. The connection then sends a PING
after that many milliseconds of silence, which the peer acknowledges. A value
under 15 000 keeps the connection alive even if a PING is lost. The js-libp2p
transport uses 10 000 by default.

## Ping and statistics

`await conn.ping()` sends a PING and resolves `true` when the peer
acknowledges it, or `false` after 5 seconds (pass another timeout in ms).

<!-- example: examples/one-socket.ts#stats -->
```ts
console.log(`ping acknowledged: ${await conn.ping()}`)
console.log(`rtt ${conn.smoothedRtt} ms, cwnd ${conn.cwnd} bytes, datagrams up to ${conn.datagramSize} bytes`)
```
<!-- /example -->

| Property | Meaning |
|---|---|
| `smoothedRtt` | Smoothed round-trip time, in ms |
| `cwnd`, `inflight` | Congestion window and bytes in flight |
| `datagramSize` | The largest datagram this connection sends, raised by path MTU discovery |
| `peerMaxData` | How many bytes the peer lets us have in flight |
| `streamCount` | Open streams |
| `remoteAddress`, `remotePort` | The peer's current address |
| `addressValidated` | Whether the peer has proven its address (always `true` for the dialer) |
| `closed` | Whether the connection has closed |

A connection emits `'migrate'` `(address, port)` when it follows its peer
to a new address.

## Closing

`conn.close(code?, reason?)` sends CONNECTION_CLOSE and closes every stream
on the connection at once. That includes:

- data still in flight, which the peer will never get;
- data the peer has received but its application hasn't read yet. The peer
  drops it when the CONNECTION_CLOSE arrives.

So close only when both sides are finished with the connection. The usual
pattern:

1. Have the receiver tell you it has read everything, for example by ending
   its side of the stream (see [Streams](streams.md#ending-and-closing)).
2. `await conn.flushed()`. It resolves `true` once the peer has acknowledged
   everything sent so far, FINs included, so your last packets aren't lost
   in flight.
3. `conn.close()`.

`mux.close()` closes all of a multiplexer's connections this way (without
waiting), then its socket.

## Errors

A connection emits `'close'` once. The argument is a `ConnectionClosedError`,
unless you closed it yourself with code 0:

| `err.code` | Meaning |
|---|---|
| `0` (`NoError`) with `err.remote` | The peer closed the connection normally |
| `6` (`ConnectionTimeout`) | Nothing heard from the peer for 30 s. Not remote: we gave up. |
| `1` (`InternalError`) | The peer hit a problem, or no longer has the connection (a stateless reset, `reason` `'stateless reset'`) |
| `4` (`ProtocolViolation`) | The peer broke the protocol, or speaks another UDX version (`reason` lists the versions it supports) |
| anything else | An application code the closing side chose; `err.reason` has its text |

Each of the connection's open streams closes with the same error, so a
`for await` over one of them throws it:

<!-- example: examples/reset.ts#connection-close -->
```ts
const closed = new Promise<void>(resolve => {
  conn.on('close', err => {
    if (err instanceof ConnectionClosedError) {
      console.log(`connection closed by ${err.remote ? 'peer' : 'us'}: code ${err.code}, reason "${err.reason}"`)
    }
    resolve()
  })
})
// The server shuts down, closing every connection with code 0 (NoError).
await server.close()
await closed
```
<!-- /example -->

This prints `connection closed by peer: code 0, reason ""`.
