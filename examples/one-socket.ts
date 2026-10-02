// Two peers, each with one UDP socket that both accepts and dials. Shows
// accept()/acceptStream(), several concurrent streams on one connection,
// ping and the connection's statistics.
// Run: node examples/one-socket.ts
import { UdxMultiplexer, type UdxStream } from '@stephanfeb/udx'

// #region peers
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
// #endregion peers

// #region streams
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
// #endregion streams

// #region stats
console.log(`ping acknowledged: ${await conn.ping()}`)
console.log(`rtt ${conn.smoothedRtt} ms, cwnd ${conn.cwnd} bytes, datagrams up to ${conn.datagramSize} bytes`)
// #endregion stats

await alice.close()
await bob.close()
