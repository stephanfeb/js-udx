// Sends 16 MiB over one stream, waiting for 'drain' whenever `write` says the
// queue is full, while the receiver reads in 1 MiB steps with pauses, then
// closes only once the receiver has read everything.
// Run: node examples/backpressure.ts
import { dial, listen, type UdxStream } from '@stephanfeb/udx'

const TOTAL = 16 * 1024 * 1024

// #region writer
/** Writes `total` bytes in 64 KiB chunks, honouring backpressure. */
async function send (stream: UdxStream, total: number): Promise<void> {
  const chunk = new Uint8Array(64 * 1024).fill(0x2a)
  for (let sent = 0; sent < total; sent += chunk.length) {
    if (!stream.write(chunk)) await drained(stream)
  }
  stream.end()
}

/** Resolves on 'drain', rejects if the stream closes first. */
async function drained (stream: UdxStream): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onDrain = (): void => { stream.off('close', onClose); resolve() }
    const onClose = (err?: Error): void => { stream.off('drain', onDrain); reject(err ?? new Error('stream closed')) }
    stream.once('drain', onDrain)
    stream.once('close', onClose)
  })
}
// #endregion writer

// #region reader
/** Counts what arrives, pausing for 10 ms after every MiB, then ends our side. */
async function receive (stream: UdxStream): Promise<number> {
  let got = 0
  let nextPause = 1024 * 1024
  for await (const chunk of stream) {
    got += chunk.length
    if (got >= nextPause) {
      nextPause += 1024 * 1024
      // While we're not iterating, the stream is paused: arriving bytes are
      // buffered and the sender's window stops reopening.
      await new Promise(resolve => setTimeout(resolve, 10))
    }
  }
  stream.end()
  return got
}
// #endregion reader

const server = await listen({ host: '127.0.0.1', port: 0 })
const received = new Promise<number>(resolve => {
  server.on('connection', conn => {
    conn.on('stream', stream => { void receive(stream).then(resolve) })
  })
})

const conn = await dial(server.address().port, '127.0.0.1')
const started = Date.now()
const stream = conn.openStream()
await send(stream, TOTAL)
// #region close
// `send` returns once everything is queued, not delivered. The receiver ends
// its side after reading the last byte, so its end tells us it has all of it.
for await (const _chunk of stream) { /* the receiver sends nothing back */ }
// Wait for the peer to acknowledge our last packets (here, the ACK of its
// FIN), then close: CONNECTION_CLOSE discards anything still in flight.
await conn.flushed()
conn.close()
// #endregion close

const got = await received
console.log(`received ${got} of ${TOTAL} bytes`)
console.log(`took ${Date.now() - started} ms`)
await server.close()
