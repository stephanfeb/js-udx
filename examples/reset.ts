// How resets and closes surface: a stream reset by the peer, and a
// connection closed by the peer with an error code.
// Run: node examples/reset.ts
import { ConnectionClosedError, StreamResetError, dial, listen } from '@stephanfeb/udx'

const REJECTED = 0x42

const server = await listen({ host: '127.0.0.1', port: 0 })
server.on('connection', conn => {
  conn.on('stream', stream => {
    // Refuse the stream: both directions stop, and the peer learns our code.
    stream.reset(REJECTED)
  })
})

const conn = await dial(server.address().port, '127.0.0.1')

// #region stream-reset
const stream = conn.openStream()
stream.write(new TextEncoder().encode('may I?'))
try {
  for await (const _chunk of stream) { /* never reached */ }
} catch (err) {
  if (err instanceof StreamResetError) {
    console.log(`stream reset by ${err.remote ? 'peer' : 'us'}, code 0x${err.code.toString(16)}`)
  }
}
// #endregion stream-reset

// #region connection-close
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
// #endregion connection-close
