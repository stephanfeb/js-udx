// A UDX echo server and a client that talks to it, in one process.
// Run: node examples/echo.ts
import { dial, listen } from '@stephanfeb/udx'

// #region server
const server = await listen({ host: '127.0.0.1', port: 0 })
server.on('connection', conn => {
  conn.on('stream', stream => {
    // Send every chunk back, and end our side when the client ends theirs.
    stream.on('data', chunk => stream.write(chunk))
    stream.on('end', () => stream.end())
  })
})
const { port } = server.address()
// #endregion server

// #region client
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
// #endregion client
