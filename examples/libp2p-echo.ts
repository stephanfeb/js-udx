// Two js-libp2p nodes over UDX: one listens, the other dials it, pings it and
// runs an echo protocol. Run: node examples/libp2p-echo.ts
// #region node
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
// #endregion node

// #region echo
const ECHO = '/echo/1.0.0'

/** Sends back whatever arrives, then ends our side once the dialer ends theirs. */
async function echo (stream: Stream): Promise<void> {
  for await (const chunk of stream) stream.send(chunk)
  await stream.close()
}
// #endregion echo

// #region dial
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
// #endregion dial

await client.stop()
await server.stop()
