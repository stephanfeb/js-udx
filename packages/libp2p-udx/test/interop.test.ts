// libp2p over UDX between js-libp2p and go-libp2p (go-libp2p-udx-transport)
// or dart-libp2p, using the peers in tools/. Noise and Yamux on every side;
// ping, identify and an /echo/1.0.0 round trip in both directions.
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { multiaddr } from '@multiformats/multiaddr'
import { createLibp2p, type Libp2p } from 'libp2p'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { type PeerProcess, startPeer } from '../../udx/test/helpers/peer-process.js'
import { UDX, udx } from '../src/index.js'
import { dartLibp2pPeer, goLibp2pPeer, type NativePeer } from './helpers/peers.js'
import type { Stream } from '@libp2p/interface'

const ECHO = '/echo/1.0.0'

type Node = Libp2p<{ ping: ReturnType<ReturnType<typeof ping>>, identify: ReturnType<ReturnType<typeof identify>> }>

async function jsNode (listen: boolean): Promise<Node> {
  const node = await createLibp2p({
    addresses: { listen: listen ? ['/ip4/127.0.0.1/udp/0/udx'] : [] },
    transports: [udx()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: { ping: ping(), identify: identify() }
  })
  await node.handle(ECHO, echo)
  return node
}

/**
 * Echoes until the remote ends, then ends. Not @libp2p/utils' echo(): it
 * pauses for backpressure, and a paused stream's iterator stops at the
 * remote's FIN with data still buffered, which is lost (jsudx-bh8).
 */
async function echo (stream: Stream): Promise<void> {
  for await (const buf of stream) stream.send(buf)
  await stream.close()
}

function pattern (n: number): Uint8Array {
  const b = new Uint8Array(n)
  for (let i = 0; i < n; i++) b[i] = (i * 31 + Math.floor(i / 251)) & 0xff
  return b
}

/**
 * Sends `data` and reads exactly as many bytes back. Not to EOF: dart-libp2p
 * never sees the FIN js-libp2p's Yamux sends (jsudx-aof), so its echo doesn't end.
 */
async function echoRoundTrip (stream: Stream, data: Uint8Array): Promise<Uint8Array> {
  const out = new Uint8Array(data.length)
  let got = 0
  const reading = (async () => {
    for await (const chunk of stream) {
      const bytes = chunk instanceof Uint8Array ? chunk : chunk.subarray()
      out.set(bytes.subarray(0, data.length - got), got)
      got += bytes.length
      if (got >= data.length) break
    }
  })()
  for (let o = 0; o < data.length; o += 64 * 1024) {
    if (!stream.send(data.subarray(o, o + 64 * 1024))) await stream.onDrain()
  }
  await reading
  stream.abort(new Error('echo done'))
  return out.subarray(0, got)
}

for (const native of [goLibp2pPeer, dartLibp2pPeer] satisfies NativePeer[]) {
  describe.skipIf(native.unavailable !== undefined)(`libp2p interop with ${native.name}`, () => {
    let binary: string
    beforeAll(() => { binary = native.build() }, 300_000)

    describe(`js-libp2p dials ${native.name}`, () => {
      let peer: PeerProcess
      let node: Node
      let target: ReturnType<typeof multiaddr>
      beforeAll(async () => {
        peer = startPeer(binary, ['listen'])
        target = multiaddr(await peer.line('READY', 60_000))
        node = await jsNode(false)
      }, 90_000)
      afterAll(async () => {
        await node?.stop()
        peer?.kill()
      })

      it('connects with Noise and Yamux and pings', async () => {
        const conn = await node.dial(target)
        expect(conn.encryption).toBe('/noise')
        expect(conn.multiplexer).toBe('/yamux/1.0.0')
        expect(await node.services.ping.ping(target)).toBeGreaterThanOrEqual(0)
      }, 30_000)

      it('learns its protocols and agent through identify', async () => {
        const id = target.getComponents().find(c => c.name === 'p2p')?.value as string
        await expect.poll(async () => {
          const p = await node.peerStore.get(node.getConnections().find(c => c.remotePeer.toString() === id)?.remotePeer as never)
          return p.protocols
        }, { timeout: 10_000 }).toContain(ECHO)
        const peerInfo = await node.peerStore.get(node.getConnections()[0]?.remotePeer as never)
        expect(peerInfo.protocols).toContain('/ipfs/ping/1.0.0')
        expect(peerInfo.addresses.some(({ multiaddr }) => UDX.exactMatch(multiaddr))).toBe(true)
      }, 30_000)

      it('echoes 1 MiB', async () => {
        const data = pattern(1 << 20)
        const back = await echoRoundTrip(await node.dialProtocol(target, ECHO), data)
        expect(back.length).toBe(data.length)
        expect(Buffer.compare(back, data)).toBe(0)
      }, 60_000)

      // Real time: UDX closes a connection after 30 s without hearing from
      // the peer. Run with UDX_SLOW_TESTS=1. Against dart-libp2p js-libp2p's
      // own connection monitor aborts it first: each heartbeat's ping stream
      // stays half-open, since Dart never sees its FIN (jsudx-aof), and the
      // second heartbeat exceeds ping's one-stream limit.
      const idle = process.env.UDX_SLOW_TESTS === undefined ? it.skip : native === dartLibp2pPeer ? it.fails : it
      idle(`keeps an idle connection open past the UDX idle timeout${native === dartLibp2pPeer ? ' (jsudx-aof)' : ''}`, async () => {
        const conn = await node.dial(target)
        await new Promise(resolve => setTimeout(resolve, 45_000))
        expect(conn.status).toBe('open')
        expect(await node.services.ping.ping(target)).toBeGreaterThanOrEqual(0)
      }, 60_000)

      // The echo ends its side only once it sees ours end.
      const halfClose = native === dartLibp2pPeer ? it.fails : it
      halfClose(`sees ${native.name} end the echo after we end ours${native === dartLibp2pPeer ? ' (jsudx-aof)' : ''}`, async () => {
        const stream = await node.dialProtocol(target, ECHO)
        stream.send(pattern(1000))
        await stream.close({ signal: AbortSignal.timeout(5_000) })
        let got = 0
        for await (const chunk of stream) got += chunk.byteLength
        expect(got).toBe(1000)
      }, 30_000)
    })

    describe(`${native.name} dials js-libp2p`, () => {
      let node: Node
      beforeAll(async () => { node = await jsNode(true) })
      afterAll(async () => { await node?.stop() })

      it('pings, identifies us and echoes 1 MiB', async () => {
        const addr = node.getMultiaddrs().find(ma => UDX.exactMatch(ma))
        expect(addr).toBeDefined()
        const peer = startPeer(binary, ['dial', String(addr), String(1 << 20)])
        try {
          expect(Number(await peer.line('PING', 60_000))).toBeGreaterThanOrEqual(0)
          const protocols = (await peer.line('PROTOCOLS')).split(' ')
          expect(protocols).toContain(ECHO)
          expect(protocols).toContain('/ipfs/ping/1.0.0')
          expect(await peer.line('AGENT')).toMatch(/js-libp2p/)
          expect(Number(await peer.line('RESULT', 60_000))).toBe(1 << 20)
        } finally {
          peer.kill()
        }
      }, 120_000)
    })
  })
}
