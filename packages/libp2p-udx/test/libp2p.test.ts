// Two js-libp2p nodes over UDX with the stack go-libp2p-udx-transport and
// dart-libp2p use: Noise, then Yamux, on each connection's first UDX stream.
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { ping } from '@libp2p/ping'
import { multiaddr, type Multiaddr } from '@multiformats/multiaddr'
import { createLibp2p, type Libp2p } from 'libp2p'
import { afterEach, describe, expect, it } from 'vitest'
import { UDX, udx, type UdxTransportInit } from '../src/index.js'
import type { Stream } from '@libp2p/interface'

const ECHO = '/js-udx/echo/1.0.0'

async function node (init?: UdxTransportInit): Promise<Libp2p<{ ping: ReturnType<ReturnType<typeof ping>>, identify: ReturnType<ReturnType<typeof identify>> }>> {
  const n = await createLibp2p({
    addresses: { listen: ['/ip4/127.0.0.1/udp/0/udx'] },
    transports: [udx(init)],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: { ping: ping(), identify: identify() }
  })
  await n.handle(ECHO, echo)
  return n
}

function addr (n: Libp2p): Multiaddr {
  const ma = n.getMultiaddrs()[0]
  if (ma === undefined) throw new Error('not listening')
  return ma
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

function pattern (n: number, seed = 0): Uint8Array {
  const b = new Uint8Array(n)
  for (let i = 0; i < n; i++) b[i] = ((i + seed) * 31 + Math.floor((i + seed) / 251)) & 0xff
  return b
}

/** Sends `data` and reads the echo back. */
async function roundTrip (stream: Stream, data: Uint8Array): Promise<Uint8Array> {
  const out = new Uint8Array(data.length)
  let got = 0
  const reading = (async () => {
    for await (const chunk of stream) {
      const bytes = chunk instanceof Uint8Array ? chunk : chunk.subarray()
      out.set(bytes, got)
      got += bytes.length
    }
  })()
  for (let o = 0; o < data.length; o += 64 * 1024) {
    if (!stream.send(data.subarray(o, o + 64 * 1024))) await stream.onDrain()
  }
  await stream.close()
  await reading
  return out.subarray(0, got)
}

describe('libp2p over UDX', () => {
  const nodes: Libp2p[] = []
  afterEach(async () => {
    await Promise.all(nodes.splice(0).map(async n => { await n.stop() }))
  })

  async function pair (init?: UdxTransportInit): Promise<[Awaited<ReturnType<typeof node>>, Awaited<ReturnType<typeof node>>]> {
    const a = await node(init)
    const b = await node(init)
    nodes.push(a, b)
    return [a, b]
  }

  it('listens on /udp/<port>/udx', async () => {
    const [a] = await pair()
    const addrs = a.getMultiaddrs()
    expect(addrs.length).toBeGreaterThan(0)
    for (const ma of addrs) expect(UDX.exactMatch(ma), ma.toString()).toBe(true)
  })

  it('connects with Noise and Yamux, then pings and identifies', async () => {
    const [a, b] = await pair()
    const conn = await a.dial(addr(b))
    expect(conn.remotePeer.equals(b.peerId)).toBe(true)
    expect(conn.encryption).toBe('/noise')
    expect(conn.multiplexer).toBe('/yamux/1.0.0')
    expect(await a.services.ping.ping(b.peerId)).toBeGreaterThanOrEqual(0)
    // Identify ran on connect: b's UDX listen address is in a's peer store.
    const peer = await a.peerStore.get(b.peerId)
    expect(peer.addresses.some(({ multiaddr }) => UDX.exactMatch(multiaddr))).toBe(true)
  })

  it('echoes 4 MiB on one protocol stream', async () => {
    const [a, b] = await pair()
    const stream = await a.dialProtocol(addr(b), ECHO)
    const data = pattern(4 << 20)
    const back = await roundTrip(stream, data)
    expect(back.length).toBe(data.length)
    expect(Buffer.compare(back, data)).toBe(0)
  }, 60_000)

  it('carries 16 concurrent Yamux streams on one UDX connection', async () => {
    const [a, b] = await pair()
    const ma = addr(b)
    const results = await Promise.all(Array.from({ length: 16 }, async (_, i) => {
      const data = pattern(64 << 10, i * 1000)
      const back = await roundTrip(await a.dialProtocol(ma, ECHO), data)
      return Buffer.compare(back, data) === 0
    }))
    expect(results.every(Boolean)).toBe(true)
    expect(a.getConnections(b.peerId)).toHaveLength(1)
  }, 60_000)

  it('closes the connection at both ends', async () => {
    const [a, b] = await pair()
    const conn = await a.dial(addr(b))
    await a.services.ping.ping(b.peerId)
    await conn.close()
    expect(conn.status).toBe('closed')
    await expect.poll(() => b.getConnections(a.peerId).length, { timeout: 10_000 }).toBe(0)
  })

  it('closes inbound connections when the listener stops', async () => {
    const [a, b] = await pair()
    await a.dial(addr(b))
    await b.stop()
    await expect.poll(() => a.getConnections(b.peerId).length, { timeout: 10_000 }).toBe(0)
  })

  // Real time: UDX closes a connection after 30 s without hearing from the
  // peer, and libp2p's Yamux keep-alive isn't guaranteed to beat it. Run with
  // UDX_SLOW_TESTS=1.
  it.runIf(process.env.UDX_SLOW_TESTS !== undefined)('keeps an idle connection open past the UDX idle timeout', async () => {
    const [a, b] = await pair()
    const conn = await a.dial(addr(b))
    await new Promise(resolve => setTimeout(resolve, 45_000))
    expect(conn.status).toBe('open')
    expect(await a.services.ping.ping(b.peerId)).toBeGreaterThanOrEqual(0)
  }, 60_000)

  it('fails a dial to a port nobody listens on when the signal fires', async () => {
    const [a] = await pair()
    const started = Date.now()
    await expect(a.dial(multiaddr('/ip4/127.0.0.1/udp/9/udx'), { signal: AbortSignal.timeout(500) })).rejects.toThrow()
    expect(Date.now() - started).toBeLessThan(5_000)
  })
})
