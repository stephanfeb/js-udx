// dart-udx parity features: anti-amplification, path validation and
// migration, path MTU discovery, acknowledged pings, connection-level flow
// control, version negotiation and stateless reset.
import { describe, expect, it } from 'vitest'
import {
  ConnectionClosedError,
  ConnectionFlowController,
  ConnectionId,
  ErrorCode,
  type Frame,
  FrameType,
  LOCAL_MAX_DATA,
  MAX_UDP_PAYLOAD_IPV4,
  MTU_SEARCH_STEP,
  ManualClock,
  MemoryNetwork,
  MtuDiscovery,
  type Packet,
  SAFE_DATAGRAM_SIZE,
  type UdxConnection,
  UdxMultiplexer,
  type UdxStream,
  VERSION_CURRENT,
  ackCovers,
  decodePacket,
  decodeVersionNegotiation,
  encodePacket
} from '../src/index.js'
import { advance, collect, equalBytes, memoryPair, pattern, runUntil, writeAll } from './helpers/net.js'

const SERVER = { address: '10.0.0.1', port: 9000 }

/** A hand-driven peer on the memory network, speaking raw packets to the server. */
function rawPeer (net: MemoryNetwork, opts: { address?: string, port?: number } = {}) {
  const sock = net.createSocket(opts.address ?? '10.0.0.9', opts.port ?? 1234)
  const received: Array<{ data: Uint8Array, pkt?: Packet }> = []
  sock.onMessage(d => {
    let pkt: Packet | undefined
    try { pkt = decodePacket(d) } catch { /* not a UDX packet, e.g. version negotiation */ }
    received.push({ data: d, pkt })
  })
  const dcid = new ConnectionId(Uint8Array.of(1, 1, 1, 1, 1, 1, 1, 1))
  const scid = new ConnectionId(Uint8Array.of(2, 2, 2, 2, 2, 2, 2, 2))
  let seq = 0
  return {
    sock,
    received,
    frames: (): Frame[] => received.flatMap(r => r.pkt?.frames ?? []),
    bytesReceived: (): number => received.reduce((n, r) => n + r.data.length, 0),
    send: (frames: Frame[], o: { src?: number, sequence?: number, version?: number } = {}): number => {
      const datagram = encodePacket({ version: o.version ?? VERSION_CURRENT, destinationCid: dcid, sourceCid: scid, sequence: o.sequence ?? seq++, destinationStreamId: 0, sourceStreamId: o.src ?? 0, frames })
      sock.send(datagram, SERVER.port, SERVER.address)
      return datagram.length
    }
  }
}

function serverOn (net: MemoryNetwork, clock: ManualClock, opts: ConstructorParameters<typeof UdxMultiplexer>[1] = {}): UdxMultiplexer {
  return new UdxMultiplexer(net.createSocket(SERVER.address, SERVER.port), { clock, ...opts })
}

/** A server whose process can "crash": its socket goes away without a word to anyone. */
function crashableServer (net: MemoryNetwork, clock: ManualClock, opts: ConstructorParameters<typeof UdxMultiplexer>[1] = {}): { crash: () => Promise<void> } {
  const sock = net.createSocket(SERVER.address, SERVER.port)
  const mux = new UdxMultiplexer(sock, { clock, ...opts })
  mux.on('connection', c => c.on('stream', s => { s.resume() }))
  return { crash: async () => { await sock.close() } }
}

describe('anti-amplification', () => {
  it('limits an acceptor to 3× what it received until the peer proves its address', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const server = serverOn(net, clock, { pmtud: false })
    let stream: UdxStream | undefined
    server.on('connection', c => c.on('stream', s => {
      stream = s
      void writeAll(s, pattern(200_000)) // the server tries to send a lot
    }))

    const peer = rawPeer(net)
    let sentToServer = peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
    sentToServer += peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: Uint8Array.of(1) }], { src: 7 })
    await advance(clock, 3000)
    expect(stream).toBeDefined()
    expect(peer.bytesReceived()).toBeLessThanOrEqual(3 * sentToServer)
    expect(peer.bytesReceived()).toBeGreaterThan(0)

    // Answer the challenge: the limit lifts.
    const challenge = peer.frames().find(f => f.type === FrameType.PathChallenge)
    expect(challenge).toBeDefined()
    if (challenge?.type !== FrameType.PathChallenge) throw new Error('no challenge')
    peer.send([{ type: FrameType.PathResponse, data: Uint8Array.from(challenge.data) }], { sequence: 0 })
    await advance(clock, 50)
    expect(peer.bytesReceived()).toBeGreaterThan(3 * (sentToServer + 50) + 10_000)
  })

  it('validates a js-udx dialer within a round trip', async () => {
    const { connect, clock } = memoryPair()
    const { acceptor } = await connect()
    await runUntil(clock, () => acceptor.addressValidated, 1000)
    expect(acceptor.addressValidated).toBe(true)
  })

  it('can be turned off', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const server = serverOn(net, clock, { antiAmplification: false })
    let conn: UdxConnection | undefined
    server.on('connection', c => { conn = c })
    rawPeer(net).send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
    await advance(clock, 20)
    expect(conn?.addressValidated).toBe(true)
  })
})

describe('path migration', () => {
  it('follows a NAT rebinding once the new address answers its challenge', async () => {
    const { clock, net, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    let received: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { received = collect(s) })
    const migrations: Array<[string, number]> = []
    acceptor.on('migrate', (a, p) => migrations.push([a, p]))

    const s = dialer.openStream()
    s.write(pattern(10_000))
    await runUntil(clock, () => (received?.bytes().length ?? 0) === 10_000)

    net.rebind('10.0.0.2', 9000, { address: '203.0.113.7', port: 61000 })
    void writeAll(s, pattern(200_000, 10_000))
    await runUntil(clock, () => received?.ended() === true, 30_000)
    expect(migrations).toEqual([['203.0.113.7', 61000]])
    expect(acceptor.remoteAddress).toBe('203.0.113.7')
    expect(acceptor.remotePort).toBe(61000)
    expect(received?.bytes().length).toBe(210_000)
  })

  it('does not move for a spoofed packet whose source never answers', async () => {
    const { clock, net, connect } = memoryPair({ delay: 5 })
    const { acceptor, dialer } = await connect()
    // A packet carrying the dialer's connection ID, sent from elsewhere.
    const spoofer = net.createSocket('198.51.100.1', 4444)
    spoofer.send(encodePacket({ version: VERSION_CURRENT, destinationCid: dialer.remoteCid, sourceCid: dialer.localCid, sequence: 0, destinationStreamId: 0, sourceStreamId: 0, frames: [{ type: FrameType.Padding }] }), 9000, '10.0.0.1')
    await advance(clock, 6000)
    expect(acceptor.remoteAddress).toBe('10.0.0.2')
  })
})

describe('path MTU discovery', () => {
  /** Sends `size` bytes on a fresh stream and waits until the acceptor has them all. */
  async function transfer (clock: ManualClock, dialer: UdxConnection, acceptor: UdxConnection, size: number): Promise<void> {
    let received: ReturnType<typeof collect> | undefined
    acceptor.once('stream', s => { received = collect(s) })
    const data = pattern(size)
    void writeAll(dialer.openStream(), data)
    await runUntil(clock, () => received?.ended() === true)
    expect(equalBytes(received?.bytes() as Uint8Array, data)).toBe(true)
  }

  it('starts at the safe size and raises it to the largest the path carries', async () => {
    const { clock, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    expect(dialer.datagramSize).toBe(SAFE_DATAGRAM_SIZE)
    // Each side probes once it has an RTT sample, from data it sent.
    await transfer(clock, dialer, acceptor, 16 << 10)
    await transfer(clock, acceptor, dialer, 16 << 10)
    await advance(clock, 1_000)
    expect(dialer.datagramSize).toBe(MAX_UDP_PAYLOAD_IPV4)
    expect(acceptor.datagramSize).toBe(MAX_UDP_PAYLOAD_IPV4)
  })

  it('settles under a smaller path MTU, within the search step, and sends full packets of that size', async () => {
    const path = 1450
    const { clock, net, connect } = memoryPair({ delay: 5, mtu: path })
    const { dialer, acceptor } = await connect()
    // A probe too large for the path is only found lost once later data is
    // acknowledged: the search follows the traffic.
    await transfer(clock, dialer, acceptor, 1 << 20)
    expect(dialer.datagramSize).toBeLessThanOrEqual(path)
    expect(dialer.datagramSize).toBeGreaterThan(path - MTU_SEARCH_STEP)

    // Data goes out in full packets of the validated size (probes of larger
    // sizes may still be under test).
    let largest = 0
    net.onSend = (d) => {
      if (decodePacket(d).frames.some(f => f.type === FrameType.Stream)) largest = Math.max(largest, d.length)
    }
    await transfer(clock, dialer, acceptor, 100_000)
    expect(largest).toBe(dialer.datagramSize)
  })

  it('falls back to the safe size when the path shrinks under it, and re-sends what was lost in pieces that fit', async () => {
    const { clock, net, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    await transfer(clock, dialer, acceptor, 256 << 10)
    expect(dialer.datagramSize).toBe(MAX_UDP_PAYLOAD_IPV4)

    // A carrier's tunnel after a handover: full-size packets vanish.
    net.conditions = { ...net.conditions, mtu: 1350 }
    await transfer(clock, dialer, acceptor, 256 << 10)
    expect(dialer.datagramSize).toBe(SAFE_DATAGRAM_SIZE)
  })

  it('stays at the safe size against a peer that never acknowledges probes', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const server = serverOn(net, clock, { antiAmplification: false })
    let conn: UdxConnection | undefined
    server.on('connection', c => { conn = c })
    const peer = rawPeer(net)
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: Uint8Array.of(1) }], { src: 7 })
    await advance(clock, 20)
    // Acknowledge the server's data so it has an RTT sample, like go-udx would; never answer probes.
    conn?.openStream().write(Uint8Array.of(1))
    await advance(clock, 20)
    const data = peer.received.find(r => r.pkt?.frames.some(f => f.type === FrameType.Stream))
    peer.send([{ type: FrameType.Ack, largestAcked: data?.pkt?.sequence ?? 0, ackDelay: 0, firstAckRangeLength: 1, ranges: [] }], { sequence: 0 })
    await advance(clock, 60_000)
    expect(peer.frames().some(f => f.type === FrameType.MtuProbe)).toBe(true)
    expect(conn?.datagramSize).toBe(SAFE_DATAGRAM_SIZE)
  })

  it('acknowledges a numbered MTU probe without a PING, as go-udx v0.3.0 sends them', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    serverOn(net, clock, { antiAmplification: false })
    const peer = rawPeer(net)
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
    await advance(clock, 20)
    const probeSeq = peer.send([{ type: FrameType.MtuProbe, size: 1300 }], { sequence: 41 })
    void probeSeq
    await advance(clock, 20)
    const acks = peer.frames().filter(f => f.type === FrameType.Ack)
    expect(acks.some(a => ackCovers(a, 41))).toBe(true)
  })

  it('can be turned off', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const server = serverOn(net, clock, { pmtud: false })
    const client = new UdxMultiplexer(net.createSocket('10.0.0.2', 9000), { clock, pmtud: false })
    const dialer = client.dial(9000, '10.0.0.1')
    void server
    void dialer.ping()
    await advance(clock, 10_000)
    expect(dialer.datagramSize).toBe(SAFE_DATAGRAM_SIZE)
  })
})

describe('MtuDiscovery', () => {
  /** Runs the search against a path that carries `path` bytes; returns the probes sent. */
  function search (d: MtuDiscovery, path: number): number[] {
    const probes: number[] = []
    for (let size = d.nextProbe(); size !== 0; size = d.nextProbe()) {
      probes.push(size)
      if (size <= path) d.onProbeAcked(size)
      else d.onProbeLost(size)
    }
    return probes
  }

  it('starts at the safe size and validates the maximum in one probe on a full path', () => {
    const d = new MtuDiscovery(new ManualClock())
    expect(d.current).toBe(SAFE_DATAGRAM_SIZE)
    expect(search(d, 1500)).toEqual([MAX_UDP_PAYLOAD_IPV4])
    expect(d.current).toBe(MAX_UDP_PAYLOAD_IPV4)
    expect(d.done).toBe(true)
  })

  it('rules a size out only after three losses, and settles within the step', () => {
    const d = new MtuDiscovery(new ManualClock())
    const probes = search(d, 1322)
    expect(probes.slice(0, 3)).toEqual([1472, 1472, 1472])
    expect(d.current).toBeLessThanOrEqual(1322)
    expect(d.current).toBeGreaterThan(1322 - MTU_SEARCH_STEP)
    expect(d.done).toBe(true)
  })

  it('probes one size at a time', () => {
    const d = new MtuDiscovery(new ManualClock())
    expect(d.nextProbe()).toBe(1472)
    expect(d.nextProbe()).toBe(0)
  })

  it('falls back to the safe size after three full-size losses with none acknowledged between', () => {
    const d = new MtuDiscovery(new ManualClock())
    search(d, 1500)
    d.onDataLost(1472)
    d.onDataLost(1472)
    d.onDataAcked(1472) // one got through: the count starts over
    d.onDataLost(1472)
    d.onDataLost(1472)
    expect(d.current).toBe(1472)
    d.onDataLost(1472)
    expect(d.current).toBe(SAFE_DATAGRAM_SIZE)
  })

  it('does not count losses of packets no larger than the safe size', () => {
    const d = new MtuDiscovery(new ManualClock())
    search(d, 1500)
    for (let i = 0; i < 10; i++) d.onDataLost(SAFE_DATAGRAM_SIZE)
    expect(d.current).toBe(1472)
  })

  it('ignores a stale probe result after a black hole, and searches again a minute later', () => {
    const clock = new ManualClock()
    const d = new MtuDiscovery(clock)
    expect(d.nextProbe()).toBe(1472)
    d.onProbeAcked(1472)
    d.blackhole()
    d.onProbeAcked(1472) // an ACK from before the path shrank
    expect(d.current).toBe(SAFE_DATAGRAM_SIZE)
    expect(d.nextProbe()).toBe(0)
    clock.skip(60_000)
    expect(d.nextProbe()).toBe(1472)
  })

  it('starts over from the safe size on reset', () => {
    const d = new MtuDiscovery(new ManualClock())
    search(d, 1500)
    d.reset()
    expect(d.current).toBe(SAFE_DATAGRAM_SIZE)
    expect(d.done).toBe(false)
    expect(d.nextProbe()).toBe(1472)
  })
})

describe('ping', () => {
  it('is acknowledged by a js-udx peer', async () => {
    const { clock, connect } = memoryPair()
    const { dialer } = await connect()
    let ok: boolean | undefined
    void dialer.ping().then(r => { ok = r })
    await runUntil(clock, () => ok !== undefined)
    expect(ok).toBe(true)
  })

  it('resolves false when nothing answers', async () => {
    const { clock, net, connect } = memoryPair()
    const { dialer } = await connect()
    net.conditions = { delay: 5, loss: 1 }
    let ok: boolean | undefined
    void dialer.ping(1000).then(r => { ok = r })
    await runUntil(clock, () => ok !== undefined)
    expect(ok).toBe(false)
  })

  it('answers a dart-udx style PING (fresh sequence) and not a go-udx one (sequence 0)', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    serverOn(net, clock, { antiAmplification: false, pmtud: false })
    const peer = rawPeer(net)
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }], { sequence: 0 })
    await advance(clock, 20)
    const acks = (): number => peer.frames().filter(f => f.type === FrameType.Ack).length
    const before = acks()
    peer.send([{ type: FrameType.Ping }], { sequence: 0 })
    await advance(clock, 50)
    expect(acks()).toBe(before)
    peer.send([{ type: FrameType.Ping }], { sequence: 5 })
    await advance(clock, 50)
    expect(acks()).toBe(before + 1)
    const last = peer.frames().filter(f => f.type === FrameType.Ack).at(-1)
    expect(last?.type === FrameType.Ack && ackCovers(last, 5)).toBe(true)
  })
})

describe('connection flow control (dart-udx semantics: MAX_DATA caps bytes in flight)', () => {
  it('exchanges MAX_DATA when connecting', async () => {
    const { connect, clock } = memoryPair()
    const { dialer, acceptor } = await connect()
    await runUntil(clock, () => dialer.peerMaxData === LOCAL_MAX_DATA, 1000)
    expect(dialer.peerMaxData).toBe(LOCAL_MAX_DATA)
    expect(acceptor.peerMaxData).toBe(LOCAL_MAX_DATA)
  })

  it('caps in-flight bytes and only ever raises the cap', () => {
    const fc = new ConnectionFlowController(1000, 5000)
    expect(fc.canSend(900, 100)).toBe(true)
    expect(fc.canSend(901, 100)).toBe(false)
    fc.updatePeerMaxData(500)
    expect(fc.peerMaxData).toBe(1000)
    fc.updatePeerMaxData(2000)
    expect(fc.canSend(1900, 100)).toBe(true)
  })

  it('answers DATA_BLOCKED with our own MAX_DATA', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    serverOn(net, clock, { antiAmplification: false })
    const peer = rawPeer(net)
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
    await advance(clock, 20)
    const before = peer.frames().filter(f => f.type === FrameType.MaxData).length
    peer.send([{ type: FrameType.DataBlocked, limit: 1 << 20 }], { sequence: 0 })
    await advance(clock, 20)
    const maxData = peer.frames().filter(f => f.type === FrameType.MaxData)
    expect(maxData).toHaveLength(before + 1)
    expect(maxData.at(-1)).toMatchObject({ maxData: LOCAL_MAX_DATA })
  })
})

describe('version negotiation', () => {
  it('answers an unsupported version with the one we speak, CIDs swapped', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    serverOn(net, clock)
    const peer = rawPeer(net)
    peer.send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(64) }], { version: 2 })
    await advance(clock, 20)
    expect(peer.received).toHaveLength(1)
    const vn = decodeVersionNegotiation(peer.received[0]?.data as Uint8Array)
    expect(vn.supportedVersions).toEqual([VERSION_CURRENT])
    expect(vn.destinationCid.key).toBe('0202020202020202')
    expect(vn.sourceCid.key).toBe('0101010101010101')
  })

  it('never answers with more bytes than it received', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    serverOn(net, clock)
    const sock = net.createSocket('10.0.0.9', 1)
    // A minimal "version 2" header with empty CIDs: 6 bytes.
    sock.send(Uint8Array.of(0, 0, 0, 2, 0, 0), SERVER.port, SERVER.address)
    let got = 0
    sock.onMessage(() => got++)
    await advance(clock, 20)
    expect(got).toBe(0)
  })

  it('closes a dial the peer answers without our version', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const fakeServer = net.createSocket(SERVER.address, SERVER.port)
    const { encodeVersionNegotiation } = await import('../src/index.js')
    fakeServer.onMessage((d, from) => {
      const p = decodePacket(d)
      fakeServer.send(encodeVersionNegotiation({ destinationCid: p.sourceCid, sourceCid: p.destinationCid, supportedVersions: [7] }), from.port, from.address)
    })
    const client = new UdxMultiplexer(net.createSocket('10.0.0.2', 9000), { clock })
    const dialer = client.dial(SERVER.port, SERVER.address)
    let err: ConnectionClosedError | undefined
    dialer.on('close', e => { err = e })
    await advance(clock, 50)
    expect(dialer.closed).toBe(true)
    expect(err?.code).toBe(ErrorCode.ProtocolViolation)
  })
})

describe('stateless reset', () => {
  const secret = new Uint8Array(32).fill(9)

  it('tells a peer whose connection a restarted server lost', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const first = crashableServer(net, clock, { statelessResetSecret: secret })
    const client = new UdxMultiplexer(net.createSocket('10.0.0.2', 9000), { clock })
    const dialer = client.dial(SERVER.port, SERVER.address)
    const s = dialer.openStream()
    s.write(pattern(1000))
    await advance(clock, 200)

    // The server restarts with the same secret, losing every connection.
    await first.crash()
    serverOn(net, clock, { statelessResetSecret: secret })
    let err: ConnectionClosedError | undefined
    dialer.on('close', e => { err = e })
    s.write(pattern(100_000)) // the client keeps sending to the lost connection
    await runUntil(clock, () => dialer.closed, 20_000)
    expect(err?.reason).toBe('stateless reset')
    expect(err?.remote).toBe(true)
  })

  it('without a secret, the peer is left to its idle timeout', async () => {
    const clock = new ManualClock(1_000_000)
    const net = new MemoryNetwork(clock, { delay: 5 })
    const first = crashableServer(net, clock)
    const client = new UdxMultiplexer(net.createSocket('10.0.0.2', 9000), { clock })
    const dialer = client.dial(SERVER.port, SERVER.address)
    const s = dialer.openStream()
    s.write(pattern(1000))
    await advance(clock, 200)
    await first.crash()
    serverOn(net, clock)
    s.write(pattern(100_000))
    await advance(clock, 20_000)
    expect(dialer.closed).toBe(false)
    await advance(clock, 20_000)
    expect(dialer.closed).toBe(true)
  })

  it('rejects a secret shorter than 32 bytes', () => {
    const clock = new ManualClock()
    const net = new MemoryNetwork(clock)
    expect(() => new UdxMultiplexer(net.createSocket(), { clock, statelessResetSecret: new Uint8Array(16) })).toThrow(RangeError)
  })
})
