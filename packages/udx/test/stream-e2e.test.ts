// Full-stack behaviour on an in-process network on virtual time. Ports the
// intent of go-udx's transfer_test.go, multistream_test.go, stream_window_test.go
// and the connection-level parts of retransmit_recovery_test.go.
import { describe, expect, it } from 'vitest'
import {
  ConnectionClosedError,
  ConnectionId,
  ErrorCode,
  FrameType,
  type LinkConditions,
  StreamResetError,
  type UdxConnection,
  VERSION_CURRENT,
  WriteAfterEndError,
  decodePacket,
  encodePacket
} from '../src/index.js'
import { advance, collect, equalBytes, memoryPair, pattern, runUntil, writeAll } from './helpers/net.js'

/** Echoes every stream back, ending it when the peer ends. */
function echoServer (conn: UdxConnection): void {
  conn.on('stream', s => {
    s.on('data', d => {
      if (!s.write(d)) {
        s.pause()
        s.once('drain', () => s.resume())
      }
    })
    s.on('end', () => s.end())
  })
}

describe('transfers', () => {
  it('moves 4 MiB in one direction intact, with the congestion window in play', async () => {
    const { clock, connect } = memoryPair({ delay: 10 })
    const { dialer, acceptor } = await connect()
    let received: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { received = collect(s) })

    const payload = pattern(4 << 20)
    void writeAll(dialer.openStream(), payload)
    await runUntil(clock, () => received?.ended() === true)
    expect(equalBytes(received?.bytes() as Uint8Array, payload)).toBe(true)
    await advance(clock, 1000) // the last ACKs
    expect(dialer.cwnd).toBeGreaterThan(14720)
    expect(dialer.inflight).toBe(0)
  })

  it('echoes 1 MiB both ways at once', async () => {
    const { clock, connect } = memoryPair({ delay: 10 })
    const { dialer, acceptor } = await connect()
    echoServer(acceptor)
    const payload = pattern(1 << 20)
    const s = dialer.openStream()
    const got = collect(s)
    void writeAll(s, payload)
    await runUntil(clock, () => got.closed())
    expect(got.error()).toBeUndefined()
    expect(equalBytes(got.bytes(), payload)).toBe(true)
  })

  it('keeps 16 concurrent streams apart', async () => {
    const { clock, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    echoServer(acceptor)
    const runs = Array.from({ length: 16 }, (_, i) => {
      const s = dialer.openStream()
      const got = collect(s)
      const payload = pattern(200_000 + i * 997, i * 7919)
      void writeAll(s, payload)
      return { got, payload }
    })
    await runUntil(clock, () => runs.every(r => r.got.closed()))
    for (const r of runs) {
      expect(r.got.error()).toBeUndefined()
      expect(equalBytes(r.got.bytes(), r.payload)).toBe(true)
    }
  })

  it.each<[string, LinkConditions]>([
    ['2% loss', { delay: 20, loss: 0.02 }],
    ['10% loss', { delay: 20, loss: 0.1 }],
    ['25% reordering', { delay: 20, reorder: 0.25, reorderDelay: 8 }],
    ['50 ms delay, 2% loss and 25% reordering', { delay: 50, loss: 0.02, reorder: 0.25, reorderDelay: 10 }]
  ])('delivers four streams intact under %s', async (_name, conditions) => {
    const { clock, net, connect } = memoryPair({ delay: 5 }, 42)
    const { dialer, acceptor } = await connect()
    echoServer(acceptor)
    net.conditions = conditions
    const runs = Array.from({ length: 4 }, (_, i) => {
      const s = dialer.openStream()
      const got = collect(s)
      const payload = pattern(256 * 1024, i * 1000)
      void writeAll(s, payload)
      return { got, payload }
    })
    await runUntil(clock, () => runs.every(r => r.got.closed()), 600_000)
    for (const r of runs) {
      expect(r.got.error()).toBeUndefined()
      expect(equalBytes(r.got.bytes(), r.payload)).toBe(true)
    }
    if ((conditions.loss ?? 0) > 0) expect(net.dropped).toBeGreaterThan(0)
  })

  it('connects even when the connection SYN is lost', async () => {
    const { clock, net, server, client } = memoryPair({ delay: 5 })
    // Lose the first datagram only: the dialer's SYN.
    let datagrams = 0
    net.onSend = () => { net.conditions = { delay: 5, loss: datagrams++ === 0 ? 1 : 0 } }
    let acceptor: UdxConnection | undefined
    void server.accept().then(c => { acceptor = c })
    const dialer = client.dial(9000, '10.0.0.1')
    let established = false
    void dialer.established.then(() => { established = true })
    await runUntil(clock, () => acceptor !== undefined && established, 10_000)
    expect(net.dropped).toBe(1)
    expect(acceptor).toBeDefined()
    expect(established).toBe(true)
  })
})

describe('stream lifecycle', () => {
  it('half-closes: each side ends independently', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    let serverSide: ReturnType<typeof collect> | undefined
    let serverStream: import('../src/index.js').UdxStream | undefined
    acceptor.on('stream', s => { serverStream = s; serverSide = collect(s) })

    const s = dialer.openStream()
    const clientSide = collect(s)
    s.write(Uint8Array.of(1, 2, 3))
    s.end()
    await runUntil(clock, () => serverSide?.ended() === true)
    expect(Array.from(serverSide?.bytes() ?? [])).toEqual([1, 2, 3])
    expect(() => s.write(Uint8Array.of(4))).toThrow(WriteAfterEndError)

    // The server can still write after the client's FIN.
    serverStream?.write(Uint8Array.of(9, 8))
    serverStream?.end()
    await runUntil(clock, () => clientSide.closed() && serverSide?.closed() === true)
    expect(Array.from(clientSide.bytes())).toEqual([9, 8])
    expect(clientSide.error()).toBeUndefined()
    expect(dialer.streamCount).toBe(0)
    expect(acceptor.streamCount).toBe(0)
  })

  it('opens and ends an empty stream (SYN and FIN together)', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    let serverSide: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { serverSide = collect(s) })
    dialer.openStream().end()
    await runUntil(clock, () => serverSide?.ended() === true)
    expect(serverSide?.bytes().length).toBe(0)
  })

  it('propagates a reset with its code', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    let serverSide: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { serverSide = collect(s) })
    const s = dialer.openStream()
    s.write(Uint8Array.of(1))
    await runUntil(clock, () => serverSide !== undefined)
    s.reset(42)
    expect(s.error).toBeInstanceOf(StreamResetError)
    await runUntil(clock, () => serverSide?.closed() === true)
    const err = serverSide?.error() as StreamResetError
    expect(err).toBeInstanceOf(StreamResetError)
    expect(err.code).toBe(42)
    expect(err.remote).toBe(true)
  })

  it('iterates a stream with for await', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    const received: number[] = []
    let done = false
    acceptor.on('stream', s => {
      void (async () => {
        for await (const chunk of s) received.push(...chunk)
        done = true
      })()
    })
    void writeAll(dialer.openStream(), pattern(10_000))
    await runUntil(clock, () => done)
    expect(equalBytes(Uint8Array.from(received), pattern(10_000))).toBe(true)
  })

  it('accepts streams with acceptStream()', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    const accepted = acceptor.acceptStream()
    dialer.openStream().write(Uint8Array.of(7))
    let stream: import('../src/index.js').UdxStream | undefined
    void accepted.then(s => { stream = s })
    await runUntil(clock, () => stream !== undefined)
    expect(stream?.initiator).toBe(false)
    expect(stream?.id).toBe(2) // acceptor IDs are even
  })
})

describe('flow control', () => {
  it('holds a slow reader to its window: buffered bytes never exceed it', async () => {
    const { clock, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    let server: import('../src/index.js').UdxStream | undefined
    acceptor.on('stream', s => { server = s }) // never read: stays paused

    const s = dialer.openStream()
    void writeAll(s, pattern(2 << 20))
    await advance(clock, 5_000)
    expect(server).toBeDefined()
    const srv = server as import('../src/index.js').UdxStream
    expect(srv.bufferedBytes).toBeGreaterThan(0)
    expect(srv.bufferedBytes).toBeLessThanOrEqual(srv.recvWindow)
    expect(s.bytesWritten).toBeLessThanOrEqual(srv.recvWindow)

    // Draining lets the rest through.
    const got = collect(srv)
    await runUntil(clock, () => got.ended())
    expect(equalBytes(got.bytes(), pattern(2 << 20))).toBe(true)
  })

  it('recovers from lost WINDOW_UPDATEs via STREAM_DATA_BLOCKED', async () => {
    const { clock, net, connect } = memoryPair({ delay: 5 })
    const { dialer, acceptor } = await connect()
    let received: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { received = collect(s) })
    // Lose every WINDOW_UPDATE for the first second. The memory network
    // applies conditions per datagram, so set them as each one is sent.
    const until = clock.now() + 1000
    let blockedFrames = 0
    net.onSend = (data) => {
      const pkt = decodePacket(data)
      const isUpdate = pkt.frames.some(f => f.type === FrameType.WindowUpdate)
      if (pkt.frames.some(f => f.type === FrameType.StreamDataBlocked)) blockedFrames++
      net.conditions = { delay: 5, loss: isUpdate && clock.now() < until ? 1 : 0 }
    }
    void writeAll(dialer.openStream(), pattern(1 << 20))
    await runUntil(clock, () => received?.ended() === true, 60_000)
    expect(equalBytes(received?.bytes() as Uint8Array, pattern(1 << 20))).toBe(true)
    expect(blockedFrames).toBeGreaterThan(0)
  })
})

describe('connection lifecycle', () => {
  it('closes the peer and its streams with CONNECTION_CLOSE', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    let serverSide: ReturnType<typeof collect> | undefined
    acceptor.on('stream', s => { serverSide = collect(s) })
    dialer.openStream().write(Uint8Array.of(1))
    await runUntil(clock, () => serverSide !== undefined)

    let closeErr: ConnectionClosedError | undefined
    acceptor.on('close', e => { closeErr = e })
    dialer.close(ErrorCode.ProtocolViolation, 'bye')
    await runUntil(clock, () => acceptor.closed)
    expect(closeErr?.code).toBe(ErrorCode.ProtocolViolation)
    expect(closeErr?.reason).toBe('bye')
    expect(closeErr?.remote).toBe(true)
    expect(serverSide?.error()).toBeInstanceOf(ConnectionClosedError)
    expect(() => dialer.openStream()).toThrow(ConnectionClosedError)
  })

  it('closes silently after the idle timeout and wakes waiting acceptors', async () => {
    const { clock, net, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    net.conditions = { delay: 5, loss: 1 } // the path goes dead
    const pending = acceptor.acceptStream().catch(e => e as Error)
    let dialerErr: ConnectionClosedError | undefined
    dialer.on('close', e => { dialerErr = e })
    let closes = 0
    net.onSend = (data) => {
      if (decodePacket(data).frames.some(f => f.type === FrameType.ConnectionClose)) closes++
    }
    await advance(clock, 31_000)
    expect(dialer.closed).toBe(true)
    expect(acceptor.closed).toBe(true)
    expect(dialerErr?.code).toBe(ErrorCode.ConnectionTimeout)
    expect(await pending).toBeInstanceOf(ConnectionClosedError)
    expect(closes).toBe(0) // silent: no CONNECTION_CLOSE
  })

  it('keeps a connection alive while packets arrive', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()
    for (let i = 0; i < 8; i++) {
      await advance(clock, 10_000)
      dialer.ping()
      acceptor.ping()
    }
    expect(dialer.closed).toBe(false)
    expect(acceptor.closed).toBe(false)
  })

  it('a writer on a dead path fails rather than hangs', async () => {
    const { clock, net, connect } = memoryPair()
    const { dialer } = await connect()
    net.conditions = { delay: 5, loss: 1 }
    const s = dialer.openStream()
    const got = collect(s)
    void writeAll(s, pattern(1 << 20)).catch(() => {})
    await advance(clock, 60_000)
    expect(got.closed()).toBe(true)
    expect(got.error()).toBeInstanceOf(ConnectionClosedError)
  })

  it('stops routing to a closed connection', async () => {
    const { clock, server, client, connect } = memoryPair()
    const { dialer } = await connect()
    expect(server.connectionCount).toBe(1)
    dialer.close()
    await runUntil(clock, () => server.connectionCount === 0)
    expect(client.connectionCount).toBe(0)
  })
})

describe('go-udx stream rules', () => {
  // Hand-crafted packets from a "peer" socket on the memory network.
  async function rawPeer (): Promise<{ send: (frames: import('../src/index.js').Frame[], src: number, seq: number) => void, acceptor: UdxConnection, streams: import('../src/index.js').UdxStream[], received: import('../src/index.js').Packet[], clock: import('../src/index.js').ManualClock }> {
    const { clock, net, server } = memoryPair()
    const peer = net.createSocket('10.0.0.9', 1234)
    const received: import('../src/index.js').Packet[] = []
    peer.onMessage(d => received.push(decodePacket(d)))
    const dcid = new ConnectionId(Uint8Array.of(1, 1, 1, 1, 1, 1, 1, 1))
    const scid = new ConnectionId(Uint8Array.of(2, 2, 2, 2, 2, 2, 2, 2))
    const send = (frames: import('../src/index.js').Frame[], src: number, seq: number): void => {
      peer.send(encodePacket({ version: VERSION_CURRENT, destinationCid: dcid, sourceCid: scid, sequence: seq, destinationStreamId: 0, sourceStreamId: src, frames }), 9000, '10.0.0.1')
    }
    const streams: import('../src/index.js').UdxStream[] = []
    let acceptor: UdxConnection | undefined
    server.on('connection', c => { acceptor = c; c.on('stream', s => streams.push(s)) })
    send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }], 0, 0)
    await runUntil(clock, () => acceptor !== undefined)
    return { send, acceptor: acceptor as UdxConnection, streams, received, clock }
  }

  // dart-udx has no connection SYN: its first datagram is already a stream's
  // SYN. A 'stream' listener attached in the 'connection' handler must still
  // see that stream, so the connection is announced before it handles it.
  it('announces a stream opened by the very first datagram', async () => {
    const { clock, net, server } = memoryPair()
    const peer = net.createSocket('10.0.0.9', 1234)
    const dcid = new ConnectionId(Uint8Array.of(3, 3, 3, 3, 3, 3, 3, 3))
    const scid = new ConnectionId(Uint8Array.of(4, 4, 4, 4, 4, 4, 4, 4))
    const streams: import('../src/index.js').UdxStream[] = []
    server.on('connection', c => { c.on('stream', s => streams.push(s)) })
    peer.send(encodePacket({ version: VERSION_CURRENT, destinationCid: dcid, sourceCid: scid, sequence: 0, destinationStreamId: 0, sourceStreamId: 1, frames: [{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }] }), 9000, '10.0.0.1')
    await runUntil(clock, () => streams.length > 0, 1000)
    expect(streams[0]?.remoteId).toBe(1)
  })

  it('acknowledges the connection SYN at once and opens no stream for it', async () => {
    const { received, streams, clock } = await rawPeer()
    await runUntil(clock, () => received.length > 0)
    const ack = received[0]?.frames[0]
    expect(ack?.type).toBe(FrameType.Ack)
    expect(received[0]?.sequence).toBe(0)
    expect(streams).toHaveLength(0)
  })

  it('opens a stream on data that overtook its SYN', async () => {
    const { send, streams, clock } = await rawPeer()
    send([{ type: FrameType.Stream, fin: false, syn: false, offset: 5, data: Uint8Array.of(6, 7) }], 11, 2)
    await runUntil(clock, () => streams.length > 0)
    const got = collect(streams[0] as import('../src/index.js').UdxStream)
    send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: Uint8Array.of(1, 2, 3, 4, 5) }], 11, 1)
    send([{ type: FrameType.Stream, fin: true, syn: false, offset: 7, data: new Uint8Array(0) }], 11, 3)
    await runUntil(clock, () => got.ended())
    expect(Array.from(got.bytes())).toEqual([1, 2, 3, 4, 5, 6, 7])
    expect(streams).toHaveLength(1)
    expect(streams[0]?.remoteId).toBe(11)
  })

  it('does not open a stream for a bare FIN or RESET', async () => {
    const { send, streams, clock } = await rawPeer()
    send([{ type: FrameType.Stream, fin: true, syn: false, offset: 0, data: new Uint8Array(0) }], 13, 1)
    send([{ type: FrameType.ResetStream, errorCode: 1 }], 15, 0)
    await advance(clock, 100)
    expect(streams).toHaveLength(0)
  })

  it('delivers each byte once across duplicates and overlaps', async () => {
    const { send, streams, clock } = await rawPeer()
    const data = pattern(100)
    send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: data.subarray(0, 40) }], 21, 1)
    send([{ type: FrameType.Stream, fin: false, syn: false, offset: 20, data: data.subarray(20, 70) }], 21, 2)
    send([{ type: FrameType.Stream, fin: false, syn: false, offset: 0, data: data.subarray(0, 40) }], 21, 3)
    send([{ type: FrameType.Stream, fin: false, syn: false, offset: 60, data: data.subarray(60, 100) }], 21, 4)
    send([{ type: FrameType.Stream, fin: true, syn: false, offset: 100, data: new Uint8Array(0) }], 21, 5)
    await runUntil(clock, () => streams.length > 0)
    const got = collect(streams[0] as import('../src/index.js').UdxStream)
    await runUntil(clock, () => got.ended())
    expect(equalBytes(got.bytes(), data)).toBe(true)
  })

  it('addresses replies to the opener by its stream ID', async () => {
    const { send, streams, received, clock } = await rawPeer()
    send([{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: Uint8Array.of(1) }], 31, 1)
    await runUntil(clock, () => streams.length > 0)
    streams[0]?.write(Uint8Array.of(2))
    await runUntil(clock, () => received.some(p => p.frames.some(f => f.type === FrameType.Stream)))
    const reply = received.find(p => p.frames.some(f => f.type === FrameType.Stream))
    expect(reply?.destinationStreamId).toBe(31)
    expect(reply?.sourceStreamId).toBe(2)
    expect(reply?.sequence).toBeGreaterThanOrEqual(0)
    // ACKs ride sequence 0 and are addressed back along the stream.
    const ack = received.find(p => p.frames.some(f => f.type === FrameType.Ack) && p.destinationStreamId === 31)
    expect(ack?.sequence).toBe(0)
  })

  it('answers PATH_CHALLENGE and DATA_BLOCKED', async () => {
    const { send, received, clock } = await rawPeer()
    send([{ type: FrameType.PathChallenge, data: Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8) }], 0, 0)
    send([{ type: FrameType.DataBlocked, limit: 100 }], 0, 0)
    const frames = (): import('../src/index.js').Frame[] => received.flatMap(p => p.frames)
    await runUntil(clock, () => frames().some(f => f.type === FrameType.PathResponse) && frames().filter(f => f.type === FrameType.MaxData).length >= 2)
    const resp = frames().find(f => f.type === FrameType.PathResponse)
    expect(resp).toMatchObject({ data: Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8) })
  })
})
