import { EventEmitter } from 'node:events'
import { ConnectionId } from './cid.js'
import { type Clock, type TimerHandle, realClock } from './clock.js'
import { DEFAULT_CID_LENGTH, ErrorCode, VERSION_CURRENT } from './constants.js'
import { UdxConnection } from './connection.js'
import { type BindOptions, type DatagramSocket, type RemoteInfo, type SocketAddress, bindUdp } from './datagram.js'
import { ConnectionClosedError } from './errors.js'
import { FrameType } from './frames.js'
import { type Packet, decodePacket } from './packet.js'
import type { UdxStreamOptions } from './stream.js'

/** Packets for an unknown connection ID held while its SYN may still be on the way. */
const EARLY_MAX_CIDS = 256
const EARLY_MAX_PACKETS_PER_CID = 32
/** How long early packets are held before being discarded. go-udx never expires them. */
const EARLY_PACKET_TTL = 10_000

export interface UdxMultiplexerOptions {
  clock?: Clock
  streamOptions?: UdxStreamOptions
}

export interface UdxMultiplexerEvents {
  /** A peer opened a connection to us. */
  connection: [connection: UdxConnection]
  close: []
}

interface EarlyPackets {
  firstSeen: number
  packets: Array<{ pkt: Packet, size: number }>
}

/**
 * One UDP socket carrying any number of UDX connections, routed by destination
 * connection ID (not by address). A packet for an unknown ID that carries a
 * STREAM frame with SYN opens a new inbound connection. Port of go-udx's
 * Multiplexer.
 */
export class UdxMultiplexer extends EventEmitter<UdxMultiplexerEvents> {
  readonly clock: Clock
  private readonly socket: DatagramSocket
  private readonly streamOptions: UdxStreamOptions | undefined
  private readonly connections = new Map<string, UdxConnection>()
  private readonly early = new Map<string, EarlyPackets>()
  private readonly backlog: UdxConnection[] = []
  private readonly acceptWaiters: Array<{ resolve: (c: UdxConnection) => void, reject: (e: Error) => void }> = []
  private sweepTimer: TimerHandle | undefined
  private closedFlag = false

  /** Datagrams dropped as undecodable or carrying another version. */
  droppedDatagrams = 0

  constructor (socket: DatagramSocket, opts: UdxMultiplexerOptions = {}) {
    super()
    this.socket = socket
    this.clock = opts.clock ?? realClock
    this.streamOptions = opts.streamOptions
    socket.onMessage((data, from) => this.onDatagram(data, from))
  }

  /** Binds a UDP socket and returns a multiplexer on it. */
  static async create (opts: BindOptions & UdxMultiplexerOptions = {}): Promise<UdxMultiplexer> {
    return new UdxMultiplexer(await bindUdp(opts), opts)
  }

  address (): SocketAddress {
    return this.socket.address()
  }

  get connectionCount (): number {
    return this.connections.size
  }

  get closed (): boolean {
    return this.closedFlag
  }

  /**
   * Opens a connection to `address:port`. The dialer picks both connection IDs
   * and sends a connection SYN at once; it doesn't wait for a reply.
   * `connection.established` resolves when the peer first answers.
   */
  dial (port: number, address: string): UdxConnection {
    if (this.closedFlag) throw new ConnectionClosedError(ErrorCode.NoError, 'multiplexer closed', false)
    let localCid: ConnectionId
    do {
      localCid = ConnectionId.random(DEFAULT_CID_LENGTH)
    } while (this.connections.has(localCid.key))
    const conn = this.createConnection(localCid, ConnectionId.random(DEFAULT_CID_LENGTH), address, port, true)
    conn.sendConnectionSyn()
    return conn
  }

  /** Resolves with the next inbound connection. Connections are also emitted as 'connection'. */
  async accept (): Promise<UdxConnection> {
    const queued = this.backlog.shift()
    if (queued !== undefined) return queued
    if (this.closedFlag) throw new ConnectionClosedError(ErrorCode.NoError, 'multiplexer closed', false)
    return await new Promise((resolve, reject) => this.acceptWaiters.push({ resolve, reject }))
  }

  /** Closes every connection (with CONNECTION_CLOSE) and the socket. */
  async close (): Promise<void> {
    if (this.closedFlag) return
    this.closedFlag = true
    if (this.sweepTimer !== undefined) this.clock.clearTimeout(this.sweepTimer)
    for (const conn of [...this.connections.values()]) conn.close()
    this.connections.clear()
    this.early.clear()
    const err = new ConnectionClosedError(ErrorCode.NoError, 'multiplexer closed', false)
    for (const w of this.acceptWaiters.splice(0)) w.reject(err)
    await this.socket.close()
    this.emit('close')
  }

  private createConnection (localCid: ConnectionId, remoteCid: ConnectionId, address: string, port: number, initiator: boolean): UdxConnection {
    const key = localCid.key
    const conn = new UdxConnection({
      localCid,
      remoteCid,
      remoteAddress: address,
      remotePort: port,
      initiator,
      clock: this.clock,
      send: (data, toAddress, toPort) => this.socket.send(data, toPort, toAddress),
      onClosed: () => {
        if (this.connections.get(key) === conn) this.connections.delete(key)
      },
      streamOptions: this.streamOptions
    })
    this.connections.set(key, conn)
    return conn
  }

  private onDatagram (data: Uint8Array, from: RemoteInfo): void {
    if (this.closedFlag) return
    let pkt: Packet
    try {
      pkt = decodePacket(data)
    } catch {
      this.droppedDatagrams++
      return
    }
    // A version mismatch is dropped, so it looks like an unreachable peer
    // rather than corrupting data: v3 moved bytes a v2 parser would misread.
    if (pkt.version !== VERSION_CURRENT) {
      this.droppedDatagrams++
      return
    }

    const key = pkt.destinationCid.key
    const conn = this.connections.get(key)
    if (conn !== undefined) {
      conn.handlePacket(pkt, data.length)
      return
    }

    const hasSyn = pkt.frames.some(f => f.type === FrameType.Stream && f.syn)
    if (!hasSyn) {
      this.holdEarly(key, pkt, data.length)
      return
    }

    // A new inbound connection: our ID is the one the dialer chose for us.
    const inbound = this.createConnection(pkt.destinationCid, pkt.sourceCid, from.address, from.port, false)
    const early = this.early.get(key)
    this.early.delete(key)
    inbound.handlePacket(pkt, data.length)
    for (const e of early?.packets ?? []) inbound.handlePacket(e.pkt, e.size)

    const waiter = this.acceptWaiters.shift()
    if (waiter !== undefined) {
      waiter.resolve(inbound)
    } else if (this.listenerCount('connection') === 0) {
      this.backlog.push(inbound)
    }
    this.emit('connection', inbound)
  }

  /** Holds a packet that may have overtaken its connection's SYN. */
  private holdEarly (key: string, pkt: Packet, size: number): void {
    let entry = this.early.get(key)
    if (entry === undefined) {
      if (this.early.size >= EARLY_MAX_CIDS) return
      entry = { firstSeen: this.clock.now(), packets: [] }
      this.early.set(key, entry)
      this.armSweep()
    }
    if (entry.packets.length < EARLY_MAX_PACKETS_PER_CID) entry.packets.push({ pkt, size })
  }

  private armSweep (): void {
    if (this.sweepTimer !== undefined) return
    this.sweepTimer = this.clock.setTimeout(() => {
      this.sweepTimer = undefined
      const now = this.clock.now()
      for (const [key, entry] of this.early) {
        if (now - entry.firstSeen >= EARLY_PACKET_TTL) this.early.delete(key)
      }
      if (this.early.size > 0) this.armSweep()
    }, EARLY_PACKET_TTL)
  }
}

/** Binds a UDP socket for accepting (and dialing) UDX connections. */
export async function listen (opts: BindOptions & UdxMultiplexerOptions = {}): Promise<UdxMultiplexer> {
  return await UdxMultiplexer.create(opts)
}

/**
 * Dials `address:port` from a new ephemeral socket, which closes when the
 * connection does.
 */
export async function dial (port: number, address: string, opts: UdxMultiplexerOptions & Omit<BindOptions, 'port' | 'host'> = {}): Promise<UdxConnection> {
  const mux = await UdxMultiplexer.create({ ...opts, type: opts.type ?? (address.includes(':') ? 'udp6' : 'udp4') })
  const conn = mux.dial(port, address)
  conn.once('close', () => { void mux.close() })
  return conn
}
