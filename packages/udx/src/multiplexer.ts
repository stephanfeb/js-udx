import { EventEmitter } from 'node:events'
import { ConnectionId } from './cid.js'
import { type Clock, type TimerHandle, realClock } from './clock.js'
import { isIPv6 } from 'node:net'
import { toHex } from './bytes.js'
import { DEFAULT_CID_LENGTH, ErrorCode, MAX_UDP_PAYLOAD_IPV4, MAX_UDP_PAYLOAD_IPV6, MIN_STATELESS_RESET_PACKET_SIZE, VERSION_CURRENT } from './constants.js'
import { UdxConnection } from './connection.js'
import { type BindOptions, type DatagramSocket, type RemoteInfo, type SocketAddress, bindUdp } from './datagram.js'
import { ConnectionClosedError } from './errors.js'
import { FrameType } from './frames.js'
import { type Packet, type PacketHeader, decodePacket, peekHeader } from './packet.js'
import { candidateResetToken, encodeStatelessReset, statelessResetToken } from './stateless-reset.js'
import { decodeVersionNegotiation, encodeVersionNegotiation } from './version.js'
import type { UdxStreamOptions } from './stream.js'

/** Packets for an unknown connection ID held while its SYN may still be on the way. */
const EARLY_MAX_CIDS = 256
const EARLY_MAX_PACKETS_PER_CID = 32
/** How long early packets are held before being discarded. go-udx never expires them. */
const EARLY_PACKET_TTL = 10_000
/**
 * A connection ID still without a SYN after this long is taken to be one we
 * lost (a restart), and answered with a stateless reset. Shorter would reset
 * connections whose SYN is merely slow: its first retransmission comes after
 * about 1 s, the second 2 s later.
 */
const STATELESS_RESET_AFTER = 3_000
/** Most version negotiation and stateless reset replies sent per second. */
const STATELESS_REPLIES_PER_SECOND = 10

export interface UdxMultiplexerOptions {
  clock?: Clock
  streamOptions?: UdxStreamOptions
  /** See UdxConnectionOptions. Defaults: all true. */
  antiAmplification?: boolean
  migration?: boolean
  pmtud?: boolean
  /** Answer packets with an unsupported version with a version negotiation packet. Default true. */
  versionNegotiation?: boolean
  /**
   * Enables stateless reset (RFC 9000 §10.3): each connection tells its peer
   * a token derived from this secret, and packets for connection IDs this
   * multiplexer doesn't know are answered with a reset, so peers of a
   * restarted process learn at once that their connection is gone. Must be at
   * least 32 bytes and survive restarts to be useful.
   */
  statelessResetSecret?: Uint8Array
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
  private readonly connOptions: Pick<UdxMultiplexerOptions, 'antiAmplification' | 'migration' | 'pmtud'>
  private readonly versionNegotiation: boolean
  private readonly resetSecret: Uint8Array | undefined
  /** Peers' stateless reset tokens (hex) → the connection they would reset. */
  private readonly resetTokens = new Map<string, UdxConnection>()
  private replyBudget = STATELESS_REPLIES_PER_SECOND
  private replyBudgetAt = 0
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
    this.connOptions = { antiAmplification: opts.antiAmplification, migration: opts.migration, pmtud: opts.pmtud }
    this.versionNegotiation = opts.versionNegotiation ?? true
    if (opts.statelessResetSecret !== undefined) statelessResetToken(opts.statelessResetSecret, ConnectionId.EMPTY) // validates the length
    this.resetSecret = opts.statelessResetSecret
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
    const tokens: string[] = []
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
        for (const t of tokens) if (this.resetTokens.get(t) === conn) this.resetTokens.delete(t)
      },
      onResetToken: (token) => {
        const t = toHex(token)
        tokens.push(t)
        this.resetTokens.set(t, conn)
      },
      resetToken: this.resetSecret !== undefined ? statelessResetToken(this.resetSecret, localCid) : undefined,
      streamOptions: this.streamOptions,
      ...this.connOptions,
      maxDatagramSize: isIPv6(address) ? MAX_UDP_PAYLOAD_IPV6 : MAX_UDP_PAYLOAD_IPV4
    })
    this.connections.set(key, conn)
    return conn
  }

  private onDatagram (data: Uint8Array, from: RemoteInfo): void {
    if (this.closedFlag) return
    const header = peekHeader(data)
    if (header === undefined) {
      this.unroutable(data)
      return
    }
    if (header.version === 0) {
      this.onVersionNegotiation(header, data)
      return
    }
    // A version mismatch is dropped, so it looks like an unreachable peer
    // rather than corrupting data: v3 moved bytes a v2 parser would misread.
    if (header.version !== VERSION_CURRENT) {
      if (!this.unroutable(data)) this.sendVersionNegotiation(header, data, from)
      return
    }

    let pkt: Packet
    try {
      pkt = decodePacket(data)
    } catch {
      this.unroutable(data)
      return
    }

    const key = pkt.destinationCid.key
    const conn = this.connections.get(key)
    if (conn !== undefined) {
      conn.handlePacket(pkt, data.length, from.address, from.port)
      return
    }

    const hasSyn = pkt.frames.some(f => f.type === FrameType.Stream && f.syn)
    if (!hasSyn) {
      if (!this.unroutable(data)) this.holdEarly(key, pkt, data, from)
      return
    }

    // A new inbound connection: our ID is the one the dialer chose for us.
    const inbound = this.createConnection(pkt.destinationCid, pkt.sourceCid, from.address, from.port, false)
    const early = this.early.get(key)
    this.early.delete(key)
    // Announce the connection before it handles anything: a dart-udx dialer's
    // first datagram already opens a stream, and 'stream' listeners attached
    // in a 'connection' handler must see it.
    const waiter = this.acceptWaiters.shift()
    if (waiter !== undefined) {
      waiter.resolve(inbound)
    } else if (this.listenerCount('connection') === 0) {
      this.backlog.push(inbound)
    }
    this.emit('connection', inbound)

    inbound.handlePacket(pkt, data.length, from.address, from.port)
    inbound.start()
    for (const e of early?.packets ?? []) inbound.handlePacket(e.pkt, e.size, from.address, from.port)
  }

  /**
   * A datagram that can't be routed to a connection may be a stateless reset
   * from a peer that lost our connection: its last 16 bytes match a token
   * the peer gave us. Returns true if it was one.
   */
  private unroutable (data: Uint8Array): boolean {
    this.droppedDatagrams++
    if (this.resetTokens.size === 0) return false
    const token = candidateResetToken(data)
    if (token === undefined) return false
    const conn = this.resetTokens.get(toHex(token))
    if (conn === undefined) return false
    conn.onStatelessReset()
    return true
  }

  /** A version negotiation packet: the peer can't speak our version. */
  private onVersionNegotiation (header: PacketHeader, data: Uint8Array): void {
    const conn = this.connections.get(header.destinationCid.key)
    if (conn === undefined) {
      this.unroutable(data)
      return
    }
    try {
      conn.onVersionNegotiation(decodeVersionNegotiation(data).supportedVersions)
    } catch {
      this.droppedDatagrams++
    }
  }

  /**
   * Tells a peer speaking another version which one we speak. Only for
   * connection IDs we don't know, never larger than the packet that prompted
   * it (so it can't amplify), and rate-limited.
   */
  private sendVersionNegotiation (header: PacketHeader, data: Uint8Array, from: RemoteInfo): void {
    if (!this.versionNegotiation || this.connections.has(header.destinationCid.key)) return
    const reply = encodeVersionNegotiation({
      destinationCid: header.sourceCid,
      sourceCid: header.destinationCid,
      // Only the version we can parse; go-udx and dart-udx also list 2 and 1.
      supportedVersions: [VERSION_CURRENT]
    })
    if (reply.length > data.length || !this.takeReplyBudget()) return
    this.socket.send(reply, from.port, from.address)
  }

  private takeReplyBudget (): boolean {
    const now = this.clock.now()
    if (now - this.replyBudgetAt >= 1000) {
      this.replyBudget = STATELESS_REPLIES_PER_SECOND
      this.replyBudgetAt = now
    }
    if (this.replyBudget <= 0) return false
    this.replyBudget--
    return true
  }

  /**
   * Holds a packet that may have overtaken its connection's SYN. If its
   * connection ID has gone unclaimed for a while, it is more likely one we
   * lost, and with stateless reset enabled the sender is told so.
   */
  private holdEarly (key: string, pkt: Packet, data: Uint8Array, from: RemoteInfo): void {
    const now = this.clock.now()
    let entry = this.early.get(key)
    if (entry === undefined) {
      if (this.early.size >= EARLY_MAX_CIDS) return
      entry = { firstSeen: now, packets: [] }
      this.early.set(key, entry)
      this.armSweep()
    }
    if (entry.packets.length < EARLY_MAX_PACKETS_PER_CID) entry.packets.push({ pkt, size: data.length })
    if (this.resetSecret !== undefined && now - entry.firstSeen >= STATELESS_RESET_AFTER) {
      this.sendStatelessReset(pkt.destinationCid, data.length, from)
    }
  }

  /** Smaller than the packet that prompted it, so two endpoints can't loop. */
  private sendStatelessReset (cid: ConnectionId, triggerSize: number, from: RemoteInfo): void {
    if (this.resetSecret === undefined || triggerSize <= MIN_STATELESS_RESET_PACKET_SIZE || !this.takeReplyBudget()) return
    const token = statelessResetToken(this.resetSecret, cid)
    const size = Math.max(MIN_STATELESS_RESET_PACKET_SIZE, Math.min(triggerSize - 1, 64))
    const reset = encodeStatelessReset(token, size)
    reset[0] = (reset[0] as number) | 0x80 // never reads as version 0–3
    this.socket.send(reset, from.port, from.address)
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
