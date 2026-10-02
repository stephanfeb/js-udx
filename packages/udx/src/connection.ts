import { randomFillSync } from 'node:crypto'
import { EventEmitter } from 'node:events'
import { AckScheduler } from './ack-scheduler.js'
import { ackCovers } from './ack-tracker.js'
import type { ConnectionId } from './cid.js'
import type { Clock, TimerHandle } from './clock.js'
import {
  AMPLIFICATION_FACTOR,
  ErrorCode,
  INITIAL_MAX_DATA,
  INITIAL_MAX_STREAMS,
  LOCAL_MAX_DATA,
  MAX_DATAGRAM_SIZE,
  MAX_IDLE_TIMEOUT,
  MAX_INCOMING_STREAMS,
  MAX_UDP_PAYLOAD_IPV4,
  PACING_GRANULARITY,
  PATH_VALIDATION_TIMEOUT,
  VERSION_CURRENT
} from './constants.js'
import { ConnectionClosedError, UdxError } from './errors.js'
import { ConnectionFlowController } from './flow-control.js'
import { type AckFrame, type Frame, FrameType, STREAM_FRAME_HEADER_LENGTH, type StreamFrame } from './frames.js'
import { type Packet, encodePacket, packetHeaderLength } from './packet.js'
import { PmtuSearch } from './pmtud.js'
import { LossRecovery } from './recovery.js'
import type { SentPacket } from './sent-packets.js'
import { type StreamHost, UdxStream, type UdxStreamOptions } from './stream.js'

/** Payload per STREAM frame before path MTU discovery: MAX_DATAGRAM_SIZE − 100, as go-udx sends. */
export const STREAM_CHUNK_SIZE = MAX_DATAGRAM_SIZE - 100

/** How often the idle watchdog checks for silence. */
const IDLE_CHECK_INTERVAL = 1000

/** Datagrams held back by the amplification limit; beyond this they are dropped (loss recovery resends data). */
const MAX_AMPLIFICATION_QUEUE = 256

/** Path challenges sent to prove the peer's address before giving up on proving it. */
const ADDRESS_CHALLENGE_ATTEMPTS = 4

export interface UdxConnectionEvents {
  /** The peer opened a stream. */
  stream: [stream: UdxStream]
  /** The peer's address changed and the new path was validated. */
  migrate: [address: string, port: number]
  /** The connection closed; `error` is unset for a local close with code 0. */
  close: [error?: ConnectionClosedError]
}

export interface UdxConnectionOptions {
  localCid: ConnectionId
  remoteCid: ConnectionId
  remoteAddress: string
  remotePort: number
  /** True for the dialer: it opens odd stream IDs, the acceptor even ones. */
  initiator: boolean
  clock: Clock
  /** Puts a datagram on the wire. */
  send: (datagram: Uint8Array, address: string, port: number) => void
  /** Called once when the connection closes, so the multiplexer can forget it. */
  onClosed?: () => void
  /** Called with a stateless reset token the peer gave for its connection ID. */
  onResetToken?: (token: Uint8Array) => void
  /** Our stateless reset token, sent to the peer in NEW_CONNECTION_ID. */
  resetToken?: Uint8Array
  streamOptions?: UdxStreamOptions
  /**
   * Limit an acceptor to sending 3× what it has received until the peer
   * proves its address with a PATH_RESPONSE (RFC 9000 §8.1). Default true.
   * Dialers know the address they dialed and are never limited.
   */
  antiAmplification?: boolean
  /** Follow the peer to a new address after validating it (RFC 9000 §9). Default true. */
  migration?: boolean
  /** Probe for a larger path MTU (RFC 8899). Default true. */
  pmtud?: boolean
  /** Largest UDP payload to probe for. Default 1472 (a 1500-byte IPv4 MTU). */
  maxDatagramSize?: number
  /**
   * Send a PING after this many milliseconds without hearing from the peer,
   * so an idle connection outlives the idle timeout at both ends. Default 0
   * (off): UDX itself doesn't keep idle connections open.
   */
  keepAliveInterval?: number
}

interface PathChallenge {
  data: Uint8Array
  address: string
  port: number
  timer: TimerHandle
}

/**
 * A UDX connection to one peer: streams multiplexed over one path, with
 * reliability, congestion control and flow control. Port of go-udx's
 * Connection, plus dart-udx's path validation, migration, anti-amplification,
 * path MTU discovery and connection-level flow control.
 *
 * Node runs it on one thread: inbound packets are handled synchronously, and
 * sends are batched by a flush scheduled as a microtask.
 */
export class UdxConnection extends EventEmitter<UdxConnectionEvents> {
  readonly localCid: ConnectionId
  readonly remoteCid: ConnectionId
  readonly initiator: boolean
  readonly clock: Clock
  /** Resolves when the first packet arrives from the peer. */
  readonly established: Promise<void>

  private remoteAddr: string
  private remotePortNum: number
  private readonly sendDatagram: (datagram: Uint8Array, address: string, port: number) => void
  private readonly onClosed: (() => void) | undefined
  private readonly onResetToken: ((token: Uint8Array) => void) | undefined
  private readonly resetToken: Uint8Array | undefined
  private readonly streamOptions: UdxStreamOptions | undefined
  private readonly migrationEnabled: boolean

  private readonly recovery: LossRecovery
  private readonly acks: AckScheduler
  private readonly connFc = new ConnectionFlowController(INITIAL_MAX_DATA, LOCAL_MAX_DATA)
  private dataBlockedSentAt = -Infinity

  private readonly streams = new Map<number, UdxStream>()
  private readonly streamsByRemoteId = new Map<number, UdxStream>()
  private readonly sendOrder: UdxStream[] = []
  private nextStreamId: number
  private maxStreams = INITIAL_MAX_STREAMS
  private activeStreams = 0
  private activeIncoming = 0

  private readonly backlog: UdxStream[] = []
  private readonly acceptWaiters: Array<{ resolve: (s: UdxStream) => void, reject: (e: Error) => void }> = []

  private flushScheduled = false
  private pacerTimer: TimerHandle | undefined
  private idleTimer: TimerHandle | undefined
  private readonly keepAliveInterval: number
  private keepAlivePending = false
  private readonly flushWaiters: Array<(flushed: boolean) => void> = []
  private lastActivity: number
  private establishedResolve: (() => void) | undefined
  private isEstablished = false

  // Anti-amplification (acceptor only, until the address is proven).
  private validated: boolean
  private ampReceived = 0
  private ampSent = 0
  private readonly ampQueue: Uint8Array[] = []
  private addressChallenge: PathChallenge | undefined
  private addressChallengeAttempts = 0

  // Migration: a challenge to a new address the peer's packets came from.
  private pathChallenge: PathChallenge | undefined

  // Path MTU discovery.
  private readonly pmtudEnabled: boolean
  private readonly maxDatagram: number
  private pmtu: PmtuSearch | undefined
  private probe: { sequence: number, size: number, timer: TimerHandle } | undefined

  // Acknowledged pings waiting to resolve.
  private readonly pings = new Map<number, { resolve: (ok: boolean) => void, timer: TimerHandle }>()

  private closedError: ConnectionClosedError | undefined
  private closedFlag = false

  bytesSent = 0
  bytesReceived = 0

  private readonly streamHost: StreamHost

  constructor (opts: UdxConnectionOptions) {
    super()
    this.localCid = opts.localCid
    this.remoteCid = opts.remoteCid
    this.remoteAddr = opts.remoteAddress
    this.remotePortNum = opts.remotePort
    this.initiator = opts.initiator
    this.clock = opts.clock
    this.sendDatagram = opts.send
    this.onClosed = opts.onClosed
    this.onResetToken = opts.onResetToken
    this.resetToken = opts.resetToken
    this.streamOptions = opts.streamOptions
    this.migrationEnabled = opts.migration ?? true
    this.pmtudEnabled = opts.pmtud ?? true
    this.maxDatagram = opts.maxDatagramSize ?? MAX_UDP_PAYLOAD_IPV4
    this.validated = opts.initiator || opts.antiAmplification === false
    this.nextStreamId = opts.initiator ? 1 : 2
    this.lastActivity = this.clock.now()
    this.keepAliveInterval = opts.keepAliveInterval ?? 0
    this.established = new Promise<void>(resolve => { this.establishedResolve = resolve })
    // Nobody may await it; a close before establishment must not surface as unhandled.
    this.established.catch(() => {})

    this.recovery = new LossRecovery({ clock: this.clock, retransmit: (pkt, seq) => this.retransmit(pkt, seq) })
    this.acks = new AckScheduler({
      clock: this.clock,
      smoothedRtt: () => this.recovery.congestion.smoothedRtt,
      sendAck: (frame, route) => this.sendControl(route.destinationStreamId, route.sourceStreamId, [frame])
    })

    this.streamHost = {
      clock: this.clock,
      sendControl: (dst, src, frames) => this.sendControl(dst, src, frames),
      scheduleFlush: () => this.scheduleFlush(),
      streamClosed: (s) => this.onStreamClosed(s)
    }

    this.armIdleCheck()
  }

  // --- Public API ---

  get remoteAddress (): string { return this.remoteAddr }
  get remotePort (): number { return this.remotePortNum }
  get closed (): boolean { return this.closedFlag }
  get smoothedRtt (): number { return this.recovery.congestion.smoothedRtt }
  get cwnd (): number { return this.recovery.congestion.cwnd }
  get inflight (): number { return this.recovery.congestion.inflight }
  get streamCount (): number { return this.activeStreams }
  /** Whether the peer's address is proven (always true for a dialer). */
  get addressValidated (): boolean { return this.validated }
  /** The largest datagram this connection sends; raised by path MTU discovery. */
  get datagramSize (): number { return this.pmtu?.current ?? this.baseDatagramSize() }
  /** The peer's limit on our bytes in flight (MAX_DATA). */
  get peerMaxData (): number { return this.connFc.peerMaxData }

  /**
   * Opens a stream. It announces itself with a SYN at once, which a write in
   * the same tick rides along with.
   */
  openStream (): UdxStream {
    if (this.closedFlag) throw this.closedError ?? new ConnectionClosedError(ErrorCode.NoError, '', false)
    if (this.activeStreams >= this.maxStreams) throw new UdxError(ErrorCode.StreamLimitError, 'stream limit exceeded')
    const s = new UdxStream(this.streamHost, this.allocateStreamId(), 0, true, this.streamOptions)
    this.addStream(s)
    this.scheduleFlush() // its SYN
    return s
  }

  /** Resolves with the next stream the peer opens. Streams are also emitted as 'stream'. */
  async acceptStream (): Promise<UdxStream> {
    const queued = this.backlog.shift()
    if (queued !== undefined) return queued
    if (this.closedFlag) throw this.closedError ?? new ConnectionClosedError(ErrorCode.NoError, '', false)
    return await new Promise((resolve, reject) => this.acceptWaiters.push({ resolve, reject }))
  }

  /**
   * Sends a PING under a fresh sequence and resolves true when the peer
   * acknowledges it, false on timeout or close. go-udx acknowledges PINGs
   * from its fix/ack-pings on; against an older go-udx this always resolves
   * false (the PING still counts as activity at the peer).
   */
  async ping (timeoutMs = 5000): Promise<boolean> {
    if (this.closedFlag) return false
    const sequence = this.ackElicitingSequence()
    this.write(this.encode(sequence, 0, 0, [{ type: FrameType.Ping }]))
    return await new Promise<boolean>(resolve => {
      const timer = this.clock.setTimeout(() => {
        this.pings.delete(sequence)
        resolve(false)
      }, timeoutMs)
      this.pings.set(sequence, { resolve, timer })
    })
  }

  /**
   * Resolves true once the peer has acknowledged everything written so far,
   * FINs included, and nothing is left to send; false if the connection
   * closes first. A graceful close waits for this before CONNECTION_CLOSE,
   * which would otherwise discard data still in flight.
   */
  async flushed (): Promise<boolean> {
    if (this.closedFlag) return false
    if (this.isFlushed()) return true
    return await new Promise<boolean>(resolve => this.flushWaiters.push(resolve))
  }

  private isFlushed (): boolean {
    if (this.recovery.congestion.inflight > 0) return false
    for (const s of this.streams.values()) if (s.hasSendable()) return false
    return true
  }

  private checkFlushed (): void {
    if (this.flushWaiters.length === 0 || !this.isFlushed()) return
    for (const w of this.flushWaiters.splice(0)) w(true)
  }

  /** Closes the connection, telling the peer with CONNECTION_CLOSE. */
  close (code: number = ErrorCode.NoError, reason = ''): void {
    if (this.closedFlag) return
    this.sendControl(0, 0, [{ type: FrameType.ConnectionClose, errorCode: code, frameType: 0, reason }])
    this.teardown(new ConnectionClosedError(code, reason, false))
  }

  // --- Setup, called by the multiplexer ---

  /**
   * @internal The dialer's opening packet: a data-bearing STREAM{SYN} on
   * stream IDs 0/0, retransmitted until acknowledged. It carries our
   * MAX_DATA and MAX_STREAMS, so they arrive reliably.
   */
  sendConnectionSyn (): void {
    this.sendDataPacket(0, 0, [
      { type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) },
      ...this.transportParameters()
    ])
    this.sendResetToken()
  }

  /**
   * @internal The acceptor's opening: our MAX_DATA and MAX_STREAMS, and,
   * when the address is unproven, a PATH_CHALLENGE to prove it.
   */
  start (): void {
    if (this.initiator) return
    const frames = this.transportParameters()
    if (!this.validated) {
      this.addressChallenge = this.newChallenge(this.remoteAddr, this.remotePortNum, this.addressChallengeTimeout(), () => this.onAddressChallengeTimeout())
      this.addressChallengeAttempts = 1
      frames.unshift({ type: FrameType.PathChallenge, data: this.addressChallenge.data })
    }
    this.sendControl(0, 0, frames)
    this.sendResetToken()
  }

  private transportParameters (): Frame[] {
    return [
      { type: FrameType.MaxData, maxData: this.connFc.localMaxData },
      { type: FrameType.MaxStreams, maxStreams: MAX_INCOMING_STREAMS }
    ]
  }

  /** Tells the peer our stateless reset token. go-udx ignores the frame; dart-udx mis-numbers it and drops the packet. */
  private sendResetToken (): void {
    if (this.resetToken === undefined) return
    this.sendControl(0, 0, [{ type: FrameType.NewConnectionId, sequence: 0, retirePriorTo: 0, connectionId: this.localCid, resetToken: this.resetToken }])
  }

  // --- Inbound ---

  /** @internal Handles a decoded packet routed here by the multiplexer. */
  handlePacket (pkt: Packet, size: number, address = this.remoteAddr, port = this.remotePortNum): void {
    if (this.closedFlag) return
    this.bytesReceived += size
    this.lastActivity = this.clock.now()
    if (!this.validated) {
      this.ampReceived += size
      this.flushAmplificationQueue()
    }
    if (!this.isEstablished) {
      this.isEstablished = true
      this.establishedResolve?.()
    }
    if (address !== this.remoteAddr || port !== this.remotePortNum) this.onNewPath(address, port)

    let dataBearing = false
    let edge = false
    let ping = false
    for (const frame of pkt.frames) {
      if (this.closedFlag) return
      if (frame.type === FrameType.Stream) {
        if (frame.data.length > 0 || frame.syn || frame.fin) dataBearing = true
        if (frame.syn || frame.fin) edge = true
      } else if (frame.type === FrameType.Ping) {
        ping = true
      }
      this.handleFrame(pkt, frame, address, port)
    }
    if (this.closedFlag) return

    // Data-bearing packets are acknowledged, as in go-udx. So is a PING sent
    // under a real sequence (dart-udx's ping and our probes), at once. go-udx
    // sends PINGs and all other control packets with sequence 0, and those are
    // never acknowledged: ACKing control packets would make ACKs elicit ACKs.
    if (dataBearing || (ping && pkt.sequence !== 0)) {
      this.acks.onDataPacket(pkt.sequence, edge || !dataBearing, { destinationStreamId: pkt.sourceStreamId, sourceStreamId: pkt.destinationStreamId })
    }
    this.maybeProbe()
  }

  private handleFrame (pkt: Packet, frame: Frame, address: string, port: number): void {
    switch (frame.type) {
      case FrameType.Stream:
        this.handleStreamFrame(pkt, frame)
        break
      case FrameType.Ack:
        this.handleAck(frame)
        break
      case FrameType.WindowUpdate:
        this.findStream(pkt)?.onWindowUpdate(frame.limit)
        break
      case FrameType.ResetStream:
        this.findStream(pkt)?.onReset(frame.errorCode)
        break
      case FrameType.StopSending:
        this.findStream(pkt)?.onStopSending(frame.errorCode)
        break
      case FrameType.StreamDataBlocked:
        this.findStream(pkt)?.onStreamDataBlocked()
        break
      case FrameType.MaxData:
        this.connFc.updatePeerMaxData(frame.maxData)
        this.scheduleFlush()
        break
      case FrameType.DataBlocked:
        this.sendControl(0, 0, [{ type: FrameType.MaxData, maxData: this.connFc.localMaxData }])
        break
      case FrameType.MaxStreams:
        if (frame.maxStreams > this.maxStreams) this.maxStreams = frame.maxStreams
        break
      case FrameType.PathChallenge:
        // Answer on the path the challenge came from.
        this.writeTo(this.encode(0, 0, 0, [{ type: FrameType.PathResponse, data: Uint8Array.from(frame.data) }]), address, port)
        break
      case FrameType.PathResponse:
        this.handlePathResponse(frame.data, address, port)
        break
      case FrameType.NewConnectionId:
        if (frame.connectionId.equals(this.remoteCid)) this.onResetToken?.(Uint8Array.from(frame.resetToken))
        break
      case FrameType.ConnectionClose:
        // Close without answering: the peer has already gone (RFC 9000 §10.2.2).
        this.teardown(new ConnectionClosedError(frame.errorCode, frame.reason, true))
        break
      case FrameType.Padding:
      case FrameType.Ping:
      case FrameType.MtuProbe:
      case FrameType.RetireConnectionId:
        break
    }
  }

  private handleAck (frame: AckFrame): void {
    if (this.recovery.onAckFrame(frame) > 0) this.scheduleFlush()
    this.checkFlushed()
    for (const [seq, ping] of this.pings) {
      if (ackCovers(frame, seq)) {
        this.clock.clearTimeout(ping.timer)
        this.pings.delete(seq)
        ping.resolve(true)
      }
    }
    if (this.probe !== undefined && ackCovers(frame, this.probe.sequence)) {
      this.clock.clearTimeout(this.probe.timer)
      this.pmtu?.onProbeAcked(this.probe.size)
      this.probe = undefined
    }
    this.maybeProbe()
  }

  /**
   * Destination stream ID is our local ID if the peer knows it, else 0;
   * source is the peer's. Look up by ours first, then by the peer's.
   */
  private findStream (pkt: Packet): UdxStream | undefined {
    return this.streams.get(pkt.destinationStreamId) ??
      (pkt.sourceStreamId !== 0 ? this.streamsByRemoteId.get(pkt.sourceStreamId) : undefined)
  }

  private handleStreamFrame (pkt: Packet, f: StreamFrame): void {
    let s = this.findStream(pkt)
    const remoteId = pkt.sourceStreamId
    // Data opens a stream as well as SYN: reordering can deliver data first.
    // A bare FIN does not; it has nothing to deliver.
    if (s === undefined && remoteId !== 0 && (f.syn || f.data.length > 0)) {
      if (this.activeIncoming >= MAX_INCOMING_STREAMS) {
        this.sendControl(remoteId, 0, [{ type: FrameType.ResetStream, errorCode: ErrorCode.StreamLimitError }])
        return
      }
      s = new UdxStream(this.streamHost, this.allocateStreamId(), remoteId, false, this.streamOptions)
      this.addStream(s)
      this.activeIncoming++
      this.deliverIncoming(s)
    }
    if (s === undefined) return
    if (f.data.length > 0) s.onData(f.offset, f.data)
    // The stream ends one past this frame's last byte.
    if (f.fin) s.onFin(f.offset + f.data.length)
  }

  private deliverIncoming (s: UdxStream): void {
    const waiter = this.acceptWaiters.shift()
    if (waiter !== undefined) {
      waiter.resolve(s)
    } else if (this.listenerCount('stream') === 0) {
      this.backlog.push(s)
    }
    this.emit('stream', s)
  }

  private allocateStreamId (): number {
    const id = this.nextStreamId
    this.nextStreamId = (this.nextStreamId + 2) >>> 0
    return id
  }

  private addStream (s: UdxStream): void {
    this.streams.set(s.id, s)
    if (s.remoteId !== 0) this.streamsByRemoteId.set(s.remoteId, s)
    this.activeStreams++
  }

  /**
   * A closed stream stays in the routing maps as a tombstone, so a late
   * retransmission for it is absorbed rather than opening a new stream.
   */
  private onStreamClosed (s: UdxStream): void {
    this.activeStreams--
    if (!s.initiator) this.activeIncoming--
    const i = this.sendOrder.indexOf(s)
    if (i >= 0) this.sendOrder.splice(i, 1)
  }

  // --- Path validation, anti-amplification and migration ---

  private newChallenge (address: string, port: number, timeoutMs: number, onTimeout: () => void): PathChallenge {
    return { data: randomFillSync(new Uint8Array(8)), address, port, timer: this.clock.setTimeout(onTimeout, timeoutMs) }
  }

  /** The address challenge is retried every RTO, a few times. */
  private addressChallengeTimeout (): number {
    return Math.min(PATH_VALIDATION_TIMEOUT, this.recovery.sentPackets.retransmitTimeout())
  }

  /**
   * Resends the same challenge a few times. After that the connection stays
   * unproven, with traffic flowing at 3× what arrives, and a late response
   * still validates it.
   */
  private onAddressChallengeTimeout (): void {
    const c = this.addressChallenge
    if (c === undefined || this.validated || this.closedFlag) return
    if (this.addressChallengeAttempts >= ADDRESS_CHALLENGE_ATTEMPTS) return
    this.addressChallengeAttempts++
    c.timer = this.clock.setTimeout(() => this.onAddressChallengeTimeout(), this.addressChallengeTimeout())
    this.sendControl(0, 0, [{ type: FrameType.PathChallenge, data: c.data }])
  }

  /**
   * Packets arrived from an address other than the peer's: challenge the new
   * address, and move there only once it answers (NAT rebinding, RFC 9000 §9).
   */
  private onNewPath (address: string, port: number): void {
    if (!this.migrationEnabled || this.pathChallenge !== undefined) return
    const challenge: PathChallenge = this.newChallenge(address, port, PATH_VALIDATION_TIMEOUT, () => {
      if (this.pathChallenge === challenge) this.pathChallenge = undefined
    })
    this.pathChallenge = challenge
    this.writeTo(this.encode(0, 0, 0, [{ type: FrameType.PathChallenge, data: challenge.data }]), address, port)
  }

  private handlePathResponse (data: Uint8Array, address: string, port: number): void {
    const ac = this.addressChallenge
    if (ac !== undefined && equalBytes(ac.data, data)) {
      this.clock.clearTimeout(ac.timer)
      this.addressChallenge = undefined
      this.markValidated()
    }
    const pc = this.pathChallenge
    if (pc !== undefined && equalBytes(pc.data, data) && pc.address === address && pc.port === port) {
      this.clock.clearTimeout(pc.timer)
      this.pathChallenge = undefined
      this.remoteAddr = address
      this.remotePortNum = port
      this.markValidated()
      // A new path: its MTU is unknown, so search again from the base size.
      this.restartPmtud()
      this.emit('migrate', address, port)
    }
  }

  private markValidated (): void {
    if (this.validated) return
    this.validated = true
    for (const d of this.ampQueue.splice(0)) this.writeTo(d, this.remoteAddr, this.remotePortNum)
    this.maybeProbe()
  }

  private flushAmplificationQueue (): void {
    while (this.ampQueue.length > 0) {
      const d = this.ampQueue[0] as Uint8Array
      if (this.ampSent + d.length > this.ampReceived * AMPLIFICATION_FACTOR) return
      this.ampQueue.shift()
      this.ampSent += d.length
      this.writeTo(d, this.remoteAddr, this.remotePortNum)
    }
  }

  // --- Path MTU discovery ---

  private headerLength (): number {
    return packetHeaderLength(this.remoteCid.length, this.localCid.length)
  }

  /** go-udx's datagram size: header + STREAM frame header + 1372 bytes. */
  private baseDatagramSize (): number {
    return this.headerLength() + STREAM_FRAME_HEADER_LENGTH + STREAM_CHUNK_SIZE
  }

  private chunkSize (): number {
    return this.datagramSize - this.headerLength() - STREAM_FRAME_HEADER_LENGTH
  }

  private restartPmtud (): void {
    if (this.probe !== undefined) this.clock.clearTimeout(this.probe.timer)
    this.probe = undefined
    this.pmtu = undefined
    this.maybeProbe()
  }

  /**
   * Sends the next probe once the peer has been heard from and its address
   * proven (a probe is large; unproven, it would only queue). A probe is a PING
   * (so peers that acknowledge PINGs answer) padded with MTU_PROBE to the size
   * under test. It is not tracked for retransmission: a lost probe is the
   * answer "too big", not congestion.
   */
  private maybeProbe (): void {
    if (!this.pmtudEnabled || this.closedFlag || this.probe !== undefined) return
    if (!this.isEstablished || !this.validated) return
    if (this.pmtu === undefined) {
      const base = this.baseDatagramSize()
      if (this.maxDatagram <= base) return
      this.pmtu = new PmtuSearch(base, this.maxDatagram)
    }
    const size = this.pmtu.nextProbe()
    if (size === undefined) return
    const sequence = this.ackElicitingSequence()
    const header = this.headerLength()
    const datagram = this.encode(sequence, 0, 0, [{ type: FrameType.Ping }, { type: FrameType.MtuProbe, size: size - header - 1 }])
    const timer = this.clock.setTimeout(() => {
      if (this.probe?.sequence !== sequence) return
      this.probe = undefined
      this.pmtu?.onProbeLost(size)
      this.maybeProbe()
    }, 2 * this.recovery.sentPackets.retransmitTimeout())
    this.probe = { sequence, size, timer }
    this.write(datagram)
  }

  // --- Outbound ---

  private scheduleFlush (): void {
    if (this.flushScheduled || this.closedFlag) return
    this.flushScheduled = true
    queueMicrotask(() => {
      this.flushScheduled = false
      this.flush()
    })
  }

  /**
   * Sends stream data while the congestion window, the pacer and the peer's
   * MAX_DATA allow, taking one frame from each stream with something to send
   * in turn.
   */
  private flush (): void {
    if (this.closedFlag) return
    this.refreshSendOrder()
    const cc = this.recovery.congestion
    const chunk = this.chunkSize()
    const full = this.datagramSize
    let idle = 0
    while (this.sendOrder.length > 0 && idle < this.sendOrder.length) {
      const wait = cc.pacer.timeUntilSend()
      if (wait > PACING_GRANULARITY) {
        this.armPacer(wait)
        return
      }
      if (!cc.canSend(full)) return // an ACK will reopen it
      if (!this.connFc.canSend(cc.inflight, full)) {
        this.sendDataBlocked()
        return
      }

      const s = this.sendOrder.shift() as UdxStream
      const frame = s.nextFrame(chunk)
      if (s.hasSendable()) this.sendOrder.push(s)
      if (frame === undefined) {
        idle++
        continue
      }
      idle = 0
      this.sendDataPacket(s.remoteId, s.id, [frame])
      if (this.closedFlag) return
    }
  }

  /** At most once per RTO: the peer answers with MAX_DATA. */
  private sendDataBlocked (): void {
    const now = this.clock.now()
    if (now - this.dataBlockedSentAt < this.recovery.sentPackets.retransmitTimeout()) return
    this.dataBlockedSentAt = now
    this.sendControl(0, 0, [{ type: FrameType.DataBlocked, limit: this.connFc.peerMaxData }])
  }

  private refreshSendOrder (): void {
    for (const s of this.streams.values()) {
      if (s.hasSendable() && !this.sendOrder.includes(s)) this.sendOrder.push(s)
    }
  }

  private armPacer (wait: number): void {
    if (this.pacerTimer !== undefined) return
    this.pacerTimer = this.clock.setTimeout(() => {
      this.pacerTimer = undefined
      this.flush()
    }, wait)
  }

  /** Sends a data-bearing packet: sequenced, tracked for ACK and retransmission, charged to the window. */
  private sendDataPacket (dst: number, src: number, frames: Frame[]): void {
    const sequence = this.recovery.nextSequence()
    const datagram = this.encode(sequence, dst, src, frames)
    this.recovery.onDataPacketSent({ sequence, size: datagram.length, frames, destinationStreamId: dst, sourceStreamId: src })
    this.recovery.congestion.pacer.onPacketSent(datagram.length)
    this.write(datagram)
  }

  /**
   * A sequence for an untracked packet the peer should acknowledge (a PING or
   * a probe). Never 0: go-udx sends its control packets as sequence 0, so a
   * PING under 0 is one nobody acknowledges.
   */
  private ackElicitingSequence (): number {
    const seq = this.recovery.nextSequence()
    return seq !== 0 ? seq : this.recovery.nextSequence()
  }

  /** Sends a control packet: sequence 0, untracked, never retransmitted. */
  private sendControl (dst: number, src: number, frames: Frame[]): void {
    if (this.closedFlag) return
    this.write(this.encode(0, dst, src, frames))
  }

  private retransmit (pkt: SentPacket, sequence: number): void {
    if (this.closedFlag) return
    this.write(this.encode(sequence, pkt.destinationStreamId, pkt.sourceStreamId, pkt.frames))
  }

  private encode (sequence: number, dst: number, src: number, frames: Frame[]): Uint8Array {
    return encodePacket({
      version: VERSION_CURRENT,
      destinationCid: this.remoteCid,
      sourceCid: this.localCid,
      sequence,
      destinationStreamId: dst,
      sourceStreamId: src,
      frames
    })
  }

  /** Sends to the peer's address, subject to the amplification limit. */
  private write (datagram: Uint8Array): void {
    if (!this.validated) {
      if (this.ampQueue.length > 0 || this.ampSent + datagram.length > this.ampReceived * AMPLIFICATION_FACTOR) {
        if (this.ampQueue.length < MAX_AMPLIFICATION_QUEUE) this.ampQueue.push(datagram)
        return
      }
      this.ampSent += datagram.length
    }
    this.writeTo(datagram, this.remoteAddr, this.remotePortNum)
  }

  private writeTo (datagram: Uint8Array, address: string, port: number): void {
    this.bytesSent += datagram.length
    this.sendDatagram(datagram, address, port)
  }

  // --- Version negotiation, stateless reset, idle timeout, teardown ---

  /** @internal A version negotiation packet addressed to us. */
  onVersionNegotiation (versions: number[]): void {
    // Only before we've heard from the peer: afterwards it can only be spoofed.
    if (this.isEstablished || versions.includes(VERSION_CURRENT)) return
    this.teardown(new ConnectionClosedError(ErrorCode.ProtocolViolation, `no common version; peer supports ${versions.join(', ')}`, true))
  }

  /** @internal The peer sent a stateless reset: it has lost this connection's state. */
  onStatelessReset (): void {
    this.teardown(new ConnectionClosedError(ErrorCode.InternalError, 'stateless reset', true))
  }

  /** max(MAX_IDLE_TIMEOUT, 3 × RTO): loss recovery always gets three RTOs first (RFC 9000 §10.1). */
  idleTimeout (): number {
    return Math.max(MAX_IDLE_TIMEOUT, 3 * this.recovery.sentPackets.retransmitTimeout())
  }

  private armIdleCheck (): void {
    this.idleTimer = this.clock.setTimeout(() => {
      this.idleTimer = undefined
      if (this.closedFlag) return
      if (this.clock.now() - this.lastActivity >= this.idleTimeout()) {
        // Silent: there may be nobody left on the path to hear a CONNECTION_CLOSE.
        this.teardown(new ConnectionClosedError(ErrorCode.ConnectionTimeout, 'idle timeout', false))
        return
      }
      if (this.keepAliveInterval > 0 && !this.keepAlivePending && this.clock.now() - this.lastActivity >= this.keepAliveInterval) {
        // Its ACK is activity here; its arrival is activity there.
        this.keepAlivePending = true
        void this.ping(this.keepAliveInterval).then(() => { this.keepAlivePending = false })
      }
      this.armIdleCheck()
    }, IDLE_CHECK_INTERVAL)
  }

  private teardown (err: ConnectionClosedError): void {
    if (this.closedFlag) return
    this.closedFlag = true
    const reportable = err.code === ErrorCode.NoError && !err.remote ? undefined : err
    this.closedError = err
    for (const t of [this.idleTimer, this.pacerTimer, this.probe?.timer, this.addressChallenge?.timer, this.pathChallenge?.timer]) {
      if (t !== undefined) this.clock.clearTimeout(t)
    }
    this.idleTimer = undefined
    this.pacerTimer = undefined
    this.probe = undefined
    this.addressChallenge = undefined
    this.pathChallenge = undefined
    this.ampQueue.length = 0
    for (const [, ping] of this.pings) {
      this.clock.clearTimeout(ping.timer)
      ping.resolve(false)
    }
    this.pings.clear()
    for (const w of this.flushWaiters.splice(0)) w(false)
    this.acks.destroy()
    this.recovery.destroy()
    for (const s of this.streams.values()) s.abort(err)
    for (const w of this.acceptWaiters.splice(0)) w.reject(err)
    this.onClosed?.()
    this.emit('close', reportable)
  }
}

function equalBytes (a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  let diff = 0
  for (let i = 0; i < a.length; i++) diff |= (a[i] as number) ^ (b[i] as number)
  return diff === 0
}
