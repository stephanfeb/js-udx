import { EventEmitter } from 'node:events'
import { AckScheduler } from './ack-scheduler.js'
import type { ConnectionId } from './cid.js'
import type { Clock, TimerHandle } from './clock.js'
import { ErrorCode, INITIAL_MAX_DATA, INITIAL_MAX_STREAMS, MAX_DATAGRAM_SIZE, MAX_IDLE_TIMEOUT, PACING_GRANULARITY, VERSION_CURRENT } from './constants.js'
import { ConnectionClosedError, UdxError } from './errors.js'
import { ConnectionFlowController } from './flow-control.js'
import { type Frame, FrameType, STREAM_FRAME_HEADER_LENGTH, type StreamFrame } from './frames.js'
import { type Packet, encodePacket, packetHeaderLength } from './packet.js'
import { LossRecovery } from './recovery.js'
import type { SentPacket } from './sent-packets.js'
import { type StreamHost, UdxStream, type UdxStreamOptions } from './stream.js'

/** Payload per STREAM frame: MAX_DATAGRAM_SIZE − 100, as go-udx sends. */
export const STREAM_CHUNK_SIZE = MAX_DATAGRAM_SIZE - 100

/** How often the idle watchdog checks for silence. */
const IDLE_CHECK_INTERVAL = 1000

/** Most incoming streams a connection holds open at once; beyond it they are reset. */
const MAX_INCOMING_STREAMS = 1024

export interface UdxConnectionEvents {
  /** The peer opened a stream. */
  stream: [stream: UdxStream]
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
  /** Puts a datagram on the wire to the connection's remote address. */
  send: (datagram: Uint8Array, address: string, port: number) => void
  /** Called once when the connection closes, so the multiplexer can forget it. */
  onClosed?: () => void
  streamOptions?: UdxStreamOptions
}

/**
 * A UDX connection to one peer: streams multiplexed over one path, with
 * reliability, congestion control and flow control. Port of go-udx's
 * Connection, on Node's single thread: inbound packets are handled
 * synchronously, and sends are batched by a flush scheduled as a microtask.
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
  private readonly streamOptions: UdxStreamOptions | undefined

  private readonly recovery: LossRecovery
  private readonly acks: AckScheduler
  private readonly connFc = new ConnectionFlowController(INITIAL_MAX_DATA)

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
  private lastActivity: number
  private establishedResolve: (() => void) | undefined
  private isEstablished = false

  private closedError: ConnectionClosedError | undefined
  private closedFlag = false

  /** Datagrams and bytes, for anti-amplification and diagnostics. */
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
    this.streamOptions = opts.streamOptions
    this.nextStreamId = opts.initiator ? 1 : 2
    this.lastActivity = this.clock.now()
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

  /**
   * Opens a stream. Nothing goes on the wire until the first write (or `end`),
   * whose first frame carries SYN.
   */
  openStream (): UdxStream {
    if (this.closedFlag) throw this.closedError ?? new ConnectionClosedError(ErrorCode.NoError, '', false)
    if (this.activeStreams >= this.maxStreams) throw new UdxError(ErrorCode.StreamLimitError, 'stream limit exceeded')
    const id = this.allocateStreamId()
    const s = new UdxStream(this.streamHost, id, 0, true, this.streamOptions)
    this.addStream(s)
    return s
  }

  /** Resolves with the next stream the peer opens. Streams are also emitted as 'stream'. */
  async acceptStream (): Promise<UdxStream> {
    const queued = this.backlog.shift()
    if (queued !== undefined) return queued
    if (this.closedFlag) throw this.closedError ?? new ConnectionClosedError(ErrorCode.NoError, '', false)
    return await new Promise((resolve, reject) => this.acceptWaiters.push({ resolve, reject }))
  }

  /** Sends a PING. Like go-udx's, it is fire-and-forget: PING packets are not acknowledged. */
  ping (): void {
    if (!this.closedFlag) this.sendControl(0, 0, [{ type: FrameType.Ping }])
  }

  /** Closes the connection, telling the peer with CONNECTION_CLOSE. */
  close (code: number = ErrorCode.NoError, reason = ''): void {
    if (this.closedFlag) return
    this.sendControl(0, 0, [{ type: FrameType.ConnectionClose, errorCode: code, frameType: 0, reason }])
    this.teardown(new ConnectionClosedError(code, reason, false))
  }

  // --- Inbound ---

  /** @internal Handles a decoded packet routed here by the multiplexer. */
  handlePacket (pkt: Packet, size: number): void {
    if (this.closedFlag) return
    this.bytesReceived += size
    this.lastActivity = this.clock.now()
    if (!this.isEstablished) {
      this.isEstablished = true
      this.establishedResolve?.()
    }

    let dataBearing = false
    let edge = false
    for (const frame of pkt.frames) {
      if (this.closedFlag) return
      if (frame.type === FrameType.Stream) {
        if (frame.data.length > 0 || frame.syn || frame.fin) dataBearing = true
        if (frame.syn || frame.fin) edge = true
      }
      this.handleFrame(pkt, frame)
    }

    // Only data-bearing packets are acknowledged: ACKing control packets would
    // make every ACK elicit another.
    if (dataBearing && !this.closedFlag) {
      this.acks.onDataPacket(pkt.sequence, edge, { destinationStreamId: pkt.sourceStreamId, sourceStreamId: pkt.destinationStreamId })
    }
  }

  private handleFrame (pkt: Packet, frame: Frame): void {
    switch (frame.type) {
      case FrameType.Stream:
        this.handleStreamFrame(pkt, frame)
        break
      case FrameType.Ack:
        if (this.recovery.onAckFrame(frame) > 0) this.scheduleFlush()
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
        this.connFc.updateMaxData(frame.maxData)
        break
      case FrameType.DataBlocked:
        this.sendControl(0, 0, [{ type: FrameType.MaxData, maxData: this.connFc.maxData }])
        break
      case FrameType.MaxStreams:
        if (frame.maxStreams > this.maxStreams) this.maxStreams = frame.maxStreams
        break
      case FrameType.PathChallenge:
        this.sendControl(0, 0, [{ type: FrameType.PathResponse, data: Uint8Array.from(frame.data) }])
        break
      case FrameType.ConnectionClose:
        // Close without answering: the peer has already gone (RFC 9000 §10.2.2).
        this.teardown(new ConnectionClosedError(frame.errorCode, frame.reason, true))
        break
      case FrameType.Padding:
      case FrameType.Ping:
      case FrameType.MtuProbe:
      case FrameType.PathResponse:
      case FrameType.NewConnectionId:
      case FrameType.RetireConnectionId:
        break
    }
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
   * Sends stream data while the congestion window and pacer allow, taking one
   * frame from each stream with something to send in turn.
   */
  private flush (): void {
    if (this.closedFlag) return
    this.refreshSendOrder()
    const cc = this.recovery.congestion
    const headerLen = packetHeaderLength(this.remoteCid.length, this.localCid.length)
    let idle = 0
    while (this.sendOrder.length > 0 && idle < this.sendOrder.length) {
      const wait = cc.pacer.timeUntilSend()
      if (wait > PACING_GRANULARITY) {
        this.armPacer(wait)
        return
      }
      if (!cc.canSend(headerLen + STREAM_FRAME_HEADER_LENGTH + STREAM_CHUNK_SIZE)) return // an ACK will reopen it

      const s = this.sendOrder.shift() as UdxStream
      const frame = s.nextFrame(STREAM_CHUNK_SIZE)
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

  private write (datagram: Uint8Array): void {
    this.bytesSent += datagram.length
    this.sendDatagram(datagram, this.remoteAddr, this.remotePortNum)
  }

  /** @internal The dialer's connection SYN: a data-bearing STREAM{SYN} on stream IDs 0/0, retransmitted until acknowledged. */
  sendConnectionSyn (): void {
    this.sendDataPacket(0, 0, [{ type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new Uint8Array(0) }])
  }

  // --- Idle timeout and teardown ---

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
      this.armIdleCheck()
    }, IDLE_CHECK_INTERVAL)
  }

  private teardown (err: ConnectionClosedError): void {
    if (this.closedFlag) return
    this.closedFlag = true
    const reportable = err.code === ErrorCode.NoError && !err.remote ? undefined : err
    this.closedError = err
    if (this.idleTimer !== undefined) this.clock.clearTimeout(this.idleTimer)
    if (this.pacerTimer !== undefined) this.clock.clearTimeout(this.pacerTimer)
    this.idleTimer = undefined
    this.pacerTimer = undefined
    this.acks.destroy()
    this.recovery.destroy()
    for (const s of this.streams.values()) s.abort(err)
    for (const w of this.acceptWaiters.splice(0)) w.reject(err)
    this.onClosed?.()
    this.emit('close', reportable)
  }
}
