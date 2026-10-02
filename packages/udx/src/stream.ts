import { EventEmitter } from 'node:events'
import type { Clock, TimerHandle } from './clock.js'
import { ErrorCode, INITIAL_MAX_STREAM_DATA, MAX_STREAM_RECV_OOO, STREAM_BLOCKED_RETRY_INTERVAL } from './constants.js'
import { StreamResetError, WriteAfterEndError } from './errors.js'
import { StreamFlowController } from './flow-control.js'
import { type Frame, FrameType, type StreamFrame } from './frames.js'

/** What a stream needs from its connection. */
export interface StreamHost {
  readonly clock: Clock
  /** Sends a control packet (sequence 0, never retransmitted). */
  sendControl: (destinationStreamId: number, sourceStreamId: number, frames: Frame[]) => void
  /** The stream has something to send; the connection will call `nextFrame`. */
  scheduleFlush: () => void
  /** The stream has finished in both directions or was reset. */
  streamClosed: (stream: UdxStream) => void
}

export interface UdxStreamEvents {
  /** Data from the peer, in order. Emitted only while flowing (see `pause`/`resume`). */
  data: [chunk: Uint8Array]
  /** The peer finished sending and every byte has been emitted. */
  end: []
  /** The write queue fell below the high-water mark after `write` returned false. */
  drain: []
  /** The stream is finished: both directions done, or reset (`error` set). Emitted once. */
  close: [error?: Error]
}

export interface UdxStreamOptions {
  /** `write` returns false once this many bytes are queued unsent. */
  writeHighWaterMark?: number
}

const EMPTY = new Uint8Array(0)
const DEFAULT_WRITE_HIGH_WATER_MARK = 256 * 1024

/**
 * A reliable, ordered, bidirectional byte stream on a UDX connection.
 *
 * Writing: `write` queues bytes and returns false once the queue reaches the
 * high-water mark; wait for 'drain'. `end` half-closes after the queue
 * flushes. `reset` aborts both directions.
 *
 * Reading: the stream starts paused and buffers what arrives. It begins
 * flowing when a 'data' listener is added, `resume` is called, or it is
 * iterated with `for await`. Bytes count as consumed when emitted, and only
 * consumption reopens the peer's flow-control window, so a paused stream
 * pushes back on the sender.
 *
 * Stream IDs are local to each side. `remoteId` is the peer's ID once known;
 * a stream we opened never learns it (go-udx behaviour), and the peer finds it
 * by our ID instead.
 */
export class UdxStream extends EventEmitter<UdxStreamEvents> {
  readonly id: number
  remoteId: number
  readonly initiator: boolean

  private readonly host: StreamHost
  private readonly fc: StreamFlowController
  private readonly writeHighWaterMark: number

  // Send side
  private sendQueue: Uint8Array[] = []
  private sendQueueHead = 0
  private queuedBytes = 0
  private bytesSent = 0
  private synSent: boolean
  private endRequested = false
  private finSent = false
  private needDrain = false
  private blocked = false
  private blockedTimer: TimerHandle | undefined

  // Receive side, reassembled on byte offsets.
  private readonly ooo = new Map<number, Uint8Array>()
  private oooBytes = 0
  private recvOffset = 0
  private readQueue: Uint8Array[] = []
  private readQueued = 0
  private paused = true
  private finalSize: number | undefined
  private endEmitted = false

  private closedFlag = false
  private closeError: Error | undefined
  private bytesReadTotal = 0

  /** @internal Created by UdxConnection. */
  constructor (host: StreamHost, id: number, remoteId: number, initiator: boolean, opts: UdxStreamOptions = {}) {
    super()
    this.host = host
    this.id = id
    this.remoteId = remoteId
    this.initiator = initiator
    // An incoming stream was opened by the peer's SYN (or data); we never send one.
    this.synSent = !initiator
    this.fc = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    this.writeHighWaterMark = opts.writeHighWaterMark ?? DEFAULT_WRITE_HIGH_WATER_MARK
    // Like a Node Readable, adding a 'data' listener starts the flow.
    ;(this as EventEmitter).on('newListener', (event: string | symbol) => {
      if (event === 'data') queueMicrotask(() => this.resume())
    })
  }

  // --- Public API ---

  /** Bytes received but not yet emitted, including out-of-order bytes. */
  get bufferedBytes (): number {
    return this.fc.bufferedBytes
  }

  /** The current (auto-tuned) receive window. */
  get recvWindow (): number {
    return this.fc.recvWindow
  }

  /** Bytes queued by `write` and not yet sent. */
  get writableLength (): number {
    return this.queuedBytes
  }

  get bytesWritten (): number {
    return this.bytesSent
  }

  get bytesRead (): number {
    return this.bytesReadTotal
  }

  get destroyed (): boolean {
    return this.closedFlag
  }

  /** Set when the stream closed because of a reset or connection failure. */
  get error (): Error | undefined {
    return this.closeError
  }

  /** True once `end` was called or the stream closed. */
  get writableEnded (): boolean {
    return this.endRequested || this.closedFlag
  }

  /** True once the peer's FIN was reached and 'end' emitted. */
  get readableEnded (): boolean {
    return this.endEmitted
  }

  get isPaused (): boolean {
    return this.paused
  }

  /**
   * Queues bytes for sending. Returns false when the queue is at or above the
   * high-water mark; wait for 'drain' before writing more. The bytes are
   * copied, so the caller may reuse its buffer.
   */
  write (data: Uint8Array): boolean {
    if (this.closeError !== undefined) throw this.closeError
    if (this.endRequested || this.closedFlag) throw new WriteAfterEndError()
    if (data.length > 0) {
      this.sendQueue.push(Uint8Array.from(data))
      this.queuedBytes += data.length
      if (this.fc.sendWindowAvailable() === 0) this.setBlocked()
      this.host.scheduleFlush()
    }
    const ok = this.queuedBytes < this.writeHighWaterMark
    if (!ok) this.needDrain = true
    return ok
  }

  /** Half-closes: sends FIN once everything queued has been sent. */
  end (): void {
    if (this.endRequested || this.closedFlag) return
    this.endRequested = true
    this.host.scheduleFlush()
  }

  /** Aborts both directions and tells the peer with RESET_STREAM. */
  reset (code: number = ErrorCode.NoError): void {
    if (this.closedFlag) return
    this.host.sendControl(this.remoteId, this.id, [{ type: FrameType.ResetStream, errorCode: code }])
    this.finish(new StreamResetError(code, false))
  }

  /** Stops emitting 'data'. Arriving bytes are buffered and the peer's window stops reopening. */
  pause (): void {
    this.paused = true
  }

  /** Starts (or restarts) emitting buffered and arriving data. */
  resume (): void {
    if (!this.paused) return
    this.paused = false
    this.drainRead()
  }

  /** Iterates received chunks until the peer ends the stream; throws if it is reset. */
  async * [Symbol.asyncIterator] (): AsyncGenerator<Uint8Array, void, undefined> {
    const chunks: Uint8Array[] = []
    let done = false
    let failure: Error | undefined
    let wake: (() => void) | undefined
    const notify = (): void => {
      const w = wake
      wake = undefined
      w?.()
    }
    const onData = (chunk: Uint8Array): void => { chunks.push(chunk); notify() }
    const onEnd = (): void => { done = true; notify() }
    const onClose = (err?: Error): void => { failure = err; done = true; notify() }
    this.on('data', onData)
    this.on('end', onEnd)
    this.on('close', onClose)
    if (this.endEmitted) done = true
    if (this.closedFlag) onClose(this.closeError)
    try {
      for (;;) {
        const chunk = chunks.shift()
        if (chunk !== undefined) {
          yield chunk
          continue
        }
        if (failure !== undefined) throw failure
        if (done) return
        await new Promise<void>(resolve => { wake = resolve })
      }
    } finally {
      this.off('data', onData)
      this.off('end', onEnd)
      this.off('close', onClose)
    }
  }

  // --- Send path, driven by the connection ---

  /** @internal Whether `nextFrame` would return a frame. */
  hasSendable (): boolean {
    if (this.closedFlag) return false
    if (this.queuedBytes > 0) return this.fc.sendWindowAvailable() > 0
    return this.endRequested && !this.finSent
  }

  /**
   * @internal The next STREAM frame to send, of at most `maxData` bytes, or
   * undefined when there is nothing to send or the peer's window is closed.
   * The FIN goes in a frame of its own after the data, as go-udx sends it.
   */
  nextFrame (maxData: number): StreamFrame | undefined {
    if (this.closedFlag) return undefined
    if (this.queuedBytes > 0) {
      const avail = this.fc.sendWindowAvailable()
      if (avail === 0) {
        this.setBlocked()
        return undefined
      }
      const data = this.take(Math.min(maxData, this.queuedBytes, avail))
      const frame: StreamFrame = { type: FrameType.Stream, fin: false, syn: !this.synSent, offset: this.bytesSent, data }
      this.synSent = true
      this.bytesSent += data.length
      this.fc.onDataSent(data.length)
      // Out of credit with more to send: the flush loop won't ask again until
      // credit arrives, so tell the peer now.
      if (this.queuedBytes > 0 && this.fc.sendWindowAvailable() === 0) this.setBlocked()
      if (this.needDrain && this.queuedBytes < this.writeHighWaterMark) {
        this.needDrain = false
        queueMicrotask(() => { if (!this.closedFlag) this.emit('drain') })
      }
      return frame
    }
    if (this.endRequested && !this.finSent) {
      this.finSent = true
      // An empty stream's FIN also carries SYN, so the peer has a stream to end.
      const frame: StreamFrame = { type: FrameType.Stream, fin: true, syn: !this.synSent, offset: this.bytesSent, data: EMPTY }
      this.synSent = true
      queueMicrotask(() => this.maybeClose())
      return frame
    }
    return undefined
  }

  private take (n: number): Uint8Array {
    const first = this.sendQueue[this.sendQueueHead] as Uint8Array
    let out: Uint8Array
    if (first.length >= n) {
      out = first.subarray(0, n)
      if (first.length === n) {
        this.sendQueue[this.sendQueueHead++] = EMPTY
      } else {
        this.sendQueue[this.sendQueueHead] = first.subarray(n)
      }
    } else {
      out = new Uint8Array(n)
      let filled = 0
      while (filled < n) {
        const chunk = this.sendQueue[this.sendQueueHead] as Uint8Array
        const k = Math.min(chunk.length, n - filled)
        out.set(chunk.subarray(0, k), filled)
        filled += k
        if (k === chunk.length) {
          this.sendQueue[this.sendQueueHead++] = EMPTY
        } else {
          this.sendQueue[this.sendQueueHead] = chunk.subarray(k)
        }
      }
    }
    this.queuedBytes -= n
    if (this.sendQueueHead > 64 && this.sendQueueHead * 2 > this.sendQueue.length) {
      this.sendQueue = this.sendQueue.slice(this.sendQueueHead)
      this.sendQueueHead = 0
    }
    return out
  }

  /**
   * Out of send credit: tell the peer with STREAM_DATA_BLOCKED, and keep
   * telling it every 500 ms. WINDOW_UPDATE is never retransmitted, so this is
   * what recovers a stream whose update was lost.
   */
  private setBlocked (): void {
    if (this.blocked) return
    this.blocked = true
    const tell = (): void => {
      if (!this.blocked || this.closedFlag) return
      this.host.sendControl(this.remoteId, this.id, [{ type: FrameType.StreamDataBlocked, streamId: this.id, limit: this.fc.sendLimit }])
      this.blockedTimer = this.host.clock.setTimeout(tell, STREAM_BLOCKED_RETRY_INTERVAL)
    }
    tell()
  }

  private clearBlocked (): void {
    this.blocked = false
    if (this.blockedTimer !== undefined) this.host.clock.clearTimeout(this.blockedTimer)
    this.blockedTimer = undefined
  }

  // --- Frames from the peer, routed by the connection ---

  /** @internal */
  onWindowUpdate (wire: number): void {
    if (this.fc.applyWindowUpdate(wire)) {
      this.clearBlocked()
      this.host.scheduleFlush()
    }
  }

  /** @internal The peer is blocked: re-advertise the current limit without growing the window. */
  onStreamDataBlocked (): void {
    if (this.closedFlag) return
    this.sendWindowUpdate(this.fc.refreshLimit())
  }

  /** @internal */
  onReset (code: number): void {
    if (this.closedFlag) return
    this.finish(new StreamResetError(code, true))
  }

  /** @internal STOP_SENDING: go-udx answers by resetting the stream. */
  onStopSending (code: number): void {
    this.reset(code)
  }

  /** @internal Places STREAM frame data at its offset; bytes already delivered are dropped. */
  onData (offset: number, data: Uint8Array): void {
    if (this.closedFlag || data.length === 0) return
    const end = offset + data.length
    if (end <= this.recvOffset) return
    if (offset < this.recvOffset) {
      data = data.subarray(this.recvOffset - offset)
      offset = this.recvOffset
    }

    if (offset > this.recvOffset) {
      if (this.ooo.has(offset)) return
      // Flow control bounds this; the cap only catches a peer ignoring its
      // limit. Discarding would be unsafe (the packet is already acknowledged),
      // so fail the stream loudly instead.
      if (this.oooBytes + data.length > MAX_STREAM_RECV_OOO) {
        this.reset(ErrorCode.FlowControlError)
        return
      }
      this.ooo.set(offset, data)
      this.oooBytes += data.length
      this.fc.onDataReceived(data.length)
      return
    }

    this.fc.onDataReceived(data.length)
    this.enqueueRead(data)
    for (;;) {
      const chunk = this.ooo.get(this.recvOffset)
      if (chunk === undefined) break
      this.ooo.delete(this.recvOffset)
      this.oooBytes -= chunk.length
      this.enqueueRead(chunk)
    }
    this.drainRead()
  }

  /** @internal The peer's FIN: the stream ends at `finalSize`, which may be ahead of what has arrived. */
  onFin (finalSize: number): void {
    if (this.closedFlag) return
    this.finalSize = finalSize
    this.drainRead()
  }

  private enqueueRead (chunk: Uint8Array): void {
    this.readQueue.push(chunk)
    this.readQueued += chunk.length
    this.recvOffset += chunk.length
  }

  private drainRead (): void {
    while (!this.paused && !this.closedFlag && this.readQueue.length > 0) {
      const chunk = this.readQueue.shift() as Uint8Array
      this.readQueued -= chunk.length
      this.bytesReadTotal += chunk.length
      if (this.fc.onDataConsumed(chunk.length)) this.sendWindowUpdate(this.fc.advertiseLimit())
      this.emit('data', chunk)
    }
    if (this.paused || this.closedFlag || this.endEmitted || this.readQueue.length > 0) return
    if (this.finalSize !== undefined && this.recvOffset >= this.finalSize) {
      this.endEmitted = true
      this.emit('end')
      this.maybeClose()
    }
  }

  private sendWindowUpdate (limit: number): void {
    this.host.sendControl(this.remoteId, this.id, [{ type: FrameType.WindowUpdate, limit: limit % 0x1_0000_0000 }])
  }

  // --- Lifecycle ---

  private maybeClose (): void {
    if (!this.closedFlag && this.finSent && this.endEmitted) this.finish(undefined)
  }

  /** @internal Closes the stream because its connection closed. */
  abort (err: Error): void {
    if (!this.closedFlag) this.finish(err)
  }

  private finish (err: Error | undefined): void {
    this.closedFlag = true
    this.closeError = err
    this.clearBlocked()
    this.sendQueue = []
    this.sendQueueHead = 0
    this.queuedBytes = 0
    this.readQueue = []
    this.readQueued = 0
    this.ooo.clear()
    this.oooBytes = 0
    this.host.streamClosed(this)
    this.emit('close', err)
  }
}
