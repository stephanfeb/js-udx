import { AckTracker } from './ack-tracker.js'
import type { Clock, TimerHandle } from './clock.js'
import { ACK_ELICITING_THRESHOLD, MAX_ACK_DELAY, MIN_ACK_DELAY } from './constants.js'
import type { AckFrame } from './frames.js'

/** Where an ACK goes: the stream IDs to put in the packet header. */
export interface AckRoute {
  destinationStreamId: number
  sourceStreamId: number
}

export interface AckSchedulerOptions {
  clock: Clock
  /** The current smoothed RTT, which sets the delayed-ACK timer. */
  smoothedRtt: () => number
  /** Sends an ACK-only packet. */
  sendAck: (frame: AckFrame, route: AckRoute) => void
}

/**
 * The receiver's acknowledgement policy (RFC 9000 §13.2), as go-udx's
 * Connection.noteReceived applies it. Only data-bearing packets are recorded
 * here; ACKing control packets would make ACKs elicit ACKs without end.
 *
 * Acknowledge at once when a packet arrives out of order, carries SYN or FIN,
 * or is the ACK_ELICITING_THRESHOLD-th pending one. Otherwise arm a timer of
 * SRTT/4 clamped to [MIN_ACK_DELAY, MAX_ACK_DELAY]. The frame reports how long
 * the largest packet waited, in whole milliseconds.
 */
export class AckScheduler {
  readonly tracker = new AckTracker()
  private readonly opts: AckSchedulerOptions
  private pendingCount = 0
  private timer: TimerHandle | undefined
  private largestArrival: number | undefined

  constructor (opts: AckSchedulerOptions) {
    this.opts = opts
  }

  /** Data-bearing packets received but not yet acknowledged. */
  get pending (): number {
    return this.pendingCount
  }

  get timerArmed (): boolean {
    return this.timer !== undefined
  }

  /**
   * Records a received data-bearing packet. `route` is the received packet's
   * header with the stream IDs swapped, i.e. where the ACK should be addressed.
   */
  onDataPacket (sequence: number, edge: boolean, route: AckRoute): void {
    const now = this.opts.clock.now()
    const hadAny = this.tracker.hasAny
    const before = this.tracker.largest
    const outOfOrder = this.tracker.add(sequence)
    if (!hadAny || this.tracker.largest !== before) this.largestArrival = now
    this.pendingCount++

    if (outOfOrder || edge || this.pendingCount >= ACK_ELICITING_THRESHOLD) {
      this.opts.sendAck(this.take(now), route)
      return
    }
    if (this.timer === undefined) {
      const delay = Math.min(MAX_ACK_DELAY, Math.max(MIN_ACK_DELAY, this.opts.smoothedRtt() / 4))
      this.timer = this.opts.clock.setTimeout(() => this.onTimer(route), delay)
    }
  }

  /** Flushes whatever is pending, as the delayed-ACK timer does. */
  private onTimer (route: AckRoute): void {
    this.timer = undefined
    if (this.pendingCount === 0) return
    this.opts.sendAck(this.take(this.opts.clock.now()), route)
  }

  private take (now: number): AckFrame {
    this.pendingCount = 0
    if (this.timer !== undefined) {
      this.opts.clock.clearTimeout(this.timer)
      this.timer = undefined
    }
    let delay = 0
    if (this.largestArrival !== undefined) {
      delay = Math.min(65535, Math.max(0, Math.floor(now - this.largestArrival)))
    }
    return this.tracker.frame(delay)
  }

  destroy (): void {
    if (this.timer !== undefined) this.opts.clock.clearTimeout(this.timer)
    this.timer = undefined
    this.pendingCount = 0
  }
}
