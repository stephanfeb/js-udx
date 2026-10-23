import type { Clock } from './clock.js'
import {
  MAX_DATAGRAM_SIZE,
  MIN_PACING_DELAY,
  PACING_BURST_PACKETS,
  PACING_GAIN,
  PACING_GAIN_SLOW_START
} from './constants.js'

/**
 * Spreads a connection's sends over the round trip instead of letting a
 * window's worth go out at once (RFC 9002 §7.7), as go-udx v0.3.0 and
 * dart-udx 4.3.0 pace.
 *
 * The rate is gain × cwnd / smoothed RTT, the gain PACING_GAIN_SLOW_START in
 * slow start and PACING_GAIN after it. The congestion controller recomputes
 * it on every change to the window or the RTT estimate (updateRate). It used
 * to be 2.88 × cwnd / min RTT, recomputed only when min RTT fell or the
 * window contracted, so it lagged the window by whole round trips.
 *
 * Sends draw on a budget that refills at the rate and holds at most a burst:
 * PACING_BURST_PACKETS datagrams, or what MIN_PACING_DELAY of the rate is
 * worth, whichever is more. A sender need not sleep between every two
 * packets, and a timer that can only wait whole milliseconds still reaches
 * the rate. The budget may go negative: a batch that overshoots it is paid
 * for by the wait after it. Until the controller has an RTT sample the rate
 * is unlimited and the initial window is the burst.
 */
export class Pacer {
  private readonly clock: Clock
  private rate = Infinity // bytes per second
  private budgetAtLastSend = 0
  private lastSendTime: number | undefined

  constructor (clock: Clock) {
    this.clock = clock
  }

  /** Bytes per second; Infinity when unpaced. */
  get pacingRate (): number {
    return this.rate
  }

  /**
   * Sets the rate from the congestion window and the smoothed RTT (ms). A
   * zero RTT (loopback, or a test clock that does not move) means unpaced.
   */
  updateRate (cwnd: number, srtt: number, slowStart: boolean): void {
    if (srtt <= 0) {
      this.rate = Infinity
      return
    }
    const gain = slowStart ? PACING_GAIN_SLOW_START : PACING_GAIN
    this.rate = gain * cwnd / (srtt / 1000)
  }

  /** The most the budget holds. */
  private maxBurst (): number {
    const burst = PACING_BURST_PACKETS * MAX_DATAGRAM_SIZE
    if (!Number.isFinite(this.rate)) return burst
    return Math.max(burst, this.rate * MIN_PACING_DELAY / 1000)
  }

  /** What may be sent at `now` without waiting; the rate is finite. */
  private budgetAt (now: number): number {
    if (this.lastSendTime === undefined) return this.maxBurst()
    const b = this.budgetAtLastSend + this.rate * (now - this.lastSendTime) / 1000
    return Math.min(b, this.maxBurst())
  }

  /**
   * Milliseconds until the next packet may be sent: 0 when the budget holds
   * a full datagram, otherwise the time it takes to (rounded up), and at
   * least MIN_PACING_DELAY.
   */
  timeUntilSend (): number {
    if (!Number.isFinite(this.rate)) return 0
    const short = MAX_DATAGRAM_SIZE - this.budgetAt(this.clock.now())
    if (short <= 0) return 0
    return Math.max(MIN_PACING_DELAY, Math.ceil(short / this.rate * 1000))
  }

  /** Draws a sent packet from the budget. */
  onPacketSent (size: number): void {
    if (!Number.isFinite(this.rate)) return
    const now = this.clock.now()
    this.budgetAtLastSend = this.budgetAt(now) - size
    this.lastSendTime = now
  }

  /** Bytes that may be sent now without waiting (negative after an overshoot); Infinity when unpaced. */
  get budget (): number {
    return Number.isFinite(this.rate) ? this.budgetAt(this.clock.now()) : Infinity
  }
}
