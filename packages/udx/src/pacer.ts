import type { Clock } from './clock.js'
import { PACING_GAIN } from './constants.js'

/**
 * Spreads data packets out at PACING_GAIN × cwnd / minRTT so a window isn't
 * sent as one burst. The rate starts unlimited and is updated by the
 * congestion controller when minRTT drops or on a congestion event.
 */
export class Pacer {
  private readonly clock: Clock
  private rate = Infinity // bytes per second
  private nextSendTime: number

  constructor (clock: Clock) {
    this.clock = clock
    this.nextSendTime = clock.now()
  }

  /** Bytes per second; Infinity when unpaced. */
  get pacingRate (): number {
    return this.rate
  }

  updateRate (cwnd: number, minRtt: number): void {
    // go-udx works in whole microseconds.
    const minRttSec = Math.floor(minRtt * 1000) / 1e6
    this.rate = minRttSec <= 0 ? Infinity : (PACING_GAIN * cwnd) / minRttSec
  }

  /** Milliseconds to wait before the next send; 0 to send now. */
  timeUntilSend (): number {
    const now = this.clock.now()
    return now < this.nextSendTime ? this.nextSendTime - now : 0
  }

  onPacketSent (size: number): void {
    if (this.rate === Infinity) return
    const interval = Math.floor(size / this.rate * 1e6) / 1000
    const now = this.clock.now()
    this.nextSendTime = (now > this.nextSendTime ? now : this.nextSendTime) + interval
  }
}
