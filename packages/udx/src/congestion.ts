import type { Clock, TimerHandle } from './clock.js'
import {
  BETA_CUBIC,
  CUBIC_C,
  INITIAL_CWND,
  INITIAL_RTT_VAR,
  INITIAL_SMOOTHED_RTT,
  INITIAL_SSTHRESH,
  LOSS_TIMER_GRANULARITY,
  MAX_ACK_DELAY,
  MAX_DATAGRAM_SIZE,
  MIN_CWND,
  PERSISTENT_CONGESTION_THRESHOLD
} from './constants.js'
import { Pacer } from './pacer.js'

/**
 * RTT estimation (RFC 9002 §5), the probe timeout (§6.2) and CUBIC
 * congestion control, as go-udx v0.3.0's CongestionController. Durations are
 * milliseconds.
 *
 * There is one PTO timer per connection, armed from the last data packet
 * sent. When it fires, nothing has been acknowledged for a PTO: onProbe
 * re-sends a probe or two, and the next PTO waits twice as long, until an
 * ACK arrives. The window is untouched; loss is declared only once an ACK
 * proves it (SentPacketManager.detectLost). This replaced one retransmission
 * deadline per packet, which on a radio gap re-sent the whole window at once
 * and again on every backoff, past the window and the pacer.
 */
export class CongestionController {
  readonly pacer: Pacer
  private readonly clock: Clock
  private readonly lastSentSequence: () => number

  /** Called when the PTO fires: the connection sends its probes. */
  onProbe: (() => void) | undefined

  private cwndBytes = INITIAL_CWND
  private ssthreshBytes = INITIAL_SSTHRESH
  private inflightBytes = 0

  // CUBIC
  private wMax = 0
  private k = 0
  private epochStart: number | undefined

  // RTT. Until the first sample srtt and rttvar are the initial estimates
  // and min RTT is 0.
  private srtt = INITIAL_SMOOTHED_RTT
  private rttvar = INITIAL_RTT_VAR
  private minRttMs = 0
  private latestRttMs = 0
  private sampled = false

  // Recovery
  private recovering = false
  private recoveryEndSeq = -1

  // PTO
  private ptoTimer: TimerHandle | undefined
  private ptoFired = 0
  private lastSendTime: number | undefined

  /**
   * @param lastSentSequence the sequence of the last data packet sent (-1 if
   *   none); a recovery epoch ends once an ACK passes it.
   */
  constructor (clock: Clock, lastSentSequence: () => number) {
    this.clock = clock
    this.lastSentSequence = lastSentSequence
    this.pacer = new Pacer(clock)
  }

  get cwnd (): number { return this.cwndBytes }
  get ssthresh (): number { return this.ssthreshBytes }
  get inflight (): number { return this.inflightBytes }
  get smoothedRtt (): number { return this.srtt }
  get rttVar (): number { return this.rttvar }
  /** The smallest RTT sample seen; 0 before the first. */
  get minRtt (): number { return this.minRttMs }
  get latestRtt (): number { return this.latestRttMs }
  get inRecovery (): boolean { return this.recovering }
  /** Whether an RTT sample has been taken. */
  get hasRttSample (): boolean { return this.sampled }
  /** PTOs fired since the last ACK that acknowledged something. */
  get ptoCount (): number { return this.ptoFired }
  get ptoArmed (): boolean { return this.ptoTimer !== undefined }

  /**
   * The base probe timeout (RFC 9002 §6.2.1): SRTT + max(4·RTTVAR, 1 ms) +
   * MAX_ACK_DELAY. There is no floor beyond the granularity and no cap: on a
   * 30 ms path a loss costs about 60 ms, not the 200 ms floor this had, and a
   * slow path's PTO is bounded by the backoff and the idle timeout. Before
   * the first sample it is about a second (INITIAL_SMOOTHED_RTT).
   */
  pto (): number {
    return this.srtt + Math.max(4 * this.rttvar, LOSS_TIMER_GRANULARITY) + MAX_ACK_DELAY
  }

  /** How long a span of loss must cover to be persistent congestion (§7.6.1): three PTOs. */
  persistentCongestionDuration (): number {
    return this.pto() * PERSISTENT_CONGESTION_THRESHOLD
  }

  canSend (bytes: number): boolean {
    return this.inflightBytes + bytes <= this.cwndBytes
  }

  /** Bytes the window admits right now. */
  available (): number {
    return Math.max(0, this.cwndBytes - this.inflightBytes)
  }

  /** A data packet entered flight: charged to the window, and the PTO measures from it. */
  onPacketSent (bytes: number): void {
    this.inflightBytes += bytes
    this.lastSendTime = this.clock.now()
    this.armPto()
  }

  /**
   * One call per ACK frame that newly acknowledged something, with every
   * data byte it acknowledged. `sentTime` is the send time of the frame's
   * largest acknowledged packet when that packet is newly acknowledged (the
   * RTT sample), otherwise undefined.
   */
  onPacketsAcked (bytes: number, sentTime: number | undefined, ackDelay: number, largestAcked: number): void {
    this.inflightBytes = Math.max(0, this.inflightBytes - bytes)
    if (sentTime !== undefined) {
      this.updateRtt(sentTime, ackDelay)
      if (this.recovering && largestAcked > this.recoveryEndSeq) this.recovering = false
      if (!this.recovering) {
        if (this.cwndBytes < this.ssthreshBytes) {
          this.cwndBytes += bytes
        } else {
          this.cubicUpdate(bytes)
        }
      }
    }

    // An ACK is proof the path answers: the PTO backoff starts over
    // (RFC 9002 §6.2.1), measured from the last send.
    this.stopPto()
    this.ptoFired = 0
    if (this.inflightBytes > 0) this.armPto()
    this.updatePacingRate()
  }

  /**
   * The RTT estimates from a new sample (RFC 9002 §5.1-5.3). The first sample
   * replaces the initial estimates as measured. Later ones subtract the
   * receiver's reported ACK delay (capped at MAX_ACK_DELAY), but only when
   * that leaves them at or above min RTT; min RTT is always taken from the
   * unadjusted sample.
   */
  private updateRtt (sentTime: number, ackDelay: number): void {
    let latest = this.clock.now() - sentTime
    if (latest < 0) return

    if (!this.sampled) {
      this.sampled = true
      this.latestRttMs = latest
      this.minRttMs = latest
      this.srtt = latest
      this.rttvar = latest / 2
      return
    }

    if (latest < this.minRttMs) this.minRttMs = latest
    const delay = Math.min(ackDelay, MAX_ACK_DELAY)
    if (latest >= this.minRttMs + delay) latest -= delay
    this.latestRttMs = latest
    this.rttvar = (3 * this.rttvar + Math.abs(this.srtt - latest)) / 4
    this.srtt = (7 * this.srtt + latest) / 8
  }

  /**
   * Tells the pacer the window and RTT it paces by, once an RTT sample
   * exists; before one the pacer is unlimited and the initial window is the
   * burst. Called after every change to either.
   */
  private updatePacingRate (): void {
    if (!this.sampled) return
    this.pacer.updateRate(this.cwndBytes, this.srtt, this.cwndBytes < this.ssthreshBytes)
  }

  private cubicUpdate (bytes: number): void {
    const now = this.clock.now()
    if (this.epochStart === undefined) this.epochStart = now
    const t = (now - this.epochStart) / 1000

    // W(t) = C·(t − K)³ + wMax, in MSS units.
    const wCubic = CUBIC_C * Math.pow(t - this.k, 3) + this.wMax / MAX_DATAGRAM_SIZE
    const target = Math.trunc(wCubic * MAX_DATAGRAM_SIZE)
    // TCP-friendly estimate.
    const wTcp = this.cwndBytes + Math.floor(MAX_DATAGRAM_SIZE * bytes / this.cwndBytes)

    if (target < wTcp) {
      this.cwndBytes = wTcp
    } else if (this.cwndBytes < target) {
      this.cwndBytes += Math.floor((target - this.cwndBytes) * MAX_DATAGRAM_SIZE / this.cwndBytes)
    } else {
      this.cwndBytes += Math.floor(MAX_DATAGRAM_SIZE * bytes / this.cwndBytes)
    }
  }

  /**
   * Contracts the window, without touching inflight, at most once per
   * recovery epoch (RFC 9002 §7.3.1): the first loss of a round trip cuts
   * the window and the rest of that round trip's losses are absorbed.
   */
  onCongestionEvent (): void {
    if (this.recovering) return
    this.epochStart = this.clock.now()
    this.recovering = true
    this.recoveryEndSeq = this.lastSentSequence()

    this.wMax = this.cwndBytes
    this.ssthreshBytes = Math.max(MIN_CWND, Math.trunc(this.wMax * BETA_CUBIC))
    this.k = Math.cbrt(this.wMax / MAX_DATAGRAM_SIZE * (1 - BETA_CUBIC) / CUBIC_C)
    this.cwndBytes = this.ssthreshBytes
    this.updatePacingRate()
  }

  /**
   * Packets declared lost: their bytes leave flight (RFC 9002 §7.3) and the
   * window contracts, once per epoch. When the data is re-sent it is booked
   * again by onPacketSent, as the new packet it is.
   */
  onPacketLost (bytes: number): void {
    this.inflightBytes = Math.max(0, this.inflightBytes - bytes)
    this.onCongestionEvent()
  }

  /**
   * The §7.6 reaction: the window collapses to the minimum and recovery ends,
   * so sending restarts in slow start, as after an outage long enough that
   * everything sent in three PTOs was lost. Called after onPacketLost.
   */
  onPersistentCongestion (): void {
    this.cwndBytes = MIN_CWND
    this.recovering = false
    this.recoveryEndSeq = -1
    this.epochStart = undefined
    this.updatePacingRate()
  }

  /** The wait for the next probe: the base PTO backed off by the PTOs fired without an ACK. */
  private nextPto (): number {
    return this.pto() * 2 ** Math.min(this.ptoFired, 20)
  }

  /** Arms the PTO to fire nextPto() after the last send. */
  private armPto (): void {
    this.stopPto()
    let wait = this.nextPto()
    if (this.lastSendTime !== undefined) wait = Math.min(wait, this.lastSendTime + wait - this.clock.now())
    this.ptoTimer = this.clock.setTimeout(() => this.onPtoTimeout(), Math.max(wait, LOSS_TIMER_GRANULARITY))
  }

  private stopPto (): void {
    if (this.ptoTimer !== undefined) this.clock.clearTimeout(this.ptoTimer)
    this.ptoTimer = undefined
  }

  /** Nothing acknowledged for a PTO: probe, and wait twice as long for the next. */
  private onPtoTimeout (): void {
    this.ptoTimer = undefined
    this.ptoFired++
    this.onProbe?.()
    if (this.inflightBytes > 0 && this.ptoTimer === undefined) {
      // Measured from now: the probe just sent is the last send.
      this.lastSendTime = this.clock.now()
      this.ptoTimer = this.clock.setTimeout(() => this.onPtoTimeout(), this.nextPto())
    }
  }

  /** Stops the PTO timer. */
  destroy (): void {
    this.stopPto()
  }
}
