import type { Clock } from './clock.js'
import {
  BETA_CUBIC,
  CUBIC_C,
  INITIAL_CWND,
  INITIAL_MIN_RTT,
  INITIAL_RTT_VAR,
  INITIAL_SMOOTHED_RTT,
  INITIAL_SSTHRESH,
  LOSS_TIMER_GRANULARITY,
  MAX_ACK_DELAY,
  MAX_DATAGRAM_SIZE,
  MAX_RTO,
  MIN_CWND,
  MIN_RTO
} from './constants.js'
import { Pacer } from './pacer.js'

/**
 * RTT estimation (RFC 9002 §5) and CUBIC congestion control, ported from
 * go-udx's CongestionController. Durations are milliseconds.
 *
 * Two pieces of go-udx's controller are deliberately left out because go-udx
 * never wires them: the PTO probe timer (its callback is nil) and
 * duplicate-ACK fast retransmit. Per-packet RTOs and SACK loss detection in
 * SentPacketManager cover both.
 */
export class CongestionController {
  readonly pacer: Pacer
  private readonly clock: Clock
  private readonly lastSentSequence: () => number

  private cwndBytes = INITIAL_CWND
  private ssthreshBytes = INITIAL_SSTHRESH
  private inflightBytes = 0

  // CUBIC
  private wMax = 0
  private k = 0
  private epochStart: number | undefined

  // RTT
  private srtt = INITIAL_SMOOTHED_RTT
  private rttvar = INITIAL_RTT_VAR
  private minRttMs = INITIAL_MIN_RTT
  private latestRttMs = 0
  private firstSample = true

  // Recovery
  private recovering = false
  private recoveryEndSeq = -1

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
  get minRtt (): number { return this.minRttMs }
  get latestRtt (): number { return this.latestRttMs }
  get inRecovery (): boolean { return this.recovering }

  /**
   * The retransmission timeout: RFC 9002's PTO, SRTT + max(4·RTTVAR, 1 ms) +
   * MAX_ACK_DELAY, in whole milliseconds, clamped to [MIN_RTO, MAX_RTO].
   *
   * go-udx uses SRTT + 4·RTTVAR. On a steady path RTTVAR decays towards zero
   * and that collapses to the RTT itself, so every packet whose ACK the
   * receiver legitimately delays is resent. The difference only shows once
   * the RTT is above the 200 ms floor. It is sender-side only, with nothing
   * on the wire.
   */
  rto (): number {
    const ms = Math.floor(this.srtt) + Math.max(4 * Math.floor(this.rttvar), LOSS_TIMER_GRANULARITY) + MAX_ACK_DELAY
    return Math.min(MAX_RTO, Math.max(MIN_RTO, ms))
  }

  canSend (bytes: number): boolean {
    return this.inflightBytes + bytes <= this.cwndBytes
  }

  /** Bytes the window admits right now. */
  available (): number {
    return Math.max(0, this.cwndBytes - this.inflightBytes)
  }

  /** A data packet entered flight. Retransmissions don't call this: their bytes never left. */
  onPacketSent (bytes: number): void {
    this.inflightBytes += bytes
  }

  /**
   * One call per ACK frame with every byte it newly acknowledged. `sentTime`
   * is the send time of the frame's largest acknowledged packet when that
   * packet is newly acknowledged (the RTT sample), otherwise undefined.
   */
  onPacketsAcked (bytes: number, sentTime: number | undefined, ackDelay: number, largestAcked: number): void {
    this.inflightBytes = Math.max(0, this.inflightBytes - bytes)
    if (sentTime === undefined) return

    this.updateRtt(sentTime, ackDelay)
    if (this.recovering && largestAcked > this.recoveryEndSeq) this.recovering = false
    if (this.recovering) return
    if (this.cwndBytes < this.ssthreshBytes) {
      this.cwndBytes += bytes
    } else {
      this.cubicUpdate(bytes)
    }
  }

  private updateRtt (sentTime: number, ackDelay: number): void {
    const cappedDelay = Math.min(ackDelay, MAX_ACK_DELAY)
    let latest = this.clock.now() - sentTime
    if (latest > cappedDelay) latest -= cappedDelay
    this.latestRttMs = latest

    if (latest < this.minRttMs) {
      this.minRttMs = latest
      this.pacer.updateRate(this.cwndBytes, this.minRttMs)
    }
    if (this.firstSample) {
      this.firstSample = false
      this.srtt = latest
      this.rttvar = latest / 2
    } else {
      this.rttvar = (3 * this.rttvar + Math.abs(this.srtt - latest)) / 4
      this.srtt = (7 * this.srtt + latest) / 8
    }
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
   * Contracts the window for loss that is being recovered by retransmission,
   * without touching inflight: the bytes are re-sent, not abandoned. At most
   * once per recovery epoch (RFC 9002 §7.3.1).
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
    this.pacer.updateRate(this.cwndBytes, this.minRttMs)
  }

  /** A packet abandoned outright: its bytes leave flight and the window contracts. */
  onPacketLost (bytes: number): void {
    this.inflightBytes = Math.max(0, this.inflightBytes - bytes)
    this.onCongestionEvent()
  }
}
