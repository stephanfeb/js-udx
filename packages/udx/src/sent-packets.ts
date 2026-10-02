import type { Clock, TimerHandle } from './clock.js'
import {
  LOSS_REORDER_THRESHOLD,
  LOSS_TIME_THRESHOLD,
  LOSS_TIMER_GRANULARITY,
  MAX_RETRANSMIT_BACKOFF,
  MIN_RETRANSMIT_TIMEOUT
} from './constants.js'
import type { CongestionController } from './congestion.js'
import type { AckFrame, Frame } from './frames.js'

/** A data-bearing packet awaiting acknowledgement. */
export interface SentPacket {
  /** Current sequence; a retransmission moves the packet to a fresh one. */
  sequence: number
  /** When the current sequence was sent. */
  sentTime: number
  /** Encoded datagram size, as charged to the congestion window. */
  size: number
  frames: Frame[]
  destinationStreamId: number
  sourceStreamId: number
  retransmitCount: number
  lastRetransmit: number
  /** When the retransmission timer next fires for this packet. */
  deadline: number
}

export interface SentPacketManagerOptions {
  clock: Clock
  congestion: CongestionController
  /**
   * Puts a timer-driven retransmission back on the wire. The packet's sequence
   * has already been moved to `sequence`.
   */
  onRetransmit?: (packet: SentPacket, sequence: number) => void
}

/**
 * Tracks sent data packets, processes ACKs, detects loss and retransmits.
 * Port of go-udx's PacketManager.
 *
 * Retransmission never reuses a sequence number (RFC 9000 §12.3): the packet
 * is re-sent under a fresh one and its tracking moves with it, with no retry
 * cap. Delivery is bounded by the connection's idle timeout instead. Both v3
 * stacks reassemble streams by byte offset, so the receiver discards
 * duplicates.
 *
 * Unlike go-udx's one timer per packet, there is one timer for the whole
 * manager, armed at the earliest packet deadline. Thousands of setTimeouts
 * cost more in Node than a scan when the timer fires.
 */
export class SentPacketManager {
  private readonly clock: Clock
  private readonly cc: CongestionController
  onRetransmit: ((packet: SentPacket, sequence: number) => void) | undefined

  private nextSeq = 0
  private lastSentSeq = -1
  /** Keyed by each packet's current sequence. */
  private readonly sent = new Map<number, SentPacket>()

  private timer: TimerHandle | undefined
  private timerDeadline = Infinity

  constructor (opts: SentPacketManagerOptions) {
    this.clock = opts.clock
    this.cc = opts.congestion
    this.onRetransmit = opts.onRetransmit
  }

  /** Allocates the next sequence number (u32, wrapping). */
  nextSequence (): number {
    const seq = this.nextSeq
    this.nextSeq = (this.nextSeq + 1) >>> 0
    return seq
  }

  /** The sequence of the last data packet sent, or -1. */
  get lastSentSequence (): number {
    return this.lastSentSeq
  }

  get pendingCount (): number {
    return this.sent.size
  }

  get timerArmed (): boolean {
    return this.timer !== undefined
  }

  get (sequence: number): SentPacket | undefined {
    return this.sent.get(sequence)
  }

  /** All tracked packets, in send order. */
  packets (): IterableIterator<SentPacket> {
    return this.sent.values()
  }

  /** SRTT + 4·RTTVAR, clamped to [200, 5000] ms. */
  retransmitTimeout (): number {
    return this.cc.rto()
  }

  /**
   * The wait before retransmission attempt `attempt` (1-based): exponential
   * from the RTO up to a ceiling of max(MAX_RETRANSMIT_BACKOFF, RTO). The
   * ceiling never drops below the RTO, or a slow path would be flooded with
   * duplicates before the peer could answer.
   */
  retransmitBackoff (attempt: number): number {
    const rto = this.retransmitTimeout()
    const ceiling = Math.max(MAX_RETRANSMIT_BACKOFF, rto)
    let backoff = ceiling
    const shift = attempt - 1
    if (shift < 32) {
      const scaled = rto * 2 ** shift
      if (scaled < ceiling) backoff = scaled
    }
    return Math.max(MIN_RETRANSMIT_TIMEOUT, backoff)
  }

  /**
   * How long a packet must have been outstanding before age alone marks it
   * lost: 9/8 of max(SRTT, latest RTT), at least the timer granularity
   * (RFC 9002 §6.1.2).
   */
  lossDelay (): number {
    const rtt = Math.max(this.cc.smoothedRtt, this.cc.latestRtt)
    return Math.max(LOSS_TIMER_GRANULARITY, rtt * LOSS_TIME_THRESHOLD)
  }

  /** Registers a data packet that has just been sent under `packet.sequence`. */
  onPacketSent (packet: Omit<SentPacket, 'sentTime' | 'retransmitCount' | 'lastRetransmit' | 'deadline'>): SentPacket {
    const now = this.clock.now()
    const tracked: SentPacket = {
      ...packet,
      sentTime: now,
      retransmitCount: 0,
      lastRetransmit: 0,
      deadline: now + this.retransmitTimeout()
    }
    this.lastSentSeq = tracked.sequence
    this.sent.set(tracked.sequence, tracked)
    this.armTimer(tracked.deadline)
    return tracked
  }

  /** Removes and returns the packets an ACK frame newly acknowledges. */
  onAckFrame (frame: AckFrame): SentPacket[] {
    const acked: SentPacket[] = []
    const take = (seq: number): void => {
      const pkt = this.sent.get(seq)
      if (pkt !== undefined) {
        this.sent.delete(seq)
        acked.push(pkt)
      }
    }

    for (let i = 0; i < frame.firstAckRangeLength; i++) take((frame.largestAcked - i) >>> 0)

    let current = frame.largestAcked - frame.firstAckRangeLength
    for (const r of frame.ranges) {
      const rangeEnd = current - r.gap
      for (let i = 0; i < r.length; i++) take((rangeEnd - i) >>> 0)
      current = rangeEnd - r.length
    }

    if (this.sent.size === 0) this.disarmTimer()
    return acked
  }

  /**
   * The packets an ACK frame's gaps show lost. A gap only counts once the
   * packet is LOSS_REORDER_THRESHOLD sequences behind the largest acknowledged
   * or older than lossDelay(); until then reordering still explains it.
   * Packets retransmitted within the last RTO are skipped.
   */
  detectLost (frame: AckFrame): SentPacket[] {
    if (frame.ranges.length === 0) return []
    const rto = this.retransmitTimeout()
    const lossDelay = this.lossDelay()
    const now = this.clock.now()
    const lost: SentPacket[] = []

    let cursor = frame.largestAcked - frame.firstAckRangeLength
    for (const r of frame.ranges) {
      for (let g = 0; g < r.gap; g++) {
        if (cursor < 0) break
        const pkt = this.sent.get(cursor)
        if (pkt !== undefined) {
          const recentlyResent = pkt.retransmitCount > 0 && now - pkt.lastRetransmit < rto
          const behind = frame.largestAcked - cursor
          const inFlight = behind < LOSS_REORDER_THRESHOLD && now - pkt.sentTime < lossDelay
          if (!recentlyResent && !inFlight) lost.push(pkt)
        }
        cursor--
      }
      cursor -= r.length
    }
    return lost
  }

  /**
   * Moves an unacknowledged packet to a fresh sequence for re-sending and
   * returns that sequence, or undefined if the packet was already acknowledged
   * or was retransmitted within the last RTO (so an RTO and a SACK trigger for
   * one loss produce one resend).
   *
   * The packet's send time restarts, so its RTT sample and loss timer measure
   * from the resend. Inflight is untouched: the bytes never left flight.
   */
  retransmit (pkt: SentPacket): number | undefined {
    const oldSeq = pkt.sequence
    if (this.sent.get(oldSeq) !== pkt) return undefined
    const now = this.clock.now()
    if (pkt.retransmitCount > 0 && now - pkt.lastRetransmit < this.retransmitTimeout()) return undefined

    const newSeq = this.nextSequence()
    this.sent.delete(oldSeq)
    pkt.sequence = newSeq
    pkt.retransmitCount++
    pkt.lastRetransmit = now
    pkt.sentTime = now
    pkt.deadline = now + this.retransmitBackoff(pkt.retransmitCount)
    this.sent.set(newSeq, pkt)
    this.armTimer(pkt.deadline)
    return newSeq
  }

  private armTimer (deadline: number): void {
    if (this.timer !== undefined && deadline >= this.timerDeadline) return
    this.disarmTimer()
    this.timerDeadline = deadline
    this.timer = this.clock.setTimeout(() => this.onTimer(), deadline - this.clock.now())
  }

  private disarmTimer (): void {
    if (this.timer !== undefined) this.clock.clearTimeout(this.timer)
    this.timer = undefined
    this.timerDeadline = Infinity
  }

  /**
   * Retransmits every packet whose deadline has passed. A timer-driven resend
   * is a probe: it leaves the congestion window alone.
   */
  private onTimer (): void {
    this.timer = undefined
    this.timerDeadline = Infinity
    const now = this.clock.now()

    const due: SentPacket[] = []
    for (const pkt of this.sent.values()) if (pkt.deadline <= now) due.push(pkt)

    for (const pkt of due) {
      const seq = this.retransmit(pkt)
      if (seq !== undefined) {
        this.onRetransmit?.(pkt, seq)
      } else if (this.sent.get(pkt.sequence) === pkt) {
        // Collapsed into a recent resend. Go stops this packet's timer here,
        // leaving it to SACK alone; keep a deadline so the timer still backs it up.
        pkt.deadline = pkt.lastRetransmit + this.retransmitTimeout()
      }
    }

    // Every remaining deadline is now in the future. The floor is a backstop:
    // a stale deadline must not turn the timer into a spin.
    let next = Infinity
    for (const pkt of this.sent.values()) if (pkt.deadline < next) next = pkt.deadline
    if (next !== Infinity) this.armTimer(Math.max(next, now + LOSS_TIMER_GRANULARITY))
  }

  /** Cancels the timer and forgets every packet. */
  destroy (): void {
    this.disarmTimer()
    this.sent.clear()
  }
}
