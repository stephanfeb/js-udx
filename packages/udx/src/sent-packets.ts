import type { Clock, TimerHandle } from './clock.js'
import {
  LOSS_REORDER_THRESHOLD,
  LOSS_TIME_THRESHOLD,
  LOSS_TIMER_GRANULARITY
} from './constants.js'
import type { CongestionController } from './congestion.js'
import type { AckFrame, Frame } from './frames.js'

/** A numbered packet awaiting acknowledgement. */
export interface SentPacket {
  /** Current sequence; a re-send moves the packet to a fresh one. */
  sequence: number
  /** When the packet last went on the wire: the RTT sample and loss detection measure from it. */
  sentTime: number
  /** When it first did: persistent congestion measures how long the data has gone undelivered. */
  firstSentTime: number
  /** Encoded datagram size, as charged to the congestion window. */
  size: number
  frames: Frame[]
  destinationStreamId: number
  sourceStreamId: number
  retransmitCount: number
  lastRetransmit: number
  /**
   * A path-MTU probe: numbered and tracked so its ACK or loss is reported,
   * but never re-sent, not in flight, and no congestion signal when lost.
   */
  probe: boolean
  acked: boolean
}

/** What a caller gives onPacketSent; the rest is filled in. */
export interface SentPacketInfo {
  sequence: number
  size: number
  frames: Frame[]
  destinationStreamId: number
  sourceStreamId: number
  probe?: boolean
}

export interface SentPacketManagerOptions {
  clock: Clock
  congestion: CongestionController
  /**
   * Told what loss detection declared lost, in sequence order, whether an ACK
   * or the loss timer revealed it. The packets are no longer tracked: the
   * receiver re-sends them (resend) and books them.
   */
  onLost?: (lost: SentPacket[]) => void
}

/**
 * Tracks sent packets, matches ACKs to them, and declares loss the way RFC
 * 9002 §6.1 does: from acknowledgements, never from a per-packet timer. Port
 * of go-udx v0.3.0's PacketManager.
 *
 * A packet is lost once a later one has been acknowledged and it is either
 * LOSS_REORDER_THRESHOLD sequences behind the largest acknowledged or older
 * than lossDelay() (9/8 of the RTT). Packets behind but not yet old enough
 * wait on the loss timer, which declares them lost when the delay elapses
 * without another ACK. Every outstanding packet below the largest
 * acknowledged is considered, not only those in the frame's gaps: a receiver
 * that acknowledges one packet at a time sends no gaps, and the ranges an ACK
 * carries are bounded.
 *
 * A re-send never reuses a sequence number (RFC 9000 §12.3): the packet goes
 * out under a fresh one and its tracking moves with it. Both v3 stacks
 * reassemble streams by byte offset, so the receiver discards duplicates.
 *
 * Packets are kept in a Map in send order, which is ascending sequence order:
 * a re-send deletes the entry and adds it again under a larger number. (Like
 * go-udx, the comparisons assume the u32 sequence does not wrap within a
 * connection's outstanding packets.)
 */
export class SentPacketManager {
  private readonly clock: Clock
  private readonly cc: CongestionController
  onLost: ((lost: SentPacket[]) => void) | undefined

  private nextSeq = 0
  private lastSentSeq = -1
  /** Keyed by each packet's current sequence, in ascending order. */
  private readonly sent = new Map<number, SentPacket>()

  private largestAck = -1
  private lastAckTime: number | undefined
  private ackBeforeLossTime: number | undefined
  private lossTimer: TimerHandle | undefined

  constructor (opts: SentPacketManagerOptions) {
    this.clock = opts.clock
    this.cc = opts.congestion
    this.onLost = opts.onLost
  }

  /** Allocates the next sequence number (u32, wrapping). */
  nextSequence (): number {
    const seq = this.nextSeq
    this.nextSeq = (this.nextSeq + 1) >>> 0
    return seq
  }

  /** The sequence of the last packet sent, or -1. */
  get lastSentSequence (): number {
    return this.lastSentSeq
  }

  /** The largest sequence the peer has acknowledged, or -1. */
  get largestAcked (): number {
    return this.largestAck
  }

  /** Tracked packets, path-MTU probes included. */
  get pendingCount (): number {
    return this.sent.size
  }

  get lossTimerArmed (): boolean {
    return this.lossTimer !== undefined
  }

  /**
   * When the ACK before the one (or the loss timer) that last declared loss
   * arrived: persistent congestion asks whether anything was acknowledged
   * since the oldest lost packet went out, which the ACK that revealed the
   * loss does not answer.
   */
  get ackBeforeLoss (): number | undefined {
    return this.ackBeforeLossTime
  }

  get (sequence: number): SentPacket | undefined {
    return this.sent.get(sequence)
  }

  /** All tracked packets, in sequence order. */
  packets (): IterableIterator<SentPacket> {
    return this.sent.values()
  }

  /**
   * How long a packet must have been outstanding before age alone marks it
   * lost: 9/8 of max(SRTT, latest RTT), at least the timer granularity (RFC
   * 9002 §6.1.2). The larger of the two: after a sudden rise in delay the
   * smoothed estimate lags, and would declare a whole window lost.
   */
  lossDelay (): number {
    const rtt = Math.max(this.cc.smoothedRtt, this.cc.latestRtt)
    return Math.max(LOSS_TIMER_GRANULARITY, rtt * LOSS_TIME_THRESHOLD)
  }

  /** Registers a packet that has just been sent under `info.sequence`. */
  onPacketSent (info: SentPacketInfo): SentPacket {
    const now = this.clock.now()
    const tracked: SentPacket = {
      ...info,
      sentTime: now,
      firstSentTime: now,
      retransmitCount: 0,
      lastRetransmit: 0,
      probe: info.probe ?? false,
      acked: false
    }
    this.lastSentSeq = tracked.sequence
    this.sent.set(tracked.sequence, tracked)
    return tracked
  }

  /**
   * Removes and returns the packets an ACK frame newly acknowledges. The
   * caller books them with the congestion controller and then runs
   * detectLost, in that order (RFC 9002 appendix A.7: the acknowledged bytes
   * grow the window before the loss contracts it).
   */
  onAckFrame (frame: AckFrame): SentPacket[] {
    const acked: SentPacket[] = []
    const take = (seq: number): void => {
      const pkt = this.sent.get(seq)
      if (pkt !== undefined) {
        pkt.acked = true
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

    if (acked.length > 0) {
      this.ackBeforeLossTime = this.lastAckTime
      this.lastAckTime = this.clock.now()
      if (frame.largestAcked > this.largestAck) this.largestAck = frame.largestAcked
    }
    return acked
  }

  /**
   * Runs loss detection over everything outstanding (RFC 9002 §6.1) and
   * returns what it declared lost, in sequence order, also handed to onLost.
   * Called after each ACK that acknowledged something; the loss timer calls
   * it when the time threshold elapses.
   */
  detectLost (): SentPacket[] {
    const lost = this.detect(this.clock.now())
    if (lost.length > 0) this.onLost?.(lost)
    return lost
  }

  private detect (now: number): SentPacket[] {
    this.stopLossTimer()
    if (this.largestAck < 0) return []
    const delay = this.lossDelay()
    const lost: SentPacket[] = []
    let nextLoss = Infinity
    for (const [seq, pkt] of this.sent) {
      if (seq > this.largestAck) break
      if (this.largestAck - seq >= LOSS_REORDER_THRESHOLD || now >= pkt.sentTime + delay) {
        lost.push(pkt)
      } else if (pkt.sentTime + delay < nextLoss) {
        nextLoss = pkt.sentTime + delay
      }
    }
    for (const pkt of lost) this.sent.delete(pkt.sequence)
    if (nextLoss !== Infinity) {
      this.lossTimer = this.clock.setTimeout(() => this.onLossTimer(), Math.max(nextLoss - now, LOSS_TIMER_GRANULARITY))
    }
    return lost
  }

  /** Declares lost what has become old enough since the last ACK. */
  private onLossTimer (): void {
    this.lossTimer = undefined
    this.ackBeforeLossTime = this.lastAckTime
    const lost = this.detect(this.clock.now())
    if (lost.length > 0) this.onLost?.(lost)
  }

  private stopLossTimer (): void {
    if (this.lossTimer !== undefined) this.clock.clearTimeout(this.lossTimer)
    this.lossTimer = undefined
  }

  /**
   * Gives a packet a fresh sequence for another transmission and tracks it
   * under that number, returning the number, or undefined when the packet
   * has been acknowledged or is a path-MTU probe and must not go out again.
   *
   * The packet may be tracked (a PTO probe re-sends the oldest outstanding
   * packet, which stays outstanding) or not (loss detection removed it):
   * either way it is tracked under the new number afterwards, with its send
   * time reset so the RTT sample and loss detection measure from this send.
   * Congestion accounting is the caller's: a probe's bytes are still in
   * flight, a lost packet's left flight when it was declared lost.
   */
  resend (pkt: SentPacket): number | undefined {
    if (pkt.acked || pkt.probe) return undefined
    if (this.sent.get(pkt.sequence) === pkt) this.sent.delete(pkt.sequence)
    const now = this.clock.now()
    const seq = this.nextSequence()
    pkt.sequence = seq
    pkt.retransmitCount++
    pkt.lastRetransmit = now
    pkt.sentTime = now
    this.lastSentSeq = seq
    this.sent.set(seq, pkt)
    return seq
  }

  /**
   * Up to n outstanding packets, lowest sequence first, path-MTU probes left
   * out: what a PTO re-sends (RFC 9002 §6.2.4). A re-send takes a fresh
   * sequence, so the lowest numbers are the longest unsent.
   */
  oldestUnacked (n: number): SentPacket[] {
    const out: SentPacket[] = []
    for (const pkt of this.sent.values()) {
      if (out.length >= n) break
      if (!pkt.probe) out.push(pkt)
    }
    return out
  }

  /** Forgets a tracked packet that is being replaced by others (re-chunked), neither acknowledged nor lost. */
  drop (pkt: SentPacket): void {
    if (this.sent.get(pkt.sequence) === pkt) this.sent.delete(pkt.sequence)
  }

  /** Stops the loss timer and forgets every packet. */
  destroy (): void {
    this.stopLossTimer()
    this.sent.clear()
  }
}
