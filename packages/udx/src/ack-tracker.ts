import { ACK_HISTORY, MAX_ACK_RANGES } from './constants.js'
import { type AckFrame, type AckRange, FrameType } from './frames.js'

/**
 * Records which data-bearing packets have arrived, so an ACK can carry SACK
 * ranges, and says whether each arrival was out of order. Port of go-udx's
 * recvTracker.
 *
 * It keeps the largest sequence and a bitmap of the ACK_HISTORY sequences
 * below it, indexed by sequence modulo the history. Sequences further back are
 * forgotten: a packet that late is not acknowledged, and the sender recovers
 * it by retransmitting under a fresh sequence.
 *
 * Sequences compare as plain u32 values, as in go-udx.
 */
export class AckTracker {
  private largestSeq = 0
  private any = false
  private readonly bits = new Uint32Array(ACK_HISTORY / 32)

  /** Whether anything has been recorded. */
  get hasAny (): boolean {
    return this.any
  }

  /** The largest sequence recorded (0 if none). */
  get largest (): number {
    return this.largestSeq
  }

  has (seq: number): boolean {
    if (!this.any || seq > this.largestSeq || this.largestSeq - seq >= ACK_HISTORY) return false
    const i = seq % ACK_HISTORY
    return ((this.bits[i >>> 5] as number) & (1 << (i & 31))) !== 0
  }

  private set (seq: number): void {
    const i = seq % ACK_HISTORY
    this.bits[i >>> 5] = (this.bits[i >>> 5] as number) | (1 << (i & 31))
  }

  private clear (seq: number): void {
    const i = seq % ACK_HISTORY
    this.bits[i >>> 5] = (this.bits[i >>> 5] as number) & ~(1 << (i & 31))
  }

  /**
   * Records `seq` and returns true if it arrived out of order: below the
   * largest seen (late, or filling a gap) or leaving a hole behind it. Either
   * is the signal to acknowledge at once. A duplicate is not out of order.
   */
  add (seq: number): boolean {
    if (!this.any) {
      this.any = true
      this.largestSeq = seq
      this.set(seq)
      return false
    }
    if (seq > this.largestSeq) {
      const gap = seq - this.largestSeq - 1
      // The slots between the old largest and seq are reused for sequences
      // that have not arrived, so they must read as missing.
      if (seq - this.largestSeq >= ACK_HISTORY) {
        this.bits.fill(0)
      } else {
        for (let s = this.largestSeq + 1; s < seq; s++) this.clear(s)
      }
      this.largestSeq = seq
      this.set(seq)
      return gap > 0
    }
    if (seq === this.largestSeq) return false
    if (this.largestSeq - seq >= ACK_HISTORY) return true
    if (this.has(seq)) return false
    this.set(seq)
    return true
  }

  /**
   * Builds the ACK: the largest sequence with the run received directly below
   * it, then up to MAX_ACK_RANGES (gap, run) pairs walking down. A gap longer
   * than 255 ends the frame, since the gap field is one byte.
   */
  frame (ackDelay: number): AckFrame {
    if (!this.any) {
      return { type: FrameType.Ack, largestAcked: 0, ackDelay, firstAckRangeLength: 1, ranges: [] }
    }
    let cursor = this.largestSeq
    let steps = 0

    const run = (): number => {
      let n = 0
      while (steps < ACK_HISTORY && this.has(cursor)) {
        n++
        steps++
        if (cursor === 0) {
          steps = ACK_HISTORY
          break
        }
        cursor--
      }
      return n
    }
    const gap = (): number => {
      let n = 0
      while (steps < ACK_HISTORY && n < 255 && !this.has(cursor)) {
        n++
        steps++
        if (cursor === 0) {
          steps = ACK_HISTORY
          break
        }
        cursor--
      }
      return n
    }

    const firstAckRangeLength = run()
    const ranges: AckRange[] = []
    while (ranges.length < MAX_ACK_RANGES && steps < ACK_HISTORY) {
      const g = gap()
      if (g === 0) break
      const n = run()
      if (n === 0) break
      ranges.push({ gap: g, length: n })
    }
    return { type: FrameType.Ack, largestAcked: this.largestSeq, ackDelay, firstAckRangeLength, ranges }
  }
}

/** Whether an ACK frame acknowledges `seq` (go-udx's range encoding: raw counts). */
export function ackCovers (frame: AckFrame, seq: number): boolean {
  if (seq <= frame.largestAcked && frame.largestAcked - seq < frame.firstAckRangeLength) return true
  let cursor = frame.largestAcked - frame.firstAckRangeLength
  for (const r of frame.ranges) {
    const end = cursor - r.gap
    if (seq <= end && end - seq < r.length) return true
    cursor = end - r.length
  }
  return false
}
