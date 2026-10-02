import { describe, expect, it } from 'vitest'
import { ACK_HISTORY, AckTracker, FrameType } from '../src/index.js'

describe('AckTracker', () => {
  it('builds the empty frame before anything arrives', () => {
    expect(new AckTracker().frame(0)).toEqual({ type: FrameType.Ack, largestAcked: 0, ackDelay: 0, firstAckRangeLength: 1, ranges: [] })
  })

  it('reports in-order arrivals as in order and acks them as one run', () => {
    const r = new AckTracker()
    for (let seq = 0; seq < 10; seq++) expect(r.add(seq)).toBe(false)
    expect(r.frame(4)).toMatchObject({ largestAcked: 9, ackDelay: 4, firstAckRangeLength: 10, ranges: [] })
  })

  it('reports a hole, a gap-filler, and treats duplicates as in order', () => {
    const r = new AckTracker()
    r.add(1)
    r.add(2)
    expect(r.add(4)).toBe(true)
    expect(r.frame(0)).toMatchObject({ largestAcked: 4, firstAckRangeLength: 1, ranges: [{ gap: 1, length: 2 }] })
    expect(r.add(3)).toBe(true)
    expect(r.add(3)).toBe(false)
    expect(r.add(4)).toBe(false)
    expect(r.frame(0)).toMatchObject({ largestAcked: 4, firstAckRangeLength: 4, ranges: [] })
  })

  it('carries at most five extra ranges', () => {
    const r = new AckTracker()
    for (let seq = 0; seq < 40; seq += 2) r.add(seq)
    const f = r.frame(0)
    expect(f.ranges).toHaveLength(5)
    expect(f.ranges.every(x => x.gap === 1 && x.length === 1)).toBe(true)
  })

  it('ends the frame at a gap longer than 255', () => {
    const r = new AckTracker()
    r.add(0)
    r.add(300)
    expect(r.frame(0)).toMatchObject({ largestAcked: 300, firstAckRangeLength: 1, ranges: [] })
  })

  it('acknowledges sequence 0 at the bottom of a run', () => {
    const r = new AckTracker()
    r.add(0)
    r.add(1)
    expect(r.frame(0)).toMatchObject({ largestAcked: 1, firstAckRangeLength: 2 })
  })

  // go-udx TestRecvTracker_ForgetsBeyondHistory
  it('forgets sequences beyond the history', () => {
    const r = new AckTracker()
    for (let seq = 1; seq <= ACK_HISTORY + 100; seq++) expect(r.add(seq)).toBe(false)
    expect(r.frame(0)).toMatchObject({ largestAcked: ACK_HISTORY + 100, firstAckRangeLength: ACK_HISTORY, ranges: [] })
    expect(r.has(50)).toBe(false)
    expect(r.add(50)).toBe(true)
    expect(r.has(50)).toBe(false)
  })

  // go-udx TestRecvTracker_JumpClearsTheSlotsItPasses
  it('clears the slots a jump passes over', () => {
    const r = new AckTracker()
    for (let seq = 1; seq <= 10; seq++) r.add(seq)
    expect(r.add(10 + ACK_HISTORY)).toBe(true)
    expect(r.frame(0)).toMatchObject({ firstAckRangeLength: 1, ranges: [] })

    const s = new AckTracker()
    for (let seq = 1; seq <= 10; seq++) s.add(seq)
    s.add(14)
    expect(s.frame(0)).toMatchObject({ firstAckRangeLength: 1, ranges: [{ gap: 3, length: 10 }] })
  })
})
