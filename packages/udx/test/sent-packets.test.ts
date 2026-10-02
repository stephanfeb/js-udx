// Port of go-udx packet_manager_test.go, retransmit_recovery_test.go and the
// detector-level cases of loss_detection_test.go.
import { describe, expect, it } from 'vitest'
import {
  CongestionController,
  FrameType,
  LOSS_REORDER_THRESHOLD,
  LOSS_TIMER_GRANULARITY,
  MAX_RETRANSMIT_BACKOFF,
  ManualClock,
  type SentPacket,
  SentPacketManager
} from '../src/index.js'

function setup (rtt?: number): { pm: SentPacketManager, cc: CongestionController, clock: ManualClock, resent: Array<[SentPacket, number]> } {
  const clock = new ManualClock(100_000)
  const cc = new CongestionController(clock, () => -1)
  if (rtt !== undefined) cc.onPacketsAcked(1200, clock.now() - rtt, 0, 1)
  const resent: Array<[SentPacket, number]> = []
  const pm = new SentPacketManager({ clock, congestion: cc, onRetransmit: (p, s) => resent.push([p, s]) })
  return { pm, cc, clock, resent }
}

function send (pm: SentPacketManager, size = 100, sequence = pm.nextSequence()): SentPacket {
  return pm.onPacketSent({
    sequence,
    size,
    frames: [{ type: FrameType.Stream, fin: false, syn: false, offset: 0, data: new Uint8Array(1) }],
    destinationStreamId: 2,
    sourceStreamId: 1
  })
}

const ack = (largestAcked: number, firstAckRangeLength: number, ranges: Array<{ gap: number, length: number }> = []) =>
  ({ type: FrameType.Ack, largestAcked, ackDelay: 0, firstAckRangeLength, ranges }) as const

describe('SentPacketManager: tracking and ACKs', () => {
  it('numbers from 0 and records the last sent', () => {
    const { pm } = setup()
    expect(pm.lastSentSequence).toBe(-1)
    expect([pm.nextSequence(), pm.nextSequence(), pm.nextSequence()]).toEqual([0, 1, 2])
    send(pm, 100, 42)
    expect(pm.lastSentSequence).toBe(42)
  })

  it('wraps the sequence at 2^32', () => {
    const { pm } = setup()
    ;(pm as unknown as { nextSeq: number }).nextSeq = 0xFFFF_FFFF
    expect(pm.nextSequence()).toBe(0xFFFF_FFFF)
    expect(pm.nextSequence()).toBe(0)
  })

  it('returns acknowledged packets with their size and stops tracking them', () => {
    const { pm } = setup()
    const pkt = send(pm, 1000)
    expect(pm.pendingCount).toBe(1)
    const acked = pm.onAckFrame(ack(pkt.sequence, 1))
    expect(acked.map(p => [p.sequence, p.size])).toEqual([[0, 1000]])
    expect(pm.pendingCount).toBe(0)
    expect(pm.timerArmed).toBe(false)
  })

  it('acknowledges across SACK ranges', () => {
    const { pm } = setup()
    for (let i = 0; i < 10; i++) send(pm)
    // 9,8 then skip 7, then 6,5
    const acked = pm.onAckFrame(ack(9, 2, [{ gap: 1, length: 2 }]))
    expect(acked.map(p => p.sequence).sort()).toEqual([5, 6, 8, 9])
    expect(pm.pendingCount).toBe(6)
  })

  it('places every range after the first correctly (the off-by-one fixed in go-udx)', () => {
    const { pm } = setup()
    for (let i = 0; i < 20; i++) send(pm)
    // 19 | gap 18 | 17,16 | gap 15,14 | 13 | gap 12 | 11..9
    const acked = pm.onAckFrame(ack(19, 1, [{ gap: 1, length: 2 }, { gap: 2, length: 1 }, { gap: 1, length: 3 }]))
    expect(acked.map(p => p.sequence).sort((a, b) => a - b)).toEqual([9, 10, 11, 13, 16, 17, 19])
  })

  it('ignores acknowledgements for unknown sequences', () => {
    const { pm } = setup()
    send(pm)
    expect(pm.onAckFrame(ack(50, 3))).toEqual([])
    expect(pm.pendingCount).toBe(1)
  })

  it('forgets everything on destroy', () => {
    const { pm, clock } = setup()
    send(pm)
    send(pm)
    pm.destroy()
    expect(pm.pendingCount).toBe(0)
    expect(clock.pending).toBe(0)
  })
})

describe('SentPacketManager: loss detection', () => {
  it('declares gaps lost once far enough behind', () => {
    const { pm } = setup()
    for (let i = 0; i < 10; i++) send(pm)
    const lost = pm.detectLost(ack(9, 5, [{ gap: 2, length: 3 }]))
    expect(lost.map(p => p.sequence).sort()).toEqual([3, 4])
  })

  it('declares nothing without SACK ranges', () => {
    const { pm } = setup()
    send(pm)
    expect(pm.detectLost(ack(0, 1))).toEqual([])
  })

  it('honours the reorder threshold', () => {
    const { pm } = setup(50)
    for (let i = 0; i < 10; i++) send(pm)
    const lost = pm.detectLost(ack(9, 2, [{ gap: 4, length: 1 }]))
    expect(lost.length).toBeGreaterThan(0)
    for (const p of lost) expect(9 - p.sequence).toBeGreaterThanOrEqual(LOSS_REORDER_THRESHOLD)
    expect(lost.map(p => p.sequence)).not.toContain(7)
  })

  it('catches a packet too close to trip the threshold once it is old', () => {
    const { pm, clock } = setup(50)
    for (let i = 0; i < 4; i++) send(pm)
    expect(pm.detectLost(ack(3, 1, [{ gap: 1, length: 2 }]))).toEqual([])
    clock.advance(pm.lossDelay())
    expect(pm.detectLost(ack(3, 1, [{ gap: 1, length: 2 }])).map(p => p.sequence)).toEqual([2])
  })

  it('skips a packet retransmitted within the last RTO', () => {
    const { pm, clock } = setup()
    for (let i = 0; i < 5; i++) send(pm)
    const p2 = pm.get(2) as SentPacket
    p2.retransmitCount = 1
    p2.lastRetransmit = clock.now()
    const frame = ack(4, 2, [{ gap: 1, length: 2 }])
    expect(pm.detectLost(frame)).toEqual([])
    clock.skip(6000)
    expect(pm.detectLost(frame).map(p => p.sequence)).toEqual([2])
  })

  it('bases the loss delay on the larger of smoothed and latest RTT', () => {
    const { pm, cc, clock } = setup(20)
    const smoothed = cc.smoothedRtt
    cc.onPacketsAcked(1200, clock.now() - 400, 0, 2)
    expect(cc.latestRtt).toBeGreaterThan(smoothed)
    expect(pm.lossDelay()).toBe(cc.latestRtt * 9 / 8)
  })

  it('floors the loss delay', () => {
    const { pm, clock } = setup()
    const cc = (pm as unknown as { cc: CongestionController }).cc
    cc.onPacketsAcked(0, clock.now(), 0, 0) // a zero RTT sample
    expect(pm.lossDelay()).toBeGreaterThanOrEqual(LOSS_TIMER_GRANULARITY)
  })
})

describe('SentPacketManager: retransmission', () => {
  it('re-keys under a fresh sequence', () => {
    const { pm } = setup()
    const pkt = send(pm)
    const seq = pkt.sequence
    const fresh = pm.retransmit(pkt)
    expect(fresh).toBeDefined()
    expect(fresh).not.toBe(seq)
    expect(pkt.sequence).toBe(fresh)
    expect(pm.get(seq)).toBeUndefined()
    expect(pm.get(fresh as number)).toBe(pkt)
    expect(pkt.retransmitCount).toBe(1)
  })

  it('restarts the send time', () => {
    const { pm, clock } = setup()
    const pkt = send(pm)
    const original = pkt.sentTime
    clock.skip(pm.retransmitTimeout() / 2)
    pm.retransmit(pkt)
    expect(pkt.sentTime).toBeGreaterThan(original)
    expect(pkt.sentTime).toBe(clock.now())
  })

  it('collapses triggers within an RTO into one resend', () => {
    const { pm, clock } = setup()
    const pkt = send(pm)
    expect(pm.retransmit(pkt)).toBeDefined()
    expect(pm.retransmit(pkt)).toBeUndefined()
    clock.skip(pm.retransmitTimeout())
    expect(pm.retransmit(pkt)).toBeDefined()
  })

  it('does nothing for an acknowledged packet and leaves no timer behind', () => {
    const { pm, clock } = setup()
    const pkt = send(pm)
    pm.onAckFrame(ack(pkt.sequence, 1))
    expect(pm.retransmit(pkt)).toBeUndefined()
    expect(clock.pending).toBe(0)
  })

  it('backs off exponentially, saturating at the cap but never below the RTO', () => {
    const fast = setup().pm
    const rto = fast.retransmitTimeout()
    let prev = 0
    for (let attempt = 1; attempt <= 20; attempt++) {
      const b = fast.retransmitBackoff(attempt)
      expect(b).toBeLessThanOrEqual(MAX_RETRANSMIT_BACKOFF)
      expect(b).toBeGreaterThanOrEqual(rto)
      expect(b).toBeGreaterThanOrEqual(prev)
      prev = b
    }
    expect(prev).toBe(MAX_RETRANSMIT_BACKOFF)

    const slow = setup(3000).pm
    const slowRto = slow.retransmitTimeout()
    expect(slowRto).toBeGreaterThan(MAX_RETRANSMIT_BACKOFF)
    for (let attempt = 1; attempt <= 20; attempt++) expect(slow.retransmitBackoff(attempt)).toBeGreaterThanOrEqual(slowRto)
  })

  it('retransmits on the RTO timer with fresh sequences and growing backoff, without a retry cap', () => {
    const { pm, clock, resent } = setup(20) // RTO at the 200 ms floor
    const pkt = send(pm)
    const rto = pm.retransmitTimeout()
    clock.advance(rto - 1)
    expect(resent).toHaveLength(0)
    clock.advance(1)
    expect(resent.map(([, s]) => s)).toEqual([1])

    const times: number[] = []
    let last = clock.now()
    while (resent.length < 15) {
      const before = resent.length
      clock.runUntil(clock.nextDeadline() as number)
      if (resent.length > before) {
        times.push(clock.now() - last)
        last = clock.now()
      }
    }
    // Attempt k waits rto·2^(k−1): the timer armed by the first resend waits one RTO.
    expect(times.slice(0, 3)).toEqual([rto, rto * 2, rto * 4])
    expect(times.at(-1)).toBe(MAX_RETRANSMIT_BACKOFF)
    expect(new Set(resent.map(([, s]) => s)).size).toBe(resent.length)
    expect(pkt.retransmitCount).toBe(15)
    expect(pm.pendingCount).toBe(1)
  })

  it('stops the timer once the retransmission is acknowledged', () => {
    const { pm, clock, resent } = setup()
    send(pm)
    clock.advance(pm.retransmitTimeout())
    const [[pkt, seq]] = resent as [[SentPacket, number]]
    pm.onAckFrame(ack(seq, 1))
    expect(pkt.sequence).toBe(seq)
    expect(clock.pending).toBe(0)
    clock.advance(60_000)
    expect(resent).toHaveLength(1)
  })

  // go-udx stops a packet's timer when the timer collapses into a recent resend,
  // which happens when the RTO grows between a resend and its timer. The packet
  // then depends on SACK alone. Here the timer is re-armed for the new RTO.
  it('keeps timer coverage when the RTO grows between a resend and its timer', () => {
    const { pm, cc, clock, resent } = setup()
    const pkt = send(pm)
    clock.advance(pm.retransmitTimeout())
    expect(resent).toHaveLength(1)
    const resentAt = clock.now()
    const firstBackoff = pm.retransmitBackoff(1)

    cc.onPacketsAcked(0, clock.now() - 2000, 0, 99) // a slow RTT sample raises the RTO
    expect(pm.retransmitTimeout()).toBeGreaterThan(firstBackoff)

    clock.advance(firstBackoff) // the old deadline: collapsed, not resent
    expect(resent).toHaveLength(1)
    expect(clock.nextDeadline()).toBe(resentAt + pm.retransmitTimeout())
    clock.runUntil(resentAt + pm.retransmitTimeout())
    expect(resent).toHaveLength(2)
    expect(pkt.retransmitCount).toBe(2)
  })

  it('arms one timer however many packets are outstanding', () => {
    const { pm, clock } = setup()
    for (let i = 0; i < 100; i++) send(pm)
    expect(clock.pending).toBe(1)
  })
})
