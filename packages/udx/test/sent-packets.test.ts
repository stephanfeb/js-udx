// Port of go-udx v0.3.0 packet_manager_test.go and the detector-level cases
// of loss_detection_test.go: loss declared from ACKs (RFC 9002 §6.1), never
// from a per-packet timer.
import { describe, expect, it } from 'vitest'
import {
  CongestionController,
  FrameType,
  LOSS_REORDER_THRESHOLD,
  LOSS_TIMER_GRANULARITY,
  ManualClock,
  type SentPacket,
  SentPacketManager
} from '../src/index.js'

function setup (rtt?: number): { pm: SentPacketManager, cc: CongestionController, clock: ManualClock, lost: SentPacket[][] } {
  const clock = new ManualClock(100_000)
  const cc = new CongestionController(clock, () => -1)
  if (rtt !== undefined) cc.onPacketsAcked(1200, clock.now() - rtt, 0, 1)
  const lost: SentPacket[][] = []
  const pm = new SentPacketManager({ clock, congestion: cc, onLost: (l) => lost.push(l) })
  return { pm, cc, clock, lost }
}

function send (pm: SentPacketManager, size = 100, sequence = pm.nextSequence(), probe = false): SentPacket {
  return pm.onPacketSent({
    sequence,
    size,
    frames: [{ type: FrameType.Stream, fin: false, syn: false, offset: 0, data: new Uint8Array(1) }],
    destinationStreamId: 2,
    sourceStreamId: 1,
    probe
  })
}

const ack = (largestAcked: number, firstAckRangeLength: number, ranges: Array<{ gap: number, length: number }> = []) =>
  ({ type: FrameType.Ack, largestAcked, ackDelay: 0, firstAckRangeLength, ranges }) as const

const seqs = (pkts: SentPacket[]): number[] => pkts.map(p => p.sequence)

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
    expect(pkt.acked).toBe(true)
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
  it('declares lost what is three or more behind the largest acknowledged', () => {
    const { pm, lost } = setup(50)
    for (let i = 0; i < 10; i++) send(pm)
    pm.onAckFrame(ack(9, 1))
    expect(seqs(pm.detectLost())).toEqual([0, 1, 2, 3, 4, 5, 6])
    expect(lost).toHaveLength(1)
    expect(pm.pendingCount).toBe(2) // 7 and 8 wait on the time threshold
  })

  it('judges every outstanding packet below the largest acknowledged, gaps reported or not', () => {
    const { pm } = setup(50)
    for (let i = 0; i < 6; i++) send(pm)
    // A receiver that acknowledges one packet at a time sends no ranges.
    pm.onAckFrame(ack(5, 1))
    expect(seqs(pm.detectLost())).toEqual([0, 1, 2])
  })

  it('honours the reorder threshold, then the loss timer catches what stays behind', () => {
    const { pm, clock, lost } = setup(80)
    for (let i = 0; i < 3; i++) send(pm)
    pm.onAckFrame(ack(2, 1))
    expect(pm.detectLost()).toEqual([]) // 0 and 1 are fewer than three behind
    expect(LOSS_REORDER_THRESHOLD).toBe(3)
    expect(pm.lossTimerArmed).toBe(true)
    clock.advance(pm.lossDelay() - 1)
    expect(lost).toHaveLength(0)
    clock.advance(1)
    expect(lost.map(seqs)).toEqual([[0, 1]])
    expect(pm.lossTimerArmed).toBe(false)
  })

  it('declares nothing before anything is acknowledged', () => {
    const { pm } = setup(50)
    for (let i = 0; i < 5; i++) send(pm)
    expect(pm.detectLost()).toEqual([])
    expect(pm.lossTimerArmed).toBe(false)
  })

  it('never judges packets above the largest acknowledged', () => {
    const { pm, clock } = setup(50)
    for (let i = 0; i < 5; i++) send(pm)
    pm.onAckFrame(ack(1, 1))
    clock.skip(10_000)
    expect(seqs(pm.detectLost())).toEqual([0])
    expect(pm.pendingCount).toBe(3)
  })

  it('bases the loss delay on the larger of smoothed and latest RTT', () => {
    const { pm, cc, clock } = setup(100)
    const sent = clock.now()
    cc.onPacketSent(100)
    clock.advance(400)
    cc.onPacketsAcked(100, sent, 0, 2)
    expect(cc.latestRtt).toBe(400)
    expect(pm.lossDelay()).toBe(400 * 9 / 8)
  })

  it('floors the loss delay', () => {
    const { pm } = setup(0)
    expect(pm.lossDelay()).toBe(LOSS_TIMER_GRANULARITY)
  })

  it('remembers the ACK before the one that revealed a loss', () => {
    const { pm, clock } = setup(50)
    for (let i = 0; i < 6; i++) send(pm)
    expect(pm.ackBeforeLoss).toBeUndefined()
    pm.onAckFrame(ack(1, 1))
    const first = clock.now()
    clock.advance(30)
    pm.onAckFrame(ack(5, 1))
    expect(pm.ackBeforeLoss).toBe(first)
  })
})

describe('SentPacketManager: re-sending', () => {
  it('moves a packet to a fresh sequence and restarts its send time, keeping the first', () => {
    const { pm, clock } = setup()
    const pkt = send(pm)
    const first = pkt.sentTime
    clock.advance(30)
    const fresh = pm.resend(pkt)
    expect(fresh).toBe(1)
    expect(pkt.sequence).toBe(1)
    expect(pkt.retransmitCount).toBe(1)
    expect(pkt.sentTime).toBe(first + 30)
    expect(pkt.firstSentTime).toBe(first)
    expect(pm.get(0)).toBeUndefined()
    expect(pm.get(1)).toBe(pkt)
    expect(pm.lastSentSequence).toBe(1)
  })

  it('tracks a lost packet again under its new number', () => {
    const { pm } = setup(50)
    for (let i = 0; i < 5; i++) send(pm)
    pm.onAckFrame(ack(4, 1))
    const [lost] = pm.detectLost()
    expect(pm.get(0)).toBeUndefined()
    expect(pm.resend(lost as SentPacket)).toBe(5)
    expect(pm.get(5)).toBe(lost)
  })

  it('refuses an acknowledged packet and a path-MTU probe', () => {
    const { pm } = setup()
    const pkt = send(pm)
    const probe = send(pm, 1472, pm.nextSequence(), true)
    pm.onAckFrame(ack(0, 1))
    expect(pm.resend(pkt)).toBeUndefined()
    expect(pm.resend(probe)).toBeUndefined()
  })

  it('hands the oldest outstanding packets to a PTO, probes left out', () => {
    const { pm } = setup()
    send(pm)
    send(pm, 1472, pm.nextSequence(), true)
    send(pm)
    send(pm)
    expect(seqs(pm.oldestUnacked(2))).toEqual([0, 2])
    const oldest = pm.oldestUnacked(1)[0] as SentPacket
    pm.resend(oldest)
    expect(seqs(pm.oldestUnacked(2))).toEqual([2, 3])
  })

  it('drops a packet that was re-chunked', () => {
    const { pm } = setup()
    const pkt = send(pm)
    pm.drop(pkt)
    expect(pm.pendingCount).toBe(0)
  })

  it('stops the loss timer on destroy', () => {
    const { pm, clock } = setup(50)
    for (let i = 0; i < 3; i++) send(pm)
    pm.onAckFrame(ack(2, 1))
    pm.detectLost()
    expect(pm.lossTimerArmed).toBe(true)
    pm.destroy()
    expect(clock.pending).toBe(0)
  })
})
