// Sender-side wiring, as go-udx's Connection does it (congestion_wiring_test.go,
// TestAck_WindowGrowsByEveryByteAcked).
import { describe, expect, it } from 'vitest'
import { FrameType, INITIAL_CWND, LossRecovery, ManualClock, type SentPacket } from '../src/index.js'

function setup (): { r: LossRecovery, clock: ManualClock, resent: Array<[SentPacket, number]> } {
  const clock = new ManualClock(5000)
  const resent: Array<[SentPacket, number]> = []
  const r = new LossRecovery({ clock, retransmit: (p, s) => resent.push([p, s]) })
  return { r, clock, resent }
}

function sendData (r: LossRecovery, size = 1000): number {
  const sequence = r.nextSequence()
  r.onDataPacketSent({
    sequence,
    size,
    frames: [{ type: FrameType.Stream, fin: false, syn: false, offset: 0, data: new Uint8Array(1) }],
    destinationStreamId: 2,
    sourceStreamId: 1
  })
  return sequence
}

const ack = (largestAcked: number, firstAckRangeLength: number, ranges: Array<{ gap: number, length: number }> = [], ackDelay = 0) =>
  ({ type: FrameType.Ack, largestAcked, ackDelay, firstAckRangeLength, ranges }) as const

describe('LossRecovery', () => {
  it('charges sent data to the window and drains it on ACK', () => {
    const { r } = setup()
    sendData(r)
    sendData(r)
    expect(r.congestion.inflight).toBe(2000)
    expect(r.onAckFrame(ack(1, 2))).toBe(2000)
    expect(r.congestion.inflight).toBe(0)
  })

  it('grows the window by every byte an ACK covers, not the largest alone', () => {
    const { r } = setup()
    sendData(r, 1000)
    sendData(r, 1000)
    r.onAckFrame(ack(1, 2))
    expect(r.congestion.cwnd).toBe(INITIAL_CWND + 2000)
  })

  it('samples RTT from the largest acknowledged packet, less the ACK delay', () => {
    const { r, clock } = setup()
    sendData(r)
    clock.advance(40)
    sendData(r)
    clock.advance(30)
    r.onAckFrame(ack(1, 2, [], 10))
    expect(r.congestion.smoothedRtt).toBe(20)
  })

  it('takes no RTT sample when the largest was already acknowledged', () => {
    const { r, clock } = setup()
    sendData(r)
    sendData(r)
    clock.advance(50)
    r.onAckFrame(ack(1, 1))
    const srtt = r.congestion.smoothedRtt
    clock.advance(100) // less than the RTO: packet 0 is still under sequence 0
    r.onAckFrame(ack(1, 2)) // newly acks 0 only
    expect(r.congestion.smoothedRtt).toBe(srtt)
    expect(r.congestion.inflight).toBe(0)
  })

  it('retransmits a SACK-detected loss once, contracts the window once, and keeps the bytes in flight', () => {
    const { r, resent } = setup()
    for (let i = 0; i < 10; i++) sendData(r)
    // 0..4 lost, 5..9 acked.
    r.onAckFrame(ack(9, 5, []))
    expect(resent).toHaveLength(0) // no ranges, nothing below can be judged
    const cwnd = r.congestion.cwnd // grown in slow start by the 5000 bytes acked
    r.onAckFrame(ack(9, 5, [{ gap: 5, length: 0 }]))
    expect(resent.map(([p]) => p.retransmitCount)).toEqual([1, 1, 1, 1, 1])
    expect(resent.map(([, s]) => s)).toEqual([10, 11, 12, 13, 14])
    expect(r.congestion.cwnd).toBe(Math.trunc(cwnd * 0.7))
    expect(r.congestion.inRecovery).toBe(true)
    expect(r.congestion.inflight).toBe(5000)

    // The same ACK again resends nothing: the packets moved to new sequences.
    r.onAckFrame(ack(9, 5, [{ gap: 5, length: 0 }]))
    expect(resent).toHaveLength(5)

    r.onAckFrame(ack(14, 5))
    expect(r.congestion.inflight).toBe(0)
    expect(r.congestion.inRecovery).toBe(false)
  })

  it('leaves the window alone on a timer-driven retransmission', () => {
    const { r, clock, resent } = setup()
    sendData(r)
    const cwnd = r.congestion.cwnd
    clock.advance(r.sentPackets.retransmitTimeout())
    expect(resent).toHaveLength(1)
    expect(r.congestion.cwnd).toBe(cwnd)
    expect(r.congestion.inflight).toBe(1000)
  })
})
