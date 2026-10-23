// Sender-side wiring, as go-udx v0.3.0's Connection does it
// (congestion_wiring_test.go, loss_detection_test.go, pto_test.go,
// pmtud_test.go).
import { describe, expect, it } from 'vitest'
import { FrameType, INITIAL_CWND, LossRecovery, MIN_CWND, ManualClock, MtuDiscovery, SAFE_DATAGRAM_SIZE, type SentPacket } from '../src/index.js'

function setup (opts: { mtu?: MtuDiscovery } = {}): { r: LossRecovery, clock: ManualClock, probes: Array<[SentPacket, number]>, queued: () => number } {
  const clock = new ManualClock(5000)
  const probes: Array<[SentPacket, number]> = []
  let queued = 0
  const r = new LossRecovery({ clock, retransmit: (p, s) => probes.push([p, s]), onLostQueued: () => { queued++ }, mtu: opts.mtu })
  return { r, clock, probes, queued: () => queued }
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

/** Drains the lost queue as the connection's flush does, ignoring the window. */
function resendAll (r: LossRecovery): number[] {
  const out: number[] = []
  while (r.nextLost() !== undefined) {
    const seq = r.resendNextLost()
    if (seq !== undefined) out.push(seq)
  }
  return out
}

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

  it('samples RTT from the largest acknowledged packet', () => {
    const { r, clock } = setup()
    sendData(r)
    clock.skip(40)
    sendData(r)
    clock.skip(30)
    r.onAckFrame(ack(1, 2, [], 10))
    expect(r.congestion.smoothedRtt).toBe(30) // the first sample stands as measured
  })

  it('takes no RTT sample when the largest was already acknowledged', () => {
    const { r, clock } = setup()
    sendData(r)
    sendData(r)
    clock.skip(50)
    r.onAckFrame(ack(1, 1))
    const srtt = r.congestion.smoothedRtt
    clock.skip(10)
    r.onAckFrame(ack(1, 2)) // newly acks 0 only
    expect(r.congestion.smoothedRtt).toBe(srtt)
    expect(r.congestion.inflight).toBe(0)
  })

  it('takes lost data out of flight, contracts the window once, and queues it for re-sending under fresh sequences', () => {
    const { r, clock, queued } = setup()
    for (let i = 0; i < 10; i++) sendData(r)
    clock.skip(20)
    // 0..4 lost, 5..9 acknowledged: no ranges needed.
    r.onAckFrame(ack(9, 5))
    const cwnd = INITIAL_CWND + 5000
    expect(r.congestion.cwnd).toBe(Math.trunc(cwnd * 0.7))
    expect(r.congestion.inRecovery).toBe(true)
    expect(r.congestion.inflight).toBe(0)
    expect(r.lostCount).toBe(5)
    expect(queued()).toBe(1)

    expect(resendAll(r)).toEqual([10, 11, 12, 13, 14])
    expect(r.congestion.inflight).toBe(5000)
    r.onAckFrame(ack(14, 5))
    expect(r.congestion.inflight).toBe(0)
    expect(r.congestion.inRecovery).toBe(false)
  })

  it('skips a queued packet that was acknowledged meanwhile', () => {
    const { r, clock } = setup()
    for (let i = 0; i < 5; i++) sendData(r)
    clock.skip(20)
    r.onAckFrame(ack(4, 1))
    expect(r.lostCount).toBe(2) // 0 and 1; 2 and 3 wait on the time threshold
    ;(r.nextLost() as SentPacket).acked = true
    expect(resendAll(r)).toEqual([5])
  })

  it('re-sends the two oldest outstanding packets on a PTO, leaving the window and flight alone', () => {
    const { r, clock, probes } = setup()
    sendData(r)
    clock.skip(20)
    r.onAckFrame(ack(0, 1)) // an RTT sample: PTO 20 + 40 + 25 = 85 ms
    for (let i = 0; i < 4; i++) sendData(r)
    const cwnd = r.congestion.cwnd
    clock.advance(85)
    expect(probes.map(([p, s]) => [p.retransmitCount, s])).toEqual([[1, 5], [1, 6]])
    expect(r.congestion.cwnd).toBe(cwnd)
    expect(r.congestion.inflight).toBe(4000)
    clock.advance(170) // backed off
    expect(probes).toHaveLength(4)
  })

  it('collapses the window on persistent congestion: loss spanning three PTOs with no ACK since', () => {
    const { r, clock } = setup()
    sendData(r)
    clock.skip(20)
    r.onAckFrame(ack(0, 1)) // PTO 85 ms
    clock.skip(1)
    const first = sendData(r)
    clock.skip(300) // more than 3 × 85 ms
    sendData(r)
    sendData(r)
    sendData(r)
    clock.skip(50)
    const last = sendData(r)
    clock.skip(20)
    r.onAckFrame(ack(last, 1))
    expect(first).toBe(1)
    expect(r.congestion.cwnd).toBe(MIN_CWND)
  })

  it('does not call it persistent congestion when something was acknowledged during the span', () => {
    const { r, clock } = setup()
    sendData(r)
    clock.skip(20)
    r.onAckFrame(ack(0, 1))
    sendData(r) // 1
    clock.skip(150)
    sendData(r) // 2
    r.onAckFrame(ack(2, 1)) // acknowledged within the span
    clock.skip(150)
    sendData(r) // 3
    sendData(r) // 4
    sendData(r) // 5
    clock.skip(20)
    r.onAckFrame(ack(5, 1))
    expect(r.congestion.cwnd).toBeGreaterThan(MIN_CWND)
  })

  it('reports probes to path-MTU discovery, never in flight and never re-sent', () => {
    const mtu = new MtuDiscovery(new ManualClock())
    const { r, clock } = setup({ mtu })
    sendData(r)
    const size = mtu.nextProbe()
    const probe = r.nextSequence()
    r.onProbeSent({ sequence: probe, size, frames: [{ type: FrameType.MtuProbe, size: 1400 }], destinationStreamId: 0, sourceStreamId: 0 })
    expect(r.congestion.inflight).toBe(1000)
    clock.skip(20)
    r.onAckFrame(ack(probe, 2))
    expect(mtu.current).toBe(size)
    expect(r.congestion.inflight).toBe(0)
  })

  it('reads three full-size losses with nothing full-size acknowledged as a black hole', () => {
    const mtu = new MtuDiscovery(new ManualClock())
    mtu.nextProbe()
    mtu.onProbeAcked(1472)
    const { r, clock } = setup({ mtu })
    for (let i = 0; i < 6; i++) sendData(r, 1472)
    sendData(r, SAFE_DATAGRAM_SIZE) // only the small one gets through
    clock.skip(20)
    r.onAckFrame(ack(6, 1))
    expect(mtu.current).toBe(SAFE_DATAGRAM_SIZE)
  })

  it('reads a loss beside acknowledged full-size data as congestion, not a black hole', () => {
    const mtu = new MtuDiscovery(new ManualClock())
    mtu.nextProbe()
    mtu.onProbeAcked(1472)
    const { r, clock } = setup({ mtu })
    for (let i = 0; i < 7; i++) sendData(r, 1472)
    clock.skip(20)
    r.onAckFrame(ack(6, 1))
    expect(mtu.current).toBe(1472)
  })

  it('falls back to the safe size after three PTOs in a row', () => {
    const mtu = new MtuDiscovery(new ManualClock())
    mtu.nextProbe()
    mtu.onProbeAcked(1472)
    const { r, clock } = setup({ mtu })
    sendData(r, 1472)
    clock.advance(10_000) // PTOs at 1 s, 3 s and 7 s before an RTT sample
    expect(r.congestion.ptoCount).toBe(3)
    expect(mtu.current).toBe(SAFE_DATAGRAM_SIZE)
  })
})
