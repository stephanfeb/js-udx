// Port of go-udx ack_policy_test.go.
import { describe, expect, it } from 'vitest'
import { type AckFrame, type AckRoute, AckScheduler, ManualClock } from '../src/index.js'

const route: AckRoute = { destinationStreamId: 1, sourceStreamId: 1 }

function setup (srtt = 100): { s: AckScheduler, clock: ManualClock, acks: AckFrame[] } {
  const clock = new ManualClock(1000)
  const acks: AckFrame[] = []
  const s = new AckScheduler({ clock, smoothedRtt: () => srtt, sendAck: f => acks.push(f) })
  return { s, clock, acks }
}

describe('AckScheduler', () => {
  it('acknowledges every second in-order packet and leaves a lone one to the timer', () => {
    const { s, acks } = setup()
    for (let seq = 1; seq <= 4; seq++) s.onDataPacket(seq, false, route)
    expect(acks).toHaveLength(2)
    expect(acks[1]).toMatchObject({ largestAcked: 4, firstAckRangeLength: 4 })

    s.onDataPacket(5, false, route)
    expect(acks).toHaveLength(2)
    expect(s.pending).toBe(1)
    expect(s.timerArmed).toBe(true)
  })

  it('acknowledges a gap, and the packet filling it, at once', () => {
    const { s, acks } = setup()
    s.onDataPacket(1, false, route)
    s.onDataPacket(2, false, route)
    s.onDataPacket(4, false, route)
    expect(acks).toHaveLength(2)
    expect(acks[1]).toMatchObject({ largestAcked: 4, firstAckRangeLength: 1, ranges: [{ gap: 1, length: 2 }] })
    s.onDataPacket(3, false, route)
    expect(acks).toHaveLength(3)
    expect(acks[2]).toMatchObject({ largestAcked: 4, firstAckRangeLength: 4, ranges: [] })
  })

  it('acknowledges SYN and FIN at once', () => {
    const { s, acks } = setup()
    s.onDataPacket(1, true, route)
    expect(acks).toHaveLength(1)
    s.onDataPacket(2, true, route)
    expect(acks).toHaveLength(2)
  })

  it('flushes on the timer and reports how long the largest packet waited', () => {
    const { s, clock, acks } = setup(28) // timer = 28/4 = 7ms
    s.onDataPacket(1, false, route)
    expect(acks).toHaveLength(0)
    clock.advance(6)
    expect(acks).toHaveLength(0)
    clock.advance(1)
    expect(acks).toEqual([expect.objectContaining({ largestAcked: 1, ackDelay: 7 })])
    expect(s.pending).toBe(0)
    expect(s.timerArmed).toBe(false)
  })

  it('clamps the timer to [1, 25] ms', () => {
    for (const [srtt, wait] of [[0, 1], [1000, 25]] as const) {
      const { s, clock, acks } = setup(srtt)
      s.onDataPacket(1, false, route)
      clock.advance(wait - 0.5)
      expect(acks).toHaveLength(0)
      clock.advance(0.5)
      expect(acks).toHaveLength(1)
    }
  })

  it('reports no delay on an ACK sent at arrival', () => {
    const { s, clock, acks } = setup()
    s.onDataPacket(1, false, route)
    clock.advance(3)
    s.onDataPacket(2, false, route)
    expect(acks).toEqual([expect.objectContaining({ ackDelay: 0 })])
  })

  it('disarms the timer when an immediate ACK covers the pending packet', () => {
    const { s, clock, acks } = setup()
    s.onDataPacket(1, false, route)
    expect(s.timerArmed).toBe(true)
    s.onDataPacket(3, false, route)
    expect(s.timerArmed).toBe(false)
    clock.advance(100)
    expect(acks).toHaveLength(1)
  })
})
