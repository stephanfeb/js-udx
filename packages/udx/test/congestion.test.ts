// Port of go-udx congestion_test.go.
import { describe, expect, it } from 'vitest'
import { BETA_CUBIC, CongestionController, INITIAL_CWND, MIN_CWND, ManualClock, PACING_GAIN, Pacer } from '../src/index.js'

function setup (): { cc: CongestionController, clock: ManualClock } {
  const clock = new ManualClock(10_000)
  return { cc: new CongestionController(clock, () => 0), clock }
}

/** Sends `bytes`, advances `rtt` ms, and acknowledges it as the frame's largest. */
function roundTrip (cc: CongestionController, clock: ManualClock, rtt: number, bytes = 1000, ackDelay = 0, largest = 1): void {
  const sent = clock.now()
  cc.onPacketSent(bytes)
  clock.advance(rtt)
  cc.onPacketsAcked(bytes, sent, ackDelay, largest)
}

describe('CongestionController', () => {
  it('starts at the initial window with nothing in flight', () => {
    const { cc } = setup()
    expect(cc.cwnd).toBe(INITIAL_CWND)
    expect(cc.canSend(1000)).toBe(true)
  })

  it('grows by every acknowledged byte in slow start', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 50)
    expect(cc.cwnd).toBe(INITIAL_CWND + 1000)
  })

  it('takes the first RTT sample whole, less the ACK delay', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 80, 1000, 5)
    expect(cc.smoothedRtt).toBe(75)
    expect(cc.rttVar).toBe(37.5)
  })

  it('smooths later samples', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 80, 1000, 0, 1)
    roundTrip(cc, clock, 100, 1000, 0, 2)
    expect(cc.smoothedRtt).toBe((7 * 80 + 100) / 8)
    expect(cc.rttVar).toBe((3 * 40 + 20) / 4)
  })

  it('caps the subtracted ACK delay at 25 ms', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 100, 1000, 50)
    expect(cc.smoothedRtt).toBe(75)
  })

  it('takes no RTT sample when the largest was not newly acknowledged', () => {
    const { cc, clock } = setup()
    cc.onPacketSent(1000)
    clock.advance(80)
    cc.onPacketsAcked(1000, undefined, 0, 1)
    expect(cc.smoothedRtt).toBe(333)
    expect(cc.inflight).toBe(0)
    expect(cc.cwnd).toBe(INITIAL_CWND)
  })

  it('tracks the minimum RTT', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 80, 1000, 0, 1)
    expect(cc.minRtt).toBe(80)
    roundTrip(cc, clock, 60, 1000, 0, 2)
    expect(cc.minRtt).toBe(60)
  })

  it('cuts the window to beta on loss and enters recovery', () => {
    const { cc, clock } = setup()
    for (let i = 0; i < 5; i++) roundTrip(cc, clock, 50, 1000, 0, i + 1)
    const before = cc.cwnd
    cc.onPacketLost(1000)
    expect(cc.inRecovery).toBe(true)
    expect(cc.cwnd).toBe(Math.trunc(before * BETA_CUBIC))
  })

  it('reduces only once per recovery epoch', () => {
    const { cc } = setup()
    cc.onCongestionEvent()
    const once = cc.cwnd
    cc.onCongestionEvent()
    expect(cc.cwnd).toBe(once)
  })

  it('never cuts below the minimum window', () => {
    const lastSent = { seq: 0 }
    const clock = new ManualClock()
    const cc = new CongestionController(clock, () => lastSent.seq)
    for (let i = 0; i < 10; i++) {
      cc.onCongestionEvent()
      lastSent.seq++
      cc.onPacketSent(100)
      cc.onPacketsAcked(100, clock.now(), 0, lastSent.seq) // leaves recovery
    }
    expect(cc.cwnd).toBeGreaterThanOrEqual(MIN_CWND)
  })

  it('does not grow during recovery, and leaves it once the ACK passes the last packet sent', () => {
    const lastSent = { seq: 5 }
    const clock = new ManualClock()
    const cc = new CongestionController(clock, () => lastSent.seq)
    cc.onPacketSent(1000)
    cc.onPacketLost(1000)
    const inRecovery = cc.cwnd
    roundTrip(cc, clock, 50, 1000, 0, 5)
    expect(cc.cwnd).toBe(inRecovery)
    expect(cc.inRecovery).toBe(true)
    roundTrip(cc, clock, 50, 1000, 0, 6)
    expect(cc.inRecovery).toBe(false)
    expect(cc.cwnd).toBeGreaterThan(inRecovery)
  })

  it('grows in congestion avoidance above ssthresh', () => {
    const { cc, clock } = setup()
    // Slow start until past ssthresh (65535).
    let largest = 0
    while (cc.cwnd < cc.ssthresh) roundTrip(cc, clock, 20, 1472, 0, ++largest)
    const start = cc.cwnd
    for (let i = 0; i < 50; i++) roundTrip(cc, clock, 20, 1472, 0, ++largest)
    expect(cc.cwnd).toBeGreaterThan(start)
    expect(cc.cwnd - start).toBeLessThan(50 * 1472) // slower than slow start
  })

  it('blocks sending once the window is full', () => {
    const { cc } = setup()
    cc.onPacketSent(cc.cwnd)
    expect(cc.canSend(1)).toBe(false)
    expect(cc.available()).toBe(0)
  })

  it('computes the RTO as SRTT + max(4·RTTVAR, 1) + max ACK delay, within [200, 5000] ms', () => {
    const { cc, clock } = setup()
    expect(cc.rto()).toBe(333 + 4 * 166 + 25)
    roundTrip(cc, clock, 1)
    expect(cc.rto()).toBe(200)
    const steady = setup()
    for (let i = 0; i < 200; i++) roundTrip(steady.cc, steady.clock, 400, 1000, 0, i + 1)
    expect(steady.cc.rttVar).toBeLessThan(1)
    expect(steady.cc.rto()).toBe(400 + 1 + 25) // never collapses onto the RTT
    const slow = setup()
    roundTrip(slow.cc, slow.clock, 3000)
    expect(slow.cc.rto()).toBe(5000)
  })
})

describe('Pacer', () => {
  it('lets the first packet go at once', () => {
    expect(new Pacer(new ManualClock()).timeUntilSend()).toBe(0)
  })

  it('paces at gain × cwnd / minRTT', () => {
    const p = new Pacer(new ManualClock())
    p.updateRate(14720, 100)
    expect(p.pacingRate).toBe(PACING_GAIN * 14720 / 0.1)
  })

  it('delays the next send after a packet, then allows it', () => {
    const clock = new ManualClock()
    const p = new Pacer(clock)
    p.updateRate(14720, 100)
    p.onPacketSent(1472)
    const delay = p.timeUntilSend()
    expect(delay).toBeGreaterThan(0)
    clock.advance(delay + 1)
    expect(p.timeUntilSend()).toBe(0)
  })

  it('is unpaced with a zero minRTT', () => {
    const p = new Pacer(new ManualClock())
    p.updateRate(14720, 0)
    p.onPacketSent(1472)
    expect(p.timeUntilSend()).toBe(0)
  })
})
