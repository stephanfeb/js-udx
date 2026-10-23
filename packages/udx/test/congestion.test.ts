// Port of go-udx congestion_test.go.
import { describe, expect, it } from 'vitest'
import {
  BETA_CUBIC,
  CongestionController,
  INITIAL_CWND,
  MAX_DATAGRAM_SIZE,
  MIN_CWND,
  MIN_PACING_DELAY,
  ManualClock,
  PACING_BURST_PACKETS,
  PACING_GAIN,
  PACING_GAIN_SLOW_START,
  Pacer
} from '../src/index.js'

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

  it('starts from the RFC 9002 initial estimates, with no min RTT', () => {
    const { cc } = setup()
    expect(cc.smoothedRtt).toBe(333)
    expect(cc.rttVar).toBe(166.5)
    expect(cc.minRtt).toBe(0)
    expect(cc.hasRttSample).toBe(false)
  })

  it('takes the first RTT sample as measured, ACK delay and all (RFC 9002 §5.3)', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 80, 1000, 5)
    expect(cc.smoothedRtt).toBe(80)
    expect(cc.rttVar).toBe(40)
    expect(cc.minRtt).toBe(80)
    expect(cc.hasRttSample).toBe(true)
  })

  it('smooths later samples', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 80, 1000, 0, 1)
    roundTrip(cc, clock, 100, 1000, 0, 2)
    expect(cc.smoothedRtt).toBe((7 * 80 + 100) / 8)
    expect(cc.rttVar).toBe((3 * 40 + 20) / 4)
  })

  it('subtracts a later sample\'s ACK delay, capped at 25 ms', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 60, 1000, 0, 1)
    roundTrip(cc, clock, 100, 1000, 50, 2)
    expect(cc.latestRtt).toBe(75)
    expect(cc.smoothedRtt).toBe((7 * 60 + 75) / 8)
  })

  it('ignores an ACK delay that would take the sample below min RTT', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 60, 1000, 0, 1)
    roundTrip(cc, clock, 70, 1000, 20, 2)
    expect(cc.latestRtt).toBe(70)
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

  it('stays in slow start until the first loss', () => {
    const { cc, clock } = setup()
    let largest = 0
    while (cc.cwnd < 1 << 20) roundTrip(cc, clock, 20, 1472, 0, ++largest)
    expect(cc.ssthresh).toBe(Number.MAX_SAFE_INTEGER)
  })

  it('grows in congestion avoidance above ssthresh', () => {
    const lastSent = { seq: 0 }
    const clock = new ManualClock(10_000)
    const cc = new CongestionController(clock, () => lastSent.seq)
    let largest = 0
    for (let i = 0; i < 20; i++) roundTrip(cc, clock, 20, 1472, 0, ++largest)
    cc.onCongestionEvent() // ends slow start
    lastSent.seq = largest
    roundTrip(cc, clock, 20, 1472, 0, ++largest) // leaves recovery
    expect(cc.cwnd).toBeGreaterThanOrEqual(cc.ssthresh)
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

  it('computes the PTO as SRTT + max(4·RTTVAR, 1) + max ACK delay, with no floor or cap', () => {
    const { cc, clock } = setup()
    expect(cc.pto()).toBe(333 + 4 * 166.5 + 25)
    roundTrip(cc, clock, 20)
    expect(cc.pto()).toBe(20 + 4 * 10 + 25) // 85 ms, not the 200 ms floor
    const steady = setup()
    for (let i = 0; i < 200; i++) roundTrip(steady.cc, steady.clock, 400, 1000, 0, i + 1)
    expect(steady.cc.rttVar).toBeLessThan(1)
    expect(steady.cc.pto()).toBeCloseTo(400 + 1 + 25, 0) // never collapses onto the RTT
    const slow = setup()
    roundTrip(slow.cc, slow.clock, 8000)
    expect(slow.cc.pto()).toBe(8000 + 4 * 4000 + 25) // not capped at 5 s
  })

  it('fires one PTO after the last send, backs off, and starts over on an ACK', () => {
    const { cc, clock } = setup()
    roundTrip(cc, clock, 20) // PTO 85 ms
    const probes: number[] = []
    cc.onProbe = () => probes.push(clock.now())
    const sent = clock.now()
    cc.onPacketSent(1000)
    cc.onPacketSent(1000)
    clock.advance(84)
    expect(probes).toEqual([])
    clock.advance(1)
    expect(probes).toEqual([sent + 85])
    expect(cc.ptoCount).toBe(1)
    clock.advance(170)
    expect(probes).toEqual([sent + 85, sent + 85 + 170])
    expect(cc.ptoCount).toBe(2)
    expect(cc.cwnd).toBeGreaterThan(INITIAL_CWND) // a PTO leaves the window alone

    cc.onPacketsAcked(1000, undefined, 0, 2)
    expect(cc.ptoCount).toBe(0)
    expect(cc.ptoArmed).toBe(true) // 1000 bytes still in flight
    cc.onPacketsAcked(1000, undefined, 0, 3)
    expect(cc.ptoArmed).toBe(false)
  })

  it('collapses the window to the minimum on persistent congestion and restarts slow start', () => {
    const { cc, clock } = setup()
    for (let i = 0; i < 5; i++) roundTrip(cc, clock, 20, 1000, 0, i + 1)
    cc.onPacketLost(1000)
    cc.onPersistentCongestion()
    expect(cc.cwnd).toBe(MIN_CWND)
    expect(cc.inRecovery).toBe(false)
    expect(cc.persistentCongestionDuration()).toBe(3 * cc.pto())
  })

  it('paces by the window and smoothed RTT once it has a sample, recomputed on every ACK', () => {
    const { cc, clock } = setup()
    expect(cc.pacer.pacingRate).toBe(Infinity)
    roundTrip(cc, clock, 100)
    expect(cc.pacer.pacingRate).toBeCloseTo(PACING_GAIN_SLOW_START * cc.cwnd / 0.1)
    roundTrip(cc, clock, 100, 1000, 0, 2)
    expect(cc.pacer.pacingRate).toBeCloseTo(PACING_GAIN_SLOW_START * cc.cwnd / (cc.smoothedRtt / 1000))
    cc.onCongestionEvent()
    expect(cc.pacer.pacingRate).toBeCloseTo(PACING_GAIN * cc.cwnd / (cc.smoothedRtt / 1000))
  })

  it('stops the PTO on destroy', () => {
    const { cc } = setup()
    cc.onPacketSent(1000)
    expect(cc.ptoArmed).toBe(true)
    cc.destroy()
    expect(cc.ptoArmed).toBe(false)
  })
})

describe('Pacer', () => {
  it('is unpaced until it has a rate', () => {
    const p = new Pacer(new ManualClock())
    p.onPacketSent(1472)
    expect(p.timeUntilSend()).toBe(0)
    expect(p.budget).toBe(Infinity)
  })

  it('paces at gain × cwnd / smoothed RTT, 2 in slow start and 1.25 after', () => {
    const p = new Pacer(new ManualClock())
    p.updateRate(14720, 100, true)
    expect(p.pacingRate).toBe(PACING_GAIN_SLOW_START * 14720 / 0.1)
    p.updateRate(14720, 100, false)
    expect(p.pacingRate).toBe(PACING_GAIN * 14720 / 0.1)
  })

  it('lets a burst of ten datagrams go at once, then waits for the budget', () => {
    const clock = new ManualClock()
    const p = new Pacer(clock)
    p.updateRate(14720, 100, false) // 184 KB/s: 2 ms of it is less than the burst
    for (let i = 0; i < PACING_BURST_PACKETS; i++) {
      expect(p.timeUntilSend()).toBe(0)
      p.onPacketSent(MAX_DATAGRAM_SIZE)
    }
    const wait = p.timeUntilSend()
    expect(wait).toBe(Math.ceil(MAX_DATAGRAM_SIZE / p.pacingRate * 1000))
    clock.skip(wait)
    expect(p.timeUntilSend()).toBe(0)
  })

  it('never asks for a wait shorter than the minimum pacing delay', () => {
    const clock = new ManualClock()
    const p = new Pacer(clock)
    p.updateRate(1 << 20, 10, false) // 131 MB/s: a datagram is worth microseconds, the burst ~220 of them
    for (let i = 0; i < 1000 && p.timeUntilSend() === 0; i++) p.onPacketSent(MAX_DATAGRAM_SIZE)
    expect(p.timeUntilSend()).toBe(MIN_PACING_DELAY)
  })

  it('is unpaced with a zero RTT', () => {
    const p = new Pacer(new ManualClock())
    p.updateRate(14720, 0, false)
    p.onPacketSent(1472)
    expect(p.timeUntilSend()).toBe(0)
  })
})
