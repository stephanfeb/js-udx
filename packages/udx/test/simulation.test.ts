// End-to-end reliability on virtual time: a sender (LossRecovery + Pacer) and a
// receiver (AckScheduler) joined by a simulated link with delay, reordering
// and loss. Deterministic, so the counts are exact: the JS equivalent of
// go-udx's loss_detection_test.go transfers, without sockets or wall time.
import { describe, expect, it } from 'vitest'
import { AckScheduler, FrameType, INITIAL_CWND, LossRecovery, ManualClock } from '../src/index.js'
import type { AckFrame } from '../src/index.js'

const PACKET_SIZE = 1418 // a full go-udx data datagram with 8-byte CIDs

interface LinkConfig {
  delay: number
  lossRate?: number
  reorderRate?: number
  reorderExtra?: number
  seed?: number
}

function mulberry32 (seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6D2B79F5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

interface Result {
  delivered: number
  retransmits: number
  reordered: number
  dropped: number
  elapsed: number
  maxCwnd: number
}

function transfer (packets: number, link: LinkConfig, timeLimit = 600_000): Result {
  const clock = new ManualClock()
  const rng = mulberry32(link.seed ?? 1)
  let reordered = 0
  let dropped = 0

  // A link direction: ordered lanes, as in the Go harness. Datagrams with the
  // same delay arrive in send order; displaced ones take the slow lane.
  const send = (deliver: () => void): void => {
    if (rng() < (link.lossRate ?? 0)) {
      dropped++
      return
    }
    let delay = link.delay
    if (rng() < (link.reorderRate ?? 0)) {
      reordered++
      delay += link.reorderExtra ?? 0
    }
    clock.setTimeout(deliver, delay)
  }

  // Receiver: records which packet ids (stream offsets) arrived.
  const received = new Set<number>()
  const receiver: AckScheduler = new AckScheduler({
    clock,
    smoothedRtt: () => 2 * link.delay,
    sendAck: (frame: AckFrame) => send(() => onAck(frame))
  })
  const onData = (seq: number, id: number): void => {
    received.add(id)
    receiver.onDataPacket(seq, false, { destinationStreamId: 1, sourceStreamId: 1 })
  }

  // Sender.
  let retransmits = 0
  let maxCwnd = 0
  const recovery = new LossRecovery({
    clock,
    retransmit: (pkt, seq) => {
      retransmits++
      const frame = pkt.frames[0]
      const id = frame?.type === FrameType.Stream ? frame.offset : -1
      send(() => onData(seq, id))
    }
  })
  let next = 0
  let pumpScheduled = false
  const pump = (): void => {
    pumpScheduled = false
    while (next < packets && recovery.congestion.canSend(PACKET_SIZE)) {
      const wait = recovery.congestion.pacer.timeUntilSend()
      if (wait > 0) {
        pumpScheduled = true
        clock.setTimeout(pump, wait)
        return
      }
      const id = next++
      const seq = recovery.nextSequence()
      recovery.onDataPacketSent({
        sequence: seq,
        size: PACKET_SIZE,
        frames: [{ type: FrameType.Stream, fin: false, syn: false, offset: id, data: new Uint8Array(0) }],
        destinationStreamId: 1,
        sourceStreamId: 1
      })
      recovery.congestion.pacer.onPacketSent(PACKET_SIZE)
      send(() => onData(seq, id))
    }
  }
  const onAck = (frame: AckFrame): void => {
    recovery.onAckFrame(frame)
    maxCwnd = Math.max(maxCwnd, recovery.congestion.cwnd)
    if (!pumpScheduled) pump()
  }

  pump()
  while (received.size < packets && clock.now() < timeLimit) {
    const at = clock.nextDeadline()
    if (at === undefined) break
    clock.runUntil(at)
  }
  recovery.destroy()
  receiver.destroy()
  return { delivered: received.size, retransmits, reordered, dropped, elapsed: clock.now(), maxCwnd }
}

const MEGABYTE = Math.ceil((1 << 20) / 1372)

const report = process.env.UDX_SIM_REPORT !== undefined
  ? (name: string, r: Result): void => { console.log(`${name}: ${JSON.stringify(r)}`) }
  : (): void => {}

describe('simulated transfer', () => {
  it('never retransmits on a clean path, and the window opens', () => {
    const r = transfer(MEGABYTE, { delay: 10 })
    report('clean 20ms', r)
    expect(r.delivered).toBe(MEGABYTE)
    expect(r.retransmits).toBe(0)
    expect(r.maxCwnd).toBeGreaterThan(INITIAL_CWND * 4)
  })

  it('never retransmits on a clean high-latency path', () => {
    const r = transfer(MEGABYTE, { delay: 150 })
    report('clean 300ms', r)
    expect(r.delivered).toBe(MEGABYTE)
    expect(r.retransmits).toBe(0)
  })

  // go-udx TestLossDetection_ReorderingCostIsBounded: same rates and ceilings.
  it.each([
    [0.02, 5],
    [0.05, 10],
    [0.20, 30]
  ])('bounds the cost of reordering %f of datagrams by 5 ms to %i%%', (rate, ceiling) => {
    const r = transfer(MEGABYTE, { delay: 10, reorderRate: rate, reorderExtra: 5, seed: 4242 })
    report(`reorder ${rate}`, r)
    expect(r.reordered).toBeGreaterThan(0)
    expect(r.delivered).toBe(MEGABYTE)
    expect(r.retransmits / MEGABYTE * 100).toBeLessThanOrEqual(ceiling)
  })

  it.each([0.01, 0.03, 0.1])('delivers everything at %f loss', (lossRate) => {
    const r = transfer(MEGABYTE, { delay: 25, lossRate, seed: 7 })
    report(`loss ${lossRate}`, r)
    expect(r.dropped).toBeGreaterThan(0)
    expect(r.delivered).toBe(MEGABYTE)
    // Most losses are recovered by SACK, not left for the RTO.
    expect(r.retransmits).toBeLessThan(r.dropped * 3)
  })

  it('delivers everything on a mobile-like path (50 ms, 2% loss, 25% reordering)', () => {
    const r = transfer(MEGABYTE, { delay: 50, lossRate: 0.02, reorderRate: 0.25, reorderExtra: 10, seed: 99 })
    report('mobile', r)
    expect(r.delivered).toBe(MEGABYTE)
  })

  it('delivers everything at 50% loss in both directions, leaning on the RTO timer', () => {
    const r = transfer(200, { delay: 20, lossRate: 0.5, seed: 3 })
    expect(r.delivered).toBe(200)
  })
})
