// Port of go-udx flow_control_window_test.go.
import { describe, expect, it } from 'vitest'
import { INITIAL_MAX_STREAM_DATA, MAX_STREAM_RECV_WINDOW, STREAM_CHUNK_SIZE, StreamFlowController, reconstructWindowOffset } from '../src/index.js'

const MOD = 2 ** 32
const wire = (limit: number): number => limit % MOD

describe('StreamFlowController', () => {
  it('advertises an absolute offset anchored to consumption', () => {
    const fc = new StreamFlowController(1000, 1000)
    expect(fc.advertisedLimit).toBe(1000)
    fc.onDataReceived(800)
    expect(fc.advertisedLimit).toBe(1000)
    expect(fc.bufferedBytes).toBe(800)
    fc.onDataConsumed(800)
    expect(fc.advertiseLimit()).toBe(2800) // consumed + doubled window
  })

  it('sustains a 64 MiB handshake without stalling', () => {
    const sender = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    const receiver = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    let delivered = 0
    while (delivered < 64 << 20) {
      expect(sender.canSend(1)).toBe(true)
      const n = Math.min(STREAM_CHUNK_SIZE, sender.sendWindowAvailable())
      sender.onDataSent(n)
      delivered += n
      receiver.onDataReceived(n)
      if (receiver.onDataConsumed(n)) sender.updateMaxStreamData(receiver.advertiseLimit())
    }
  })

  it('auto-tunes the window up to the cap', () => {
    const fc = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    let prev = fc.recvWindow
    for (let i = 0; i < 64; i++) {
      fc.onDataReceived(1 << 20)
      fc.onDataConsumed(1 << 20)
      fc.advertiseLimit()
      expect(fc.recvWindow).toBeGreaterThanOrEqual(prev)
      expect(fc.recvWindow).toBeLessThanOrEqual(MAX_STREAM_RECV_WINDOW)
      prev = fc.recvWindow
    }
    expect(prev).toBe(MAX_STREAM_RECV_WINDOW)
  })

  it('only ever raises the send limit', () => {
    const fc = new StreamFlowController(1000, 1000)
    expect(fc.updateMaxStreamData(5000)).toBe(true)
    expect(fc.updateMaxStreamData(2000)).toBe(false)
    expect(fc.sendLimit).toBe(5000)
    expect(fc.updateMaxStreamData(5000)).toBe(false)
  })

  it('applies backpressure until the application consumes', () => {
    const sender = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    const receiver = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    let sent = 0
    while (sender.canSend(1)) {
      const n = Math.min(1024, sender.sendWindowAvailable())
      sender.onDataSent(n)
      receiver.onDataReceived(n)
      sent += n
    }
    expect(sent).toBe(INITIAL_MAX_STREAM_DATA)
    const before = receiver.recvWindow
    expect(receiver.refreshLimit()).toBe(INITIAL_MAX_STREAM_DATA)
    expect(receiver.recvWindow).toBe(before) // a "blocked" peer can't inflate our buffer
    sender.updateMaxStreamData(receiver.refreshLimit())
    expect(sender.canSend(1)).toBe(false)
    receiver.onDataConsumed(INITIAL_MAX_STREAM_DATA)
    sender.updateMaxStreamData(receiver.advertiseLimit())
    expect(sender.canSend(1)).toBe(true)
  })

  it('survives the 32-bit wire offset wrapping', () => {
    const window = MAX_STREAM_RECV_WINDOW
    const sender = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    const nearWrap = MOD - window / 2
    sender.updateMaxStreamData(nearWrap)
    sender.onDataSent(nearWrap)
    expect(sender.canSend(1)).toBe(false)

    const receiver = new StreamFlowController(INITIAL_MAX_STREAM_DATA, window)
    receiver.onDataConsumed(nearWrap)
    const limit = receiver.advertiseLimit()
    expect(limit).toBeGreaterThan(MOD)
    expect(sender.applyWindowUpdate(wire(limit))).toBe(true)
    expect(sender.sendLimit).toBe(limit)
    expect(sender.canSend(1)).toBe(true)
  })

  it('sustains a transfer across the wrap', () => {
    const sender = new StreamFlowController(INITIAL_MAX_STREAM_DATA, INITIAL_MAX_STREAM_DATA)
    const receiver = new StreamFlowController(INITIAL_MAX_STREAM_DATA, MAX_STREAM_RECV_WINDOW)
    const runway = 8 << 20
    const jump = MOD - runway
    sender.updateMaxStreamData(jump)
    sender.onDataSent(jump)
    receiver.onDataReceived(jump)
    receiver.onDataConsumed(jump)
    sender.updateMaxStreamData(receiver.advertiseLimit())
    let delivered = jump
    while (delivered < jump + 2 * runway) {
      expect(sender.canSend(1)).toBe(true)
      const n = Math.min(STREAM_CHUNK_SIZE, sender.sendWindowAvailable())
      sender.onDataSent(n)
      delivered += n
      receiver.onDataReceived(n)
      if (receiver.onDataConsumed(n)) sender.applyWindowUpdate(wire(receiver.advertiseLimit()))
    }
  })
})

describe('reconstructWindowOffset', () => {
  it.each([
    ['origin', 65536, 0],
    ['mid range', 5 << 20, 4 << 20],
    ['just below wrap', MOD - 1024, MOD - (4 << 20)],
    ['exactly at wrap', MOD, MOD - 1024],
    ['just past wrap', MOD + 4096, MOD - 4096],
    ['far past wrap', 9 * MOD + 777, 9 * MOD],
    ['second wrap boundary', 2 * MOD, 2 * MOD - 512]
  ])('picks the nearest candidate: %s', (_name, full, reference) => {
    expect(reconstructWindowOffset(wire(full), reference)).toBe(full)
  })

  it('is exact across the wrap', () => {
    const start = MOD - 2048
    let reference = start
    for (let full = start; full < start + 4096; full++) {
      expect(reconstructWindowOffset(wire(full), reference)).toBe(full)
      if (full % 64 === 0) reference = full
    }
  })
})
