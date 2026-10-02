/** An opaque timer handle returned by `Clock.setTimeout`. */
export interface TimerHandle {
  readonly __timer: true
}

/**
 * Time and timers, in milliseconds. Everything time-dependent in the protocol
 * goes through a Clock so tests can run it on virtual time.
 */
export interface Clock {
  now(): number
  setTimeout(fn: () => void, ms: number): TimerHandle
  clearTimeout(handle: TimerHandle): void
}

/**
 * Monotonic real time. Timers are unref'd: an open socket keeps the process
 * alive, a pending retransmission alone should not.
 */
export const realClock: Clock = {
  now: () => performance.now(),
  setTimeout: (fn, ms) => {
    const t = setTimeout(fn, Math.max(0, ms))
    t.unref?.()
    return t as unknown as TimerHandle
  },
  clearTimeout: (handle) => {
    clearTimeout(handle as unknown as ReturnType<typeof setTimeout>)
  }
}

interface ManualTimer {
  handle: TimerHandle
  at: number
  order: number
  fn: () => void
}

/**
 * Virtual time for tests. Timers fire only from `advance`/`runUntil`, in
 * deadline order (ties in creation order), with `now()` set to each deadline
 * as it fires.
 */
export class ManualClock implements Clock {
  private time: number
  private readonly timers = new Map<TimerHandle, ManualTimer>()
  private order = 0

  constructor (start = 0) {
    this.time = start
  }

  now (): number {
    return this.time
  }

  setTimeout (fn: () => void, ms: number): TimerHandle {
    const handle = { __timer: true } as const
    this.timers.set(handle, { handle, at: this.time + Math.max(0, ms), order: this.order++, fn })
    return handle
  }

  clearTimeout (handle: TimerHandle): void {
    this.timers.delete(handle)
  }

  /** Number of armed timers. */
  get pending (): number {
    return this.timers.size
  }

  /** The earliest armed deadline, if any. */
  nextDeadline (): number | undefined {
    return this.next()?.at
  }

  /**
   * Moves time forward without firing anything, as if the event loop were
   * busy. Due timers fire on the next `advance`/`runUntil`.
   */
  skip (ms: number): void {
    this.time += ms
  }

  /** Moves time forward by `ms`, firing every timer that falls due on the way. */
  advance (ms: number): void {
    this.runUntil(this.time + ms)
  }

  /** Moves time to `t`, firing every timer due at or before it. */
  runUntil (t: number): void {
    for (;;) {
      const next = this.next()
      if (next === undefined || next.at > t) break
      this.timers.delete(next.handle)
      this.time = Math.max(this.time, next.at)
      next.fn()
    }
    this.time = Math.max(this.time, t)
  }

  private next (): ManualTimer | undefined {
    let best: ManualTimer | undefined
    for (const timer of this.timers.values()) {
      if (best === undefined || timer.at < best.at || (timer.at === best.at && timer.order < best.order)) best = timer
    }
    return best
  }
}
