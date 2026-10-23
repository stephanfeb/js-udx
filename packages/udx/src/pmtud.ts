import type { Clock } from './clock.js'
import {
  BLACKHOLE_PACKETS,
  BLACKHOLE_RETRY,
  MAX_UDP_PAYLOAD_IPV4,
  MTU_PROBE_TRIES,
  MTU_SEARCH_STEP,
  SAFE_DATAGRAM_SIZE
} from './constants.js'

/**
 * Finds how large a datagram the path carries, in the packetization layer
 * (DPLPMTUD, RFC 8899; RFC 9000 §14), and watches for the path shrinking
 * underneath a validated size. Port of go-udx v0.3.0's MTUDiscovery. Sizes
 * are UDP payload sizes in bytes.
 *
 * Streams send datagrams of the base size, SAFE_DATAGRAM_SIZE (1200 bytes),
 * which any path carries. The search probes up from there towards the
 * maximum with packets padded to the size under test: one is acknowledged,
 * that size is validated and streams use it; one is lost MTU_PROBE_TRIES
 * times, that size is ruled out. The first probe is the maximum, then each is
 * halfway between the largest validated and the smallest ruled out, and the
 * search ends when the two are within MTU_SEARCH_STEP. A probe is numbered
 * and tracked like data, so ACKs and loss detection report on it, but it is
 * never re-sent, not in flight, and its loss is no congestion signal (§14.4).
 *
 * A black hole is a path that stopped carrying the validated size: a
 * carrier's tunnel on the far side of a handover, where every full-size
 * packet vanishes and only small ones get through. BLACKHOLE_PACKETS
 * consecutive full-size data packets lost with none acknowledged between, or
 * BLACKHOLE_PTOS probe timeouts in a row (the connection's call), take the
 * size back to the base at once; re-sends are re-chunked to fit, and the
 * search starts over after BLACKHOLE_RETRY.
 *
 * js-udx used to send about 1472-byte datagrams from the start and search
 * only upwards from there, so a path that carried less (Rogers 5G, seen
 * 2026-10-22) dropped every full packet.
 */
export class MtuDiscovery {
  private readonly clock: Clock
  /** The size every path carries; streams start here. */
  readonly base: number
  /** The largest size the search probes. */
  readonly max: number

  private currentSize: number
  private probe = 0
  private lowFailed = 0
  private tries = 0
  private inFlight = false
  private finished = false
  private retryAt: number | undefined
  private largeLost = 0

  constructor (clock: Clock, max: number = MAX_UDP_PAYLOAD_IPV4, base: number = SAFE_DATAGRAM_SIZE) {
    this.clock = clock
    this.base = base
    this.max = Math.max(max, base)
    this.currentSize = base
    this.restart()
  }

  private restart (): void {
    this.probe = this.max
    this.lowFailed = this.max + 1
    this.tries = 0
    this.inFlight = false
    this.finished = this.max <= this.base
    this.retryAt = undefined
    this.largeLost = 0
  }

  /** The validated datagram size: what streams send now. */
  get current (): number {
    return this.currentSize
  }

  /** Whether the search has ended (also after a black hole, until the retry). */
  get done (): boolean {
    return this.finished
  }

  /**
   * The size to probe now, marked in flight, or 0 when nothing is to be sent:
   * the search is over, a probe is in flight, or the wait after a black hole
   * has not passed.
   */
  nextProbe (): number {
    if (this.retryAt !== undefined) {
      if (this.clock.now() < this.retryAt) return 0
      this.restart()
    }
    if (this.finished || this.inFlight) return 0
    this.inFlight = true
    return this.probe
  }

  /** Validates `size` and moves the search up. */
  onProbeAcked (size: number): void {
    this.inFlight = false
    if (this.retryAt !== undefined) return // stale: the path has shrunk since
    if (size > this.currentSize) this.currentSize = size
    this.tries = 0
    this.next()
  }

  /** Counts a loss of `size`; the MTU_PROBE_TRIES-th rules it out and moves the search down. */
  onProbeLost (size: number): void {
    this.inFlight = false
    if (this.retryAt !== undefined) return
    this.tries++
    if (this.tries < MTU_PROBE_TRIES) return // the same size again
    this.tries = 0
    if (size < this.lowFailed) this.lowFailed = size
    this.next()
  }

  private next (): void {
    if (this.lowFailed - this.currentSize <= MTU_SEARCH_STEP) {
      this.finished = true
      this.probe = 0
      return
    }
    this.probe = Math.floor((this.currentSize + this.lowFailed) / 2)
  }

  /** Told of every acknowledged data packet: a full-size one getting through clears the black-hole count. */
  onDataAcked (size: number): void {
    if (size > this.base) this.largeLost = 0
  }

  /**
   * Told of every lost data packet: BLACKHOLE_PACKETS full-size ones in a row,
   * none acknowledged between, mean the path no longer carries the size.
   */
  onDataLost (size: number): void {
    if (this.currentSize <= this.base || size <= this.base) return
    this.largeLost++
    if (this.largeLost >= BLACKHOLE_PACKETS) this.blackhole()
  }

  /** Takes the size back to the base at once; the search restarts after BLACKHOLE_RETRY. */
  blackhole (): void {
    if (this.currentSize <= this.base) return
    this.currentSize = this.base
    this.probe = 0
    this.finished = true
    this.inFlight = false
    this.largeLost = 0
    this.retryAt = this.clock.now() + BLACKHOLE_RETRY
  }

  /** Starts over from the base, for a connection that moved to a new path (RFC 9000 §9.4). */
  reset (): void {
    this.currentSize = this.base
    this.restart()
  }
}
