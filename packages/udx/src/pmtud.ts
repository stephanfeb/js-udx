/**
 * Path MTU discovery as a binary search between the size known to work and
 * an upper bound (RFC 8899 DPLPMTUD, search phase only), as in go-udx and
 * dart-udx. Sizes are UDP payload sizes in bytes.
 *
 * The floor is the size the connection already sends, which go-udx and
 * dart-udx assume works. Discovery only ever raises it. Black-hole detection
 * (shrinking below the floor) is not implemented.
 */
export class PmtuSearch {
  /** The largest size known to work. */
  current: number
  private readonly max: number
  private searchHigh: number
  private probing: number | undefined

  constructor (floor: number, max: number) {
    this.current = floor
    this.max = max
    this.searchHigh = max
  }

  /** True once the search has converged. */
  get done (): boolean {
    return this.probing === undefined && this.nextSize() <= this.current
  }

  /** The size to probe next, or undefined if a probe is in flight or the search is over. */
  nextProbe (): number | undefined {
    if (this.probing !== undefined) return undefined
    const size = this.nextSize()
    if (size <= this.current) return undefined
    this.probing = size
    return size
  }

  private nextSize (): number {
    return Math.floor((this.current + this.searchHigh + 1) / 2)
  }

  onProbeAcked (size: number): void {
    if (this.probing !== size) return
    this.probing = undefined
    this.current = Math.max(this.current, size)
  }

  onProbeLost (size: number): void {
    if (this.probing !== size) return
    this.probing = undefined
    this.searchHigh = Math.min(this.searchHigh, size - 1)
  }

  get upperBound (): number {
    return this.max
  }
}
