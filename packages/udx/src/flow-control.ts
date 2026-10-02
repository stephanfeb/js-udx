import { MAX_STREAM_RECV_WINDOW } from './constants.js'

const TWO_POW_32 = 0x1_0000_0000

/**
 * Reconstructs a 64-bit stream offset from its value modulo 2^32: the
 * candidate nearest `reference`. WINDOW_UPDATE carries only 32 bits; a
 * receiver never advertises more than 2^31 beyond the limit the sender
 * already holds, so the nearest candidate is unambiguous.
 */
export function reconstructWindowOffset (wire: number, reference: number): number {
  const base = Math.floor(reference / TWO_POW_32) * TWO_POW_32
  let best = base + wire
  for (const cand of [best - TWO_POW_32, best + TWO_POW_32]) {
    if (Math.abs(cand - reference) < Math.abs(best - reference)) best = cand
  }
  return best
}

/**
 * Per-stream flow control (go-udx StreamFlowController). Limits are absolute
 * byte offsets, not window sizes.
 *
 * Send side: we may send while `dataSent + n <= maxStreamData`, the offset the
 * peer last advertised.
 *
 * Receive side: we advertise `dataConsumed + recvWindow`, anchored to what the
 * application has consumed rather than received, so a slow reader holds the
 * sender back. When less than half a window of credit remains, the window
 * doubles (up to MAX_STREAM_RECV_WINDOW) and a new limit is advertised. The
 * advertised limit only ever increases.
 */
export class StreamFlowController {
  private maxStreamData: number
  private dataSent = 0
  private dataRecvd = 0
  private dataConsumed = 0
  private window: number
  private lastAdvertised: number

  constructor (initialMaxStreamData: number, recvWindow: number) {
    this.maxStreamData = initialMaxStreamData
    this.window = recvWindow
    this.lastAdvertised = recvWindow
  }

  canSend (n: number): boolean {
    return this.dataSent + n <= this.maxStreamData
  }

  /** Bytes the peer's limit still admits. */
  sendWindowAvailable (): number {
    return Math.max(0, this.maxStreamData - this.dataSent)
  }

  /** The absolute offset the peer has allowed us to send up to. */
  get sendLimit (): number {
    return this.maxStreamData
  }

  onDataSent (n: number): void {
    this.dataSent += n
  }

  /** Applies a WINDOW_UPDATE's 32-bit wire value. Returns true if the limit grew. */
  applyWindowUpdate (wire: number): boolean {
    return this.updateMaxStreamData(reconstructWindowOffset(wire, this.maxStreamData))
  }

  /** Raises the send limit; a stale (lower) limit is ignored. */
  updateMaxStreamData (max: number): boolean {
    if (max <= this.maxStreamData) return false
    this.maxStreamData = max
    return true
  }

  onDataReceived (n: number): void {
    this.dataRecvd += n
  }

  /** Records bytes handed to the application. Returns true when a WINDOW_UPDATE is due. */
  onDataConsumed (n: number): boolean {
    this.dataConsumed += n
    return this.lastAdvertised - this.dataConsumed < this.window / 2
  }

  /** Grows the window (doubling, capped) and returns the limit to advertise. */
  advertiseLimit (): number {
    if (this.window < MAX_STREAM_RECV_WINDOW) this.window = Math.min(MAX_STREAM_RECV_WINDOW, this.window * 2)
    return this.advertise()
  }

  /** The current limit, re-advertised without growing the window (reply to STREAM_DATA_BLOCKED). */
  refreshLimit (): number {
    return this.advertise()
  }

  private advertise (): number {
    const limit = this.dataConsumed + this.window
    if (limit > this.lastAdvertised) this.lastAdvertised = limit
    return this.lastAdvertised
  }

  get advertisedLimit (): number {
    return this.lastAdvertised
  }

  get recvWindow (): number {
    return this.window
  }

  /** Received but not yet consumed (including out-of-order bytes). */
  get bufferedBytes (): number {
    return this.dataRecvd - this.dataConsumed
  }
}

/**
 * Connection-level flow control (go-udx FlowController). go-udx tracks MAX_DATA
 * but does not enforce it, and nor does this yet. It answers DATA_BLOCKED with
 * the current MAX_DATA, as go-udx does.
 */
export class ConnectionFlowController {
  private connMaxData: number

  constructor (initialMaxData: number) {
    this.connMaxData = initialMaxData
  }

  get maxData (): number {
    return this.connMaxData
  }

  updateMaxData (maxData: number): void {
    if (maxData > this.connMaxData) this.connMaxData = maxData
  }
}
