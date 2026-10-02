import type { Clock } from './clock.js'
import type { DatagramSocket, RemoteInfo } from './datagram.js'

export interface LinkConditions {
  /** One-way delay in ms. */
  delay?: number
  /** Probability a datagram is dropped. */
  loss?: number
  /** Probability a datagram is delayed by `reorderDelay` extra, landing behind later ones. */
  reorder?: number
  reorderDelay?: number
  /** Largest datagram (UDP payload) the path carries; larger ones are dropped. */
  mtu?: number
}

/** Seeded PRNG (mulberry32), so a lossy run is reproducible. */
export function seededRandom (seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6D2B79F5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

/**
 * An in-process datagram network on a Clock, for deterministic tests of the
 * full stack. Every datagram is copied, delayed, and possibly dropped or
 * reordered according to the network's conditions.
 */
export class MemoryNetwork {
  readonly clock: Clock
  conditions: LinkConditions
  private readonly random: () => number
  private readonly sockets = new Map<string, (data: Uint8Array, from: RemoteInfo) => void>()
  /** NAT bindings: socket key → the address its datagrams appear from, and back. */
  private readonly outside = new Map<string, RemoteInfo>()
  private readonly inside = new Map<string, string>()
  private nextPort = 40000
  /** Optional tap on every datagram sent, before loss is applied. */
  onSend: ((data: Uint8Array, from: RemoteInfo, to: RemoteInfo) => void) | undefined
  sent = 0
  dropped = 0

  constructor (clock: Clock, conditions: LinkConditions = {}, seed = 1) {
    this.clock = clock
    this.conditions = conditions
    this.random = seededRandom(seed)
  }

  createSocket (address = '10.0.0.1', port = this.nextPort++): DatagramSocket {
    const key = `${address}:${port}`
    if (this.sockets.has(key)) throw new Error(`address in use: ${key}`)
    let handler: ((data: Uint8Array, from: RemoteInfo) => void) | undefined
    let open = true
    const deliver = (data: Uint8Array, from: RemoteInfo): void => { if (open) handler?.(data, from) }
    this.sockets.set(key, deliver)
    const self: RemoteInfo = { address, port }
    return {
      send: (data, toPort, toAddress) => {
        if (!open) return
        const from = this.outside.get(key) ?? self
        const to = { address: toAddress, port: toPort }
        this.onSend?.(data, from, to)
        this.sent++
        const c = this.conditions
        if (this.random() < (c.loss ?? 0) || data.length > (c.mtu ?? Infinity)) {
          this.dropped++
          return
        }
        let delay = c.delay ?? 1
        if (this.random() < (c.reorder ?? 0)) delay += c.reorderDelay ?? 0
        const copy = Uint8Array.from(data)
        const target = this.inside.get(`${toAddress}:${toPort}`) ?? `${toAddress}:${toPort}`
        this.clock.setTimeout(() => this.sockets.get(target)?.(copy, from), delay)
      },
      onMessage: (h) => { handler = h },
      address: () => ({ address, port, family: 'IPv4' }),
      close: async () => {
        open = false
        if (this.sockets.get(key) === deliver) this.sockets.delete(key)
      }
    }
  }

  /**
   * Simulates a NAT rebinding: from now on the socket at `address:port`
   * sends from `as`, and datagrams to `as` reach it. Its old address no
   * longer reaches it.
   */
  rebind (address: string, port: number, as: RemoteInfo): void {
    const key = `${address}:${port}`
    const previous = this.outside.get(key)
    if (previous !== undefined) this.inside.delete(`${previous.address}:${previous.port}`)
    this.outside.set(key, as)
    this.inside.set(`${as.address}:${as.port}`, key)
    // The old address now leads nowhere.
    this.inside.set(key, `unbound:${key}`)
  }
}
