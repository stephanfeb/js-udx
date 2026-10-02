import { type LinkConditions, ManualClock, MemoryNetwork, type UdxConnection, UdxMultiplexer, type UdxStream } from '../../src/index.js'

/** Lets queued microtasks (send flushes, drain events) and immediates run. */
export async function settle (): Promise<void> {
  await new Promise<void>(resolve => setImmediate(resolve))
}

/**
 * Runs virtual time, firing timers in order and settling microtasks between
 * them, until `done()` holds or `limit` ms of virtual time have passed.
 */
export async function runUntil (clock: ManualClock, done: () => boolean, limit = 120_000): Promise<void> {
  const end = clock.now() + limit
  await settle()
  while (!done()) {
    const next = clock.nextDeadline()
    if (next === undefined || next > end) {
      await settle()
      if (done() || clock.nextDeadline() === undefined || (clock.nextDeadline() as number) > end) break
      continue
    }
    clock.runUntil(next)
    await settle()
  }
}

/** Advances virtual time by `ms`, settling between timers. */
export async function advance (clock: ManualClock, ms: number): Promise<void> {
  const end = clock.now() + ms
  await runUntil(clock, () => false, ms)
  clock.runUntil(end)
  await settle()
}

export interface Pair {
  clock: ManualClock
  net: MemoryNetwork
  server: UdxMultiplexer
  client: UdxMultiplexer
  /** Dials client → server and returns both ends once the server accepts. */
  connect: () => Promise<{ dialer: UdxConnection, acceptor: UdxConnection }>
}

export function memoryPair (conditions: LinkConditions = { delay: 5 }, seed = 1): Pair {
  const clock = new ManualClock(1_000_000)
  const net = new MemoryNetwork(clock, conditions, seed)
  const server = new UdxMultiplexer(net.createSocket('10.0.0.1', 9000), { clock })
  const client = new UdxMultiplexer(net.createSocket('10.0.0.2', 9000), { clock })
  return {
    clock,
    net,
    server,
    client,
    connect: async () => {
      const accepted = server.accept()
      const dialer = client.dial(9000, '10.0.0.1')
      let acceptor: UdxConnection | undefined
      void accepted.then(c => { acceptor = c })
      await runUntil(clock, () => acceptor !== undefined, 60_000)
      if (acceptor === undefined) throw new Error('connection was never accepted')
      return { dialer, acceptor }
    }
  }
}

/** Collects everything a stream emits until it ends or closes. */
export function collect (stream: UdxStream): { chunks: Uint8Array[], bytes: () => Uint8Array, ended: () => boolean, closed: () => boolean, error: () => Error | undefined } {
  const chunks: Uint8Array[] = []
  let ended = false
  let closed = false
  let error: Error | undefined
  stream.on('data', c => chunks.push(c))
  stream.on('end', () => { ended = true })
  stream.on('close', e => { closed = true; error = e })
  return {
    chunks,
    bytes: () => concat(chunks),
    ended: () => ended,
    closed: () => closed,
    error: () => error
  }
}

export function concat (chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0))
  let o = 0
  for (const c of chunks) {
    out.set(c, o)
    o += c.length
  }
  return out
}

/** The go-udx interop payload pattern: byte(i*31 + i/251). */
export function pattern (n: number, seed = 0): Uint8Array {
  const out = new Uint8Array(n)
  for (let i = 0; i < n; i++) out[i] = ((i + seed) * 31 + Math.floor((i + seed) / 251)) & 0xff
  return out
}

/** Writes all of `data`, honouring backpressure, then optionally ends the stream. */
export async function writeAll (stream: UdxStream, data: Uint8Array, opts: { end?: boolean, chunk?: number } = {}): Promise<void> {
  const step = opts.chunk ?? 64 * 1024
  for (let o = 0; o < data.length; o += step) {
    if (!stream.write(data.subarray(o, o + step))) {
      await new Promise<void>((resolve, reject) => {
        const onClose = (e?: Error): void => {
          stream.off('drain', onDrain)
          reject(e ?? new Error('closed before drain'))
        }
        const onDrain = (): void => {
          stream.off('close', onClose)
          resolve()
        }
        stream.once('drain', onDrain)
        stream.once('close', onClose)
      })
    }
  }
  if (opts.end !== false) stream.end()
}

export function equalBytes (a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && Buffer.compare(Buffer.from(a.buffer, a.byteOffset, a.length), Buffer.from(b.buffer, b.byteOffset, b.length)) === 0
}
