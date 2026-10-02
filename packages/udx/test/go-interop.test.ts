// Interop with go-udx over real UDP sockets, against tools/go-peer built from
// ../go-udx. Skipped when Go or go-udx isn't available.
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { UdxMultiplexer, dial, type UdxConnection } from '../src/index.js'
import { type GoPeer, buildGoPeer, goPeerUnavailable, startGoPeer } from './helpers/go-peer.js'
import { collect, equalBytes, pattern, writeAll } from './helpers/net.js'

const unavailable = goPeerUnavailable()

async function until (cond: () => boolean, timeoutMs = 60_000): Promise<void> {
  const end = Date.now() + timeoutMs
  while (!cond()) {
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

describe.skipIf(unavailable !== undefined)('interop with go-udx', () => {
  let binary: string
  beforeAll(() => { binary = buildGoPeer() }, 180_000)

  describe('JS dials a go-udx listener', () => {
    let peer: GoPeer
    let port: number
    let conn: UdxConnection
    beforeAll(async () => {
      peer = startGoPeer(binary, ['listen', '127.0.0.1:0'])
      port = Number(await peer.line('READY'))
      conn = await dial(port, '127.0.0.1')
    }, 30_000)
    afterAll(() => {
      conn?.close()
      peer?.kill()
    })

    async function echo (size: number, streams: number): Promise<void> {
      const runs = Array.from({ length: streams }, (_, i) => {
        const s = conn.openStream()
        const got = collect(s)
        const payload = pattern(size, i * 1000)
        void writeAll(s, payload)
        return { got, payload }
      })
      await until(() => runs.every(r => r.got.closed()))
      for (const r of runs) {
        expect(r.got.error()).toBeUndefined()
        expect(r.got.bytes().length).toBe(r.payload.length)
        expect(equalBytes(r.got.bytes(), r.payload)).toBe(true)
      }
    }

    it('establishes', async () => {
      await conn.established
    })

    it('echoes a small message', async () => { await echo(17, 1) }, 30_000)
    it('echoes 4 MiB on one stream', async () => { await echo(4 << 20, 1) }, 60_000)
    it('echoes 512 KiB on each of 8 concurrent streams', async () => { await echo(512 << 10, 8) }, 60_000)
    it('echoes an empty stream', async () => { await echo(0, 1) }, 30_000)

    // go-udx never acknowledges PINGs, so probes and pings go unanswered: the
    // datagram size stays at go-udx's own, and ping() reports false.
    it('keeps the go-udx datagram size and survives unanswered probes', async () => {
      await new Promise(resolve => setTimeout(resolve, 4000))
      expect(conn.datagramSize).toBe(1418)
      expect(await conn.ping(500)).toBe(false)
      await echo(100_000, 1)
    }, 30_000)
  })

  describe('go-udx dials a JS listener', () => {
    let mux: UdxMultiplexer
    const accepted: UdxConnection[] = []
    beforeAll(async () => {
      mux = await UdxMultiplexer.create({ host: '127.0.0.1' })
      mux.on('connection', conn => {
        accepted.push(conn)
        conn.on('stream', s => {
          s.on('data', d => {
            if (!s.write(d)) {
              s.pause()
              s.once('drain', () => s.resume())
            }
          })
          s.on('end', () => s.end())
        })
      })
    })
    afterAll(async () => { await mux?.close() })

    it.each([
      [1000, 1],
      [4 << 20, 1],
      [256 << 10, 8]
    ])('echoes %i bytes on each of %i streams', async (size, streams) => {
      const peer = startGoPeer(binary, ['dial', `127.0.0.1:${mux.address().port}`, String(size), String(streams)])
      try {
        const result = await peer.line('RESULT', 60_000)
        expect(Number(result)).toBe(size * streams)
        // go-udx answered our PATH_CHALLENGE, lifting the amplification limit.
        expect(accepted.at(-1)?.addressValidated).toBe(true)
      } finally {
        peer.kill()
      }
    }, 90_000)
  })
})
