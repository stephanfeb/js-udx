// Interop with dart-udx over real UDP sockets, against tools/dart-peer compiled
// from ../dart-udx. Skipped when Dart or dart-udx isn't available.
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { UdxMultiplexer, dial, type UdxConnection } from '../src/index.js'
import { buildDartPeer, dartPeerUnavailable } from './helpers/dart-peer.js'
import { collect, equalBytes, pattern, writeAll } from './helpers/net.js'
import { type PeerProcess, startPeer } from './helpers/peer-process.js'

const unavailable = dartPeerUnavailable()

async function until (cond: () => boolean, timeoutMs = 60_000): Promise<void> {
  const end = Date.now() + timeoutMs
  while (!cond()) {
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

describe.skipIf(unavailable !== undefined)('interop with dart-udx', () => {
  let binary: string
  beforeAll(() => { binary = buildDartPeer() }, 300_000)

  describe('JS dials a dart-udx listener', () => {
    let peer: PeerProcess
    let port: number
    const conns: UdxConnection[] = []
    beforeAll(async () => {
      peer = startPeer(binary, ['listen', '127.0.0.1'])
      port = Number(await peer.line('READY'))
    }, 30_000)
    afterAll(() => {
      for (const c of conns) c.close()
      peer?.kill()
    })

    async function connect (): Promise<UdxConnection> {
      const conn = await dial(port, '127.0.0.1')
      conns.push(conn)
      return conn
    }

    async function echo (conn: UdxConnection, size: number, streams: number, timeoutMs = 60_000): Promise<void> {
      const runs = Array.from({ length: streams }, (_, i) => {
        const s = conn.openStream()
        const got = collect(s)
        const payload = pattern(size, i * 1000)
        void writeAll(s, payload)
        return { got, payload }
      })
      await until(() => runs.every(r => r.got.closed()), timeoutMs)
      for (const r of runs) {
        expect(r.got.error()).toBeUndefined()
        expect(r.got.bytes().length).toBe(r.payload.length)
        expect(equalBytes(r.got.bytes(), r.payload)).toBe(true)
      }
    }

    // One stream per connection, as the libp2p transport uses it (it upgrades
    // the dialer's first stream with Noise + Yamux).
    it('echoes a small message', async () => { await echo(await connect(), 17, 1) }, 30_000)
    it('echoes 4 MiB', async () => { await echo(await connect(), 4 << 20, 1) }, 60_000)
    it('echoes an empty stream', async () => { await echo(await connect(), 0, 1) }, 30_000)

    it('answers pings and lets PMTUD reach the IPv4 ceiling', async () => {
      const conn = await connect()
      expect(await conn.ping(2000)).toBe(true)
      await until(() => conn.datagramSize === 1472, 20_000)
    }, 30_000)

    // dartudx-4u8: dart-udx registers a stream a peer opens under the
    // destination id the peer used, and go-udx and js-udx always address a
    // stream they open to 0. Any later stream on the connection is routed to
    // Dart's stream 0 — even after that one finished, during its close delay —
    // where its bytes are acknowledged and dropped. These flip when it's fixed.
    it.fails('echoes on 2 concurrent streams of one connection (dartudx-4u8)', async () => {
      await echo(await connect(), 1000, 2, 5_000)
    }, 30_000)
    it.fails('echoes on 2 back-to-back streams of one connection (dartudx-4u8)', async () => {
      const conn = await connect()
      await echo(conn, 17, 1)
      await echo(conn, 17, 1, 5_000)
    }, 30_000)
  })

  describe('dart-udx dials a JS listener', () => {
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

    // Dart numbers the streams it opens (1, 3, 5, …), so several concurrent
    // ones are told apart here even though Dart can't do the same.
    it.each([
      [1000, 1],
      [4 << 20, 1],
      [256 << 10, 8]
    ])('echoes %i bytes on each of %i streams', async (size, streams) => {
      const peer = startPeer(binary, ['dial', `127.0.0.1:${mux.address().port}`, String(size), String(streams)])
      try {
        const result = await peer.line('RESULT', 90_000)
        expect(Number(result)).toBe(size * streams)
        // dart-udx answered our PATH_CHALLENGE, lifting the amplification limit.
        expect(accepted.at(-1)?.addressValidated).toBe(true)
      } finally {
        peer.kill()
      }
    }, 120_000)
  })
})
