// JS to JS over real UDP sockets on loopback.
import { networkInterfaces } from 'node:os'
import { afterEach, describe, expect, it } from 'vitest'
import { UdxMultiplexer, type UdxConnection, dial, listen } from '../src/index.js'
import { collect, equalBytes, pattern, writeAll } from './helpers/net.js'

async function until (cond: () => boolean, timeoutMs = 30_000): Promise<void> {
  const end = Date.now() + timeoutMs
  while (!cond()) {
    if (Date.now() > end) throw new Error('timed out')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

function echo (mux: UdxMultiplexer): void {
  mux.on('connection', conn => conn.on('stream', s => {
    s.on('data', d => {
      if (!s.write(d)) {
        s.pause()
        s.once('drain', () => s.resume())
      }
    })
    s.on('end', () => s.end())
  }))
}

const hasIpv6Loopback = Object.values(networkInterfaces()).flat().some(i => i?.address === '::1')

describe('over UDP loopback', () => {
  const cleanup: Array<() => Promise<void> | void> = []
  afterEach(async () => {
    for (const c of cleanup.splice(0)) await c()
  })

  it('binds port 0 and reports the real port', async () => {
    const mux = await listen({ host: '127.0.0.1' })
    cleanup.push(async () => await mux.close())
    expect(mux.address().port).toBeGreaterThan(0)
    expect(mux.address().family).toBe('IPv4')
  })

  it('echoes 4 MiB', async () => {
    const mux = await listen({ host: '127.0.0.1' })
    echo(mux)
    const conn = await dial(mux.address().port, '127.0.0.1')
    cleanup.push(async () => { conn.close(); await mux.close() })
    const s = conn.openStream()
    const got = collect(s)
    void writeAll(s, pattern(4 << 20))
    await until(() => got.closed())
    expect(equalBytes(got.bytes(), pattern(4 << 20))).toBe(true)
  })

  it.skipIf(!hasIpv6Loopback)('works over IPv6', async () => {
    const mux = await listen({ host: '::1' })
    echo(mux)
    expect(mux.address().family).toBe('IPv6')
    const conn = await dial(mux.address().port, '::1')
    cleanup.push(async () => { conn.close(); await mux.close() })
    const s = conn.openStream()
    const got = collect(s)
    void writeAll(s, pattern(100_000))
    await until(() => got.closed())
    expect(equalBytes(got.bytes(), pattern(100_000))).toBe(true)
  })

  it('routes many connections from one socket by connection ID', async () => {
    const server = await listen({ host: '127.0.0.1' })
    echo(server)
    const client = await UdxMultiplexer.create({ host: '127.0.0.1' })
    cleanup.push(async () => { await client.close(); await server.close() })
    const conns: UdxConnection[] = Array.from({ length: 5 }, () => client.dial(server.address().port, '127.0.0.1'))
    const runs = conns.map((c, i) => {
      const s = c.openStream()
      const got = collect(s)
      void writeAll(s, pattern(50_000, i * 1000))
      return { got, i }
    })
    await until(() => runs.every(r => r.got.closed()))
    for (const r of runs) expect(equalBytes(r.got.bytes(), pattern(50_000, r.i * 1000))).toBe(true)
    expect(server.connectionCount).toBe(5)
  })

  it('closes the dial socket with the connection', async () => {
    const mux = await listen({ host: '127.0.0.1' })
    cleanup.push(async () => await mux.close())
    const conn = await dial(mux.address().port, '127.0.0.1')
    await conn.established
    conn.close()
    await until(() => mux.connectionCount === 0)
  })
})
