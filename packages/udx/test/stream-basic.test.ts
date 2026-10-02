import { describe, expect, it } from 'vitest'
import { collect, memoryPair, runUntil } from './helpers/net.js'

describe('connection and stream basics (memory network)', () => {
  it('connects, opens a stream and echoes', async () => {
    const { clock, connect } = memoryPair()
    const { dialer, acceptor } = await connect()

    acceptor.on('stream', s => {
      s.on('data', d => s.write(d))
      s.on('end', () => s.end())
    })

    const s = dialer.openStream()
    const got = collect(s)
    s.write(new TextEncoder().encode('hello udx'))
    s.end()
    await runUntil(clock, () => got.closed())
    expect(new TextDecoder().decode(got.bytes())).toBe('hello udx')
    expect(got.ended()).toBe(true)
    expect(got.error()).toBeUndefined()
  })
})
