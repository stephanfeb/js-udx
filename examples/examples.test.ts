// Runs every program in examples/ as a user would (`node examples/<name>.ts`,
// against the built packages) and checks what it prints, and checks that the
// docs embed the current version of each.
import { execFileSync, spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { beforeAll, describe, expect, it } from 'vitest'
// @ts-expect-error: a plain-JS tool without type declarations
import { embedAll } from '../tools/embed-examples.mjs'

const ROOT = fileURLToPath(new URL('..', import.meta.url))

/** Node runs .ts files directly from 22.18 (type stripping). */
const typescript = (process.features as { typescript?: string | false }).typescript
const canRunTs = typescript !== undefined && typescript !== false

async function run (example: string): Promise<string> {
  const child = spawn(process.execPath, [`examples/${example}`], { cwd: ROOT })
  let out = ''
  let err = ''
  child.stdout.on('data', (b: Buffer) => { out += b.toString() })
  child.stderr.on('data', (b: Buffer) => { err += b.toString() })
  const code = await new Promise<number | null>(resolve => child.on('exit', resolve))
  if (code !== 0) throw new Error(`${example} exited with ${code}:\n${err}`)
  expect(err).toBe('')
  return out
}

describe('docs', () => {
  it('embed the current examples', () => {
    expect(embedAll({ check: true })).toEqual([])
  })
})

describe.runIf(canRunTs)('examples', () => {
  beforeAll(() => {
    execFileSync(process.execPath, ['node_modules/typescript/bin/tsc', '-b', 'packages/udx', 'packages/libp2p-udx'], { cwd: ROOT })
  }, 120_000)

  it('echo.ts', async () => {
    expect(await run('echo.ts')).toBe('hello, udx\n')
  }, 30_000)

  it('backpressure.ts', async () => {
    expect(await run('backpressure.ts')).toMatch(/^received 16777216 of 16777216 bytes\ntook \d+ ms\n$/)
  }, 60_000)

  it('reset.ts', async () => {
    expect(await run('reset.ts')).toBe('stream reset by peer, code 0x42\nconnection closed by peer: code 0, reason ""\n')
  }, 30_000)

  it('one-socket.ts', async () => {
    const out = await run('one-socket.ts')
    expect(out).toMatch(/^bob counted 1000, 100000, 1000000\nping acknowledged: true\n/)
    expect(out).toMatch(/datagrams up to 1472 bytes\n$/)
  }, 30_000)

  it('libp2p-echo.ts', async () => {
    expect(await run('libp2p-echo.ts')).toMatch(/^server listening on \/ip4\/127\.0\.0\.1\/udp\/\d+\/udx\nping: ok\necho: hello over libp2p\n$/)
  }, 60_000)
})
