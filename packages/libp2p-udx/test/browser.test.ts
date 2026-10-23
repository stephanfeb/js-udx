import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'
import { describe, expect, it } from 'vitest'

// `@stephanfeb/libp2p-udx/multiaddr` is the browser entry: web apps running
// js-libp2p import it so relays that advertise /udx addresses (go-ricochet,
// dart-libp2p) don't fail their reservations with "Protocol 768 was unknown".
// It must bundle for the browser, unlike the package root, which pulls in the
// transport and node:dgram.

const here = (path: string): string => fileURLToPath(new URL(path, import.meta.url))

async function bundleForBrowser (entry: string): Promise<string> {
  const result = await build({
    entryPoints: [entry],
    bundle: true,
    platform: 'browser',
    format: 'esm',
    write: false,
    logLevel: 'silent'
  })
  const [output] = result.outputFiles
  if (output === undefined) throw new Error(`esbuild wrote nothing for ${entry}`)
  return output.text
}

describe('browser entry', () => {
  it('is exported as ./multiaddr, built from src/multiaddr.ts', () => {
    const pkg = JSON.parse(readFileSync(here('../package.json'), 'utf8'))
    expect(pkg.exports['./multiaddr']).toEqual({ types: './dist/multiaddr.d.ts', import: './dist/multiaddr.js' })
  })

  it('bundles for the browser without Node built-ins', async () => {
    const code = await bundleForBrowser(here('../src/multiaddr.ts'))
    expect(code).not.toMatch(/node:/)
    expect(code).toContain('registerUdxProtocol')
  })

  it('registers udx when imported', async () => {
    // The bundle carries its own @multiformats/multiaddr, as a browser app's
    // would; building and parsing a /udx address with it needs the codec.
    const code = await bundleForBrowser(here('../src/multiaddr.ts'))
    const mod = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`)
    expect(mod.parseUdxMultiaddr(mod.toUdxMultiaddr('203.0.113.7', 4001))).toEqual({ host: '203.0.113.7', port: 4001, family: 4 })
  })

  it('the package root does not bundle for the browser', async () => {
    await expect(bundleForBrowser(here('../src/index.ts'))).rejects.toThrow(/node:/)
  })
})
