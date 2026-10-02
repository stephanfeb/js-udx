import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const toolDir = fileURLToPath(new URL('../../../../tools/go-peer', import.meta.url))
const goUdxDir = fileURLToPath(new URL('../../../../../go-udx', import.meta.url))

/** Why the Go peer can't run here, or undefined if it can. */
export function goPeerUnavailable (): string | undefined {
  if (!existsSync(goUdxDir)) return `go-udx not found at ${goUdxDir}`
  try {
    execFileSync('go', ['version'], { stdio: 'ignore' })
  } catch {
    return 'go toolchain not installed'
  }
  return undefined
}

/** Builds tools/go-peer against ../go-udx and returns the binary's path. */
export function buildGoPeer (): string {
  const out = join(mkdtempSync(join(tmpdir(), 'js-udx-go-peer-')), 'go-peer')
  execFileSync('go', ['build', '-o', out, '.'], { cwd: toolDir, stdio: 'inherit' })
  return out
}
