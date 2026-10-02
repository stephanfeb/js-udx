import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
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

export interface GoPeer {
  proc: ChildProcess
  /** Resolves with the first stderr line matching `prefix`, minus the prefix. */
  line: (prefix: string, timeoutMs?: number) => Promise<string>
  stderr: () => string
  exited: Promise<number | null>
  kill: () => void
}

export function startGoPeer (binary: string, args: string[]): GoPeer {
  const proc = spawn(binary, args, { stdio: ['ignore', 'ignore', 'pipe'] })
  let err = ''
  const waiters: Array<() => void> = []
  proc.stderr?.setEncoding('utf8')
  proc.stderr?.on('data', (d: string) => {
    err += d
    for (const w of waiters.splice(0)) w()
  })
  const exited = new Promise<number | null>(resolve => proc.on('exit', code => {
    for (const w of waiters.splice(0)) w()
    resolve(code)
  }))
  return {
    proc,
    stderr: () => err,
    exited,
    kill: () => { proc.kill('SIGKILL') },
    line: async (prefix, timeoutMs = 30_000) => {
      const deadline = Date.now() + timeoutMs
      for (;;) {
        const match = err.split('\n').find(l => l.startsWith(prefix))
        if (match !== undefined) return match.slice(prefix.length).trim()
        if (proc.exitCode !== null) throw new Error(`go-peer exited (${proc.exitCode}) without "${prefix}": ${err}`)
        if (Date.now() > deadline) throw new Error(`timed out waiting for "${prefix}": ${err}`)
        await new Promise<void>(resolve => {
          waiters.push(resolve)
          setTimeout(resolve, 100)
        })
      }
    }
  }
}
