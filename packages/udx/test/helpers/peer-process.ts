// A child-process interop peer that reports on stderr ("READY <port>",
// "RESULT <bytes>", "CORRUPT <detail>"), as tools/go-peer and tools/dart-peer do.
import { type ChildProcess, spawn } from 'node:child_process'

export interface PeerProcess {
  proc: ChildProcess
  /** Resolves with the first stderr line matching `prefix`, minus the prefix. */
  line: (prefix: string, timeoutMs?: number) => Promise<string>
  stderr: () => string
  exited: Promise<number | null>
  kill: () => void
}

export function startPeer (binary: string, args: string[]): PeerProcess {
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
        if (proc.exitCode !== null) throw new Error(`peer exited (${proc.exitCode}) without "${prefix}": ${err}`)
        if (Date.now() > deadline) throw new Error(`timed out waiting for "${prefix}": ${err}`)
        await new Promise<void>(resolve => {
          waiters.push(resolve)
          setTimeout(resolve, 100)
        })
      }
    }
  }
}
