import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const toolDir = fileURLToPath(new URL('../../../../tools/dart-peer', import.meta.url))
const dartUdxDir = fileURLToPath(new URL('../../../../../dart-udx', import.meta.url))

/** Why the Dart peer can't run here, or undefined if it can. */
export function dartPeerUnavailable (): string | undefined {
  if (!existsSync(dartUdxDir)) return `dart-udx not found at ${dartUdxDir}`
  try {
    execFileSync('dart', ['--version'], { stdio: 'ignore' })
  } catch {
    return 'dart SDK not installed'
  }
  return undefined
}

/**
 * Compiles tools/dart-peer against ../dart-udx and returns the executable's
 * path. Compiled rather than `dart run` so each spawn starts in milliseconds.
 */
export function buildDartPeer (): string {
  const out = join(mkdtempSync(join(tmpdir(), 'js-udx-dart-peer-')), 'dart-peer')
  execFileSync('dart', ['pub', 'get'], { cwd: toolDir, stdio: 'ignore' })
  execFileSync('dart', ['compile', 'exe', 'bin/peer.dart', '-o', out], { cwd: toolDir, stdio: 'ignore' })
  return out
}
