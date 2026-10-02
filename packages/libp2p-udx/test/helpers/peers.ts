// Builds the native libp2p interop peers in tools/ against the sibling
// checkouts. Each is skipped (with a reason) when its toolchain or repos are missing.
import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../../../../', import.meta.url))
const sibling = (name: string): string => join(root, '..', name)

function missing (tool: string[], repos: string[]): string | undefined {
  for (const repo of repos) if (!existsSync(sibling(repo))) return `${repo} not found at ${sibling(repo)}`
  try {
    execFileSync(tool[0] as string, tool.slice(1), { stdio: 'ignore' })
  } catch {
    return `${tool[0] as string} not installed`
  }
  return undefined
}

export interface NativePeer {
  name: string
  /** Why it can't run here, or undefined if it can. */
  unavailable: string | undefined
  build: () => string
}

export const goLibp2pPeer: NativePeer = {
  name: 'go-libp2p',
  unavailable: missing(['go', 'version'], ['go-udx', 'go-libp2p-udx-transport']),
  build: () => {
    const out = join(mkdtempSync(join(tmpdir(), 'js-udx-go-libp2p-peer-')), 'peer')
    execFileSync('go', ['build', '-o', out, '.'], { cwd: join(root, 'tools/go-libp2p-peer'), stdio: 'inherit' })
    return out
  }
}

export const dartLibp2pPeer: NativePeer = {
  name: 'dart-libp2p',
  unavailable: missing(['dart', '--version'], ['dart-udx', 'dart-libp2p']),
  build: () => {
    const dir = join(root, 'tools/dart-libp2p-peer')
    const out = join(mkdtempSync(join(tmpdir(), 'js-udx-dart-libp2p-peer-')), 'peer')
    execFileSync('dart', ['pub', 'get'], { cwd: dir, stdio: 'ignore' })
    execFileSync('dart', ['compile', 'exe', 'bin/peer.dart', '-o', out], { cwd: dir, stdio: 'ignore' })
    return out
  }
}
