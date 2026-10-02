import dgram from 'node:dgram'
import { isIPv6 } from 'node:net'

export interface RemoteInfo {
  address: string
  port: number
}

export interface SocketAddress {
  address: string
  port: number
  family: 'IPv4' | 'IPv6'
}

/**
 * The datagram socket a multiplexer runs on. `bindUdp` provides the real one;
 * MemoryNetwork provides an in-process one on virtual time for tests.
 */
export interface DatagramSocket {
  send: (data: Uint8Array, port: number, address: string) => void
  onMessage: (handler: (data: Uint8Array, from: RemoteInfo) => void) => void
  address: () => SocketAddress
  close: () => Promise<void>
}

export interface BindOptions {
  /** Port to bind; 0 (the default) picks a free one. */
  port?: number
  /** Address to bind; defaults to all interfaces of the family. */
  host?: string
  /** Defaults to 'udp6' when `host` is an IPv6 address, else 'udp4'. */
  type?: 'udp4' | 'udp6'
  /** Restrict a 'udp6' socket to IPv6 (no IPv4-mapped addresses). */
  ipv6Only?: boolean
  /** Socket buffer sizes to request; failures are ignored. Default 4 MiB each. */
  recvBufferSize?: number
  sendBufferSize?: number
}

/** Binds a UDP socket with Node's dgram. */
export async function bindUdp (opts: BindOptions = {}): Promise<DatagramSocket> {
  const type = opts.type ?? (opts.host !== undefined && isIPv6(opts.host) ? 'udp6' : 'udp4')
  const socket = dgram.createSocket({ type, ipv6Only: opts.ipv6Only })
  await new Promise<void>((resolve, reject) => {
    const onError = (err: Error): void => reject(err)
    socket.once('error', onError)
    socket.bind(opts.port ?? 0, opts.host, () => {
      socket.off('error', onError)
      resolve()
    })
  })
  // Send failures are reported per datagram and ignored; a socket-level error
  // after binding must not crash the process either.
  socket.on('error', () => {})
  for (const [set, size] of [
    [(n: number) => socket.setRecvBufferSize(n), opts.recvBufferSize ?? 4 << 20],
    [(n: number) => socket.setSendBufferSize(n), opts.sendBufferSize ?? 4 << 20]
  ] as const) {
    try { set(size) } catch { /* best effort: the OS may cap or refuse it */ }
  }

  let closed = false
  // dgram sends complete asynchronously; closing with sends queued drops them
  // (a final CONNECTION_CLOSE, typically), so close waits for them briefly.
  let queued = 0
  let flushed: (() => void) | undefined
  return {
    send: (data, port, address) => {
      if (closed) return
      queued++
      socket.send(data, port, address, () => {
        if (--queued === 0) flushed?.()
      })
    },
    onMessage: (handler) => {
      socket.on('message', (msg, rinfo) => handler(msg, { address: rinfo.address, port: rinfo.port }))
    },
    address: () => {
      const a = socket.address()
      return { address: a.address, port: a.port, family: a.family === 'IPv6' ? 'IPv6' : 'IPv4' }
    },
    close: async () => {
      if (closed) return
      closed = true
      if (queued > 0) {
        await new Promise<void>(resolve => {
          flushed = resolve
          setTimeout(resolve, 1000).unref()
        })
      }
      await new Promise<void>(resolve => socket.close(() => resolve()))
    }
  }
}
