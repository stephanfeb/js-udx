import { AbstractMultiaddrConnection } from '@libp2p/utils'
import { ConnectionClosedError, ErrorCode, type UdxConnection, type UdxStream } from '@stephanfeb/udx'
import type { AbortOptions } from '@libp2p/interface'
import type { AbstractMultiaddrConnectionInit, SendResult } from '@libp2p/utils'
import type { Uint8ArrayList } from 'uint8arraylist'

export interface UdxMultiaddrConnectionInit extends AbstractMultiaddrConnectionInit {
  /** The connection's first stream, which carries the libp2p upgrade. */
  stream: UdxStream
  /** The UDX connection, which exists for this one stream. */
  connection: UdxConnection
  /** How long a graceful close waits for the peer to acknowledge what we sent. */
  closeTimeout: number
}

/**
 * The raw libp2p connection: one UDX stream, upgraded with Noise and Yamux
 * like a TCP socket, as go-libp2p-udx-transport and dart-libp2p do. Closing it
 * closes the whole UDX connection.
 */
export class UdxMultiaddrConnection extends AbstractMultiaddrConnection {
  private readonly stream: UdxStream
  private readonly connection: UdxConnection
  private readonly closeTimeout: number

  constructor (init: UdxMultiaddrConnectionInit) {
    super(init)
    this.stream = init.stream
    this.connection = init.connection
    this.closeTimeout = init.closeTimeout

    this.stream.on('data', data => { this.onData(data) })
    this.stream.on('end', () => { this.onRemoteCloseWrite() })
    this.stream.on('drain', () => { this.safeDispatchEvent('drain') })
    this.stream.on('close', err => {
      if (err !== undefined && !isCleanClose(err)) {
        this.abort(err)
      } else {
        this.onTransportClosed()
      }
    })
    this.connection.on('close', err => {
      this.log('udx connection closed %s', err?.message ?? '')
      if (err !== undefined && !isCleanClose(err)) {
        this.abort(err)
      } else {
        this.onTransportClosed()
      }
    })
  }

  sendData (data: Uint8ArrayList): SendResult {
    let sentBytes = 0
    let canSendMore = true
    for (const buf of data) {
      sentBytes += buf.byteLength
      canSendMore = this.stream.write(buf)
      if (!canSendMore) break
    }
    return { sentBytes, canSendMore }
  }

  /**
   * Ends our side and waits, up to `closeTimeout`, for the peer to acknowledge
   * everything: CONNECTION_CLOSE discards whatever is still in flight.
   */
  async sendClose (options?: AbortOptions): Promise<void> {
    if (this.connection.closed) return
    this.stream.end()
    let timer: ReturnType<typeof setTimeout> | undefined
    let onAbort: (() => void) | undefined
    try {
      await Promise.race([
        this.connection.flushed(),
        new Promise<void>(resolve => {
          timer = setTimeout(resolve, this.closeTimeout)
          onAbort = resolve
          options?.signal?.addEventListener('abort', onAbort, { once: true })
        })
      ])
    } finally {
      clearTimeout(timer)
      if (onAbort !== undefined) options?.signal?.removeEventListener('abort', onAbort)
    }
    this.connection.close()
  }

  sendReset (err: Error): void {
    this.stream.reset(ErrorCode.InternalError)
    this.connection.close(ErrorCode.InternalError, err.message.slice(0, 256))
  }

  sendPause (): void {
    this.stream.pause()
  }

  sendResume (): void {
    this.stream.resume()
  }
}

/** A peer's graceful CONNECTION_CLOSE, or our own. */
function isCleanClose (err: Error): boolean {
  return err instanceof ConnectionClosedError && err.code === ErrorCode.NoError
}
