export type CodecErrorKind = 'too-short' | 'invalid-cid-length' | 'unknown-frame-type' | 'value-out-of-range'

/**
 * A datagram that could not be decoded, or a value that cannot be encoded.
 * Decoding failures reject the whole packet, as go-udx does.
 */
export class CodecError extends Error {
  readonly kind: CodecErrorKind

  constructor (kind: CodecErrorKind, message: string) {
    super(message)
    this.name = 'CodecError'
    this.kind = kind
  }
}

/** A UDX protocol error with a wire error code (see `ErrorCode`). */
export class UdxError extends Error {
  readonly code: number

  constructor (code: number, reason: string) {
    super(`udx error ${code}: ${reason}`)
    this.name = 'UdxError'
    this.code = code
  }
}

/** The stream was reset, locally (`remote: false`) or by the peer. */
export class StreamResetError extends Error {
  readonly code: number
  readonly remote: boolean

  constructor (code: number, remote: boolean) {
    super(`stream reset ${remote ? 'by peer' : 'locally'} with code ${code}`)
    this.name = 'StreamResetError'
    this.code = code
    this.remote = remote
  }
}

/** The connection closed. `code` is the CONNECTION_CLOSE error code (6 for an idle timeout). */
export class ConnectionClosedError extends Error {
  readonly code: number
  readonly reason: string
  readonly remote: boolean

  constructor (code: number, reason: string, remote: boolean) {
    super(`connection closed${remote ? ' by peer' : ''}: code ${code}${reason !== '' ? ` (${reason})` : ''}`)
    this.name = 'ConnectionClosedError'
    this.code = code
    this.reason = reason
    this.remote = remote
  }
}

/** A write after the stream's writable side was ended. */
export class WriteAfterEndError extends Error {
  constructor () {
    super('write after end')
    this.name = 'WriteAfterEndError'
  }
}
