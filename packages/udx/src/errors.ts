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
