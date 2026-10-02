import { createHmac, randomFillSync, timingSafeEqual } from 'node:crypto'
import type { ConnectionId } from './cid.js'
import { MIN_STATELESS_RESET_PACKET_SIZE, STATELESS_RESET_TOKEN_LENGTH } from './constants.js'

// Stateless reset, as dart-udx implements it: random bytes followed by a
// 16-byte token, at least 39 bytes in all. A receiver recognises one by
// matching the trailing 16 bytes against tokens it was given; go-udx neither
// sends nor checks them.

const MIN_SECRET_LENGTH = 32

/** Derives the reset token for a CID: the first 16 bytes of HMAC-SHA256(secret, cid). */
export function statelessResetToken (secret: Uint8Array, cid: ConnectionId): Uint8Array {
  if (secret.length < MIN_SECRET_LENGTH) {
    throw new RangeError(`stateless reset secret must be at least ${MIN_SECRET_LENGTH} bytes`)
  }
  const digest = createHmac('sha256', secret).update(cid.bytes).digest()
  return new Uint8Array(digest.subarray(0, STATELESS_RESET_TOKEN_LENGTH))
}

export function encodeStatelessReset (token: Uint8Array, size: number = MIN_STATELESS_RESET_PACKET_SIZE): Uint8Array {
  if (token.length !== STATELESS_RESET_TOKEN_LENGTH) {
    throw new RangeError(`stateless reset token must be ${STATELESS_RESET_TOKEN_LENGTH} bytes`)
  }
  const buf = new Uint8Array(Math.max(size, MIN_STATELESS_RESET_PACKET_SIZE))
  randomFillSync(buf, 0, buf.length - STATELESS_RESET_TOKEN_LENGTH)
  buf.set(token, buf.length - STATELESS_RESET_TOKEN_LENGTH)
  return buf
}

/** The trailing token of a datagram that could be a stateless reset, or undefined if it is too short. */
export function candidateResetToken (datagram: Uint8Array): Uint8Array | undefined {
  if (datagram.length < MIN_STATELESS_RESET_PACKET_SIZE) return undefined
  return datagram.subarray(datagram.length - STATELESS_RESET_TOKEN_LENGTH)
}

export function resetTokensEqual (a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && timingSafeEqual(a, b)
}
