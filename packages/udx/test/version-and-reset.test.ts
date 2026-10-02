import { describe, expect, it } from 'vitest'
import {
  ConnectionId,
  MIN_STATELESS_RESET_PACKET_SIZE,
  candidateResetToken,
  encodeStatelessReset,
  isVersionSupported,
  negotiateVersion,
  resetTokensEqual,
  statelessResetToken
} from '../src/index.js'

describe('version negotiation helpers', () => {
  it('prefers the highest common version', () => {
    expect(negotiateVersion([1, 3])).toBe(3)
    expect(negotiateVersion([2, 1])).toBe(2)
    expect(negotiateVersion([7])).toBe(0)
  })

  it('reports supported versions', () => {
    expect(isVersionSupported(3)).toBe(true)
    expect(isVersionSupported(0)).toBe(false)
  })
})

describe('stateless reset', () => {
  const secret = new Uint8Array(32).fill(7)
  const cid = new ConnectionId(Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8))

  it('derives a stable 16-byte token per CID', () => {
    const t1 = statelessResetToken(secret, cid)
    expect(t1.length).toBe(16)
    expect(resetTokensEqual(t1, statelessResetToken(secret, cid))).toBe(true)
    expect(resetTokensEqual(t1, statelessResetToken(secret, ConnectionId.random()))).toBe(false)
  })

  it('requires a secret of at least 32 bytes', () => {
    expect(() => statelessResetToken(new Uint8Array(31), cid)).toThrow(RangeError)
  })

  it('puts the token in the last 16 bytes of a packet at least 39 bytes long', () => {
    const token = statelessResetToken(secret, cid)
    const packet = encodeStatelessReset(token)
    expect(packet.length).toBe(MIN_STATELESS_RESET_PACKET_SIZE)
    expect(resetTokensEqual(candidateResetToken(packet) as Uint8Array, token)).toBe(true)
    expect(encodeStatelessReset(token, 10).length).toBe(MIN_STATELESS_RESET_PACKET_SIZE)
    expect(candidateResetToken(new Uint8Array(38))).toBeUndefined()
  })
})
