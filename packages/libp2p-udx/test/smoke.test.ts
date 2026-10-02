import { describe, expect, it } from 'vitest'
import { UDX_PROTOCOL_CODE, UDX_WIRE_VERSION } from '../src/index.js'

describe('libp2p-udx package', () => {
  it('uses the udx multiaddr code shared with go and dart', () => {
    expect(UDX_PROTOCOL_CODE).toBe(0x0300)
    expect(UDX_WIRE_VERSION).toBe(3)
  })
})
