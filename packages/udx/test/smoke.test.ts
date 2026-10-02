import { describe, expect, it } from 'vitest'
import { VERSION_CURRENT, SUPPORTED_VERSIONS } from '../src/index.js'

describe('udx package', () => {
  it('speaks wire v3', () => {
    expect(VERSION_CURRENT).toBe(3)
    expect(SUPPORTED_VERSIONS[0]).toBe(VERSION_CURRENT)
  })
})
