// js-libp2p's transport compliance suite (@libp2p/interface-compliance-tests).
// It's written for mocha, so its globals are shimmed onto vitest's.
import { afterAll, afterEach, beforeAll, beforeEach, describe, it } from 'vitest'
import { UDX, udx } from '../src/index.js'

type MochaTest = (this: { skip: () => void, timeout: (ms: number) => void }) => unknown

Object.assign(globalThis, {
  describe,
  it: (name: string, fn: MochaTest) => it(name, async (ctx) => {
    await fn.call({ skip: () => { ctx.skip() }, timeout: () => {} })
  }, 120_000),
  before: beforeAll,
  after: afterAll,
  beforeEach,
  afterEach
})

// Imported after the shim: the suite registers its tests at import time.
const { default: transportCompliance } = await import('@libp2p/interface-compliance-tests/transport')

describe('interface-transport compliance', () => {
  transportCompliance({
    async setup () {
      return {
        dialer: { transports: [udx()] },
        listener: {
          addresses: { listen: ['/ip4/127.0.0.1/udp/0/udx', '/ip4/127.0.0.1/udp/0/udx'] },
          transports: [udx()]
        },
        dialMultiaddrMatcher: UDX,
        listenMultiaddrMatcher: UDX
      }
    },
    async teardown () {}
  })
})
