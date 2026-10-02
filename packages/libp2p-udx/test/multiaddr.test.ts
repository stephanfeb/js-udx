import { defaultLogger } from '@libp2p/logger'
import { multiaddr } from '@multiformats/multiaddr'
import { describe, expect, it } from 'vitest'
import { UDX, UDX_PROTOCOL_CODE, UDX_WIRE_VERSION, UdxTransport, isUdxMultiaddr, parseUdxMultiaddr, toUdxMultiaddr } from '../src/index.js'

const PEER = '12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA'

describe('udx multiaddrs', () => {
  it('uses the udx code shared with go and dart, over wire v3', () => {
    expect(UDX_PROTOCOL_CODE).toBe(0x0300)
    expect(UDX_WIRE_VERSION).toBe(3)
    // Size 0: the component is just the varint of 0x0300.
    expect(Array.from(multiaddr('/udx').bytes)).toEqual([0x80, 0x06])
  })

  it('parses IPv4 and IPv6 addresses, with or without a peer ID', () => {
    expect(parseUdxMultiaddr(multiaddr('/ip4/127.0.0.1/udp/4001/udx'))).toEqual({ host: '127.0.0.1', port: 4001, family: 4 })
    expect(parseUdxMultiaddr(multiaddr(`/ip6/::1/udp/9/udx/p2p/${PEER}`))).toEqual({ host: '::1', port: 9, family: 6 })
    expect(UDX.exactMatch(multiaddr('/ip4/127.0.0.1/udp/4001/udx'))).toBe(true)
    expect(UDX.exactMatch(multiaddr(`/ip6/::1/udp/9/udx/p2p/${PEER}`))).toBe(true)
  })

  it('rejects anything that is not exactly ip/udp/udx', () => {
    for (const s of ['/ip4/127.0.0.1/udp/4001', '/ip4/127.0.0.1/tcp/4001', '/ip4/127.0.0.1/udp/4001/quic-v1',
      '/ip4/127.0.0.1/udp/4001/udx/p2p-circuit', '/dns4/example.com/udp/4001/udx', '/udx']) {
      expect(isUdxMultiaddr(multiaddr(s)), s).toBe(false)
      expect(UDX.exactMatch(multiaddr(s)), s).toBe(false)
    }
  })

  it('formats socket addresses, unmapping IPv4-mapped IPv6', () => {
    expect(toUdxMultiaddr('10.0.0.1', 5).toString()).toBe('/ip4/10.0.0.1/udp/5/udx')
    expect(toUdxMultiaddr('::ffff:10.0.0.1', 5).toString()).toBe('/ip4/10.0.0.1/udp/5/udx')
    expect(toUdxMultiaddr('fe80::1', 5).toString()).toBe('/ip6/fe80::1/udp/5/udx')
  })

  it('filters dial and listen addresses', () => {
    const t = new UdxTransport({ logger: defaultLogger() })
    const addrs = ['/ip4/1.2.3.4/udp/1/udx', '/ip4/1.2.3.4/tcp/1', `/ip4/1.2.3.4/udp/2/udx/p2p/${PEER}`].map(s => multiaddr(s))
    expect(t.dialFilter(addrs).map(String)).toEqual(['/ip4/1.2.3.4/udp/1/udx', `/ip4/1.2.3.4/udp/2/udx/p2p/${PEER}`])
    expect(t.listenFilter(addrs)).toHaveLength(2)
  })
})
