/**
 * @packageDocumentation
 *
 * A js-libp2p transport over UDX, interoperable with go-libp2p-udx-transport
 * and dart-libp2p.
 *
 * @example
 *
 * ```TypeScript
 * import { createLibp2p } from 'libp2p'
 * import { noise } from '@chainsafe/libp2p-noise'
 * import { yamux } from '@chainsafe/libp2p-yamux'
 * import { udx } from '@stephanfeb/libp2p-udx'
 *
 * const node = await createLibp2p({
 *   addresses: { listen: ['/ip4/0.0.0.0/udp/0/udx'] },
 *   transports: [udx()],
 *   connectionEncrypters: [noise()],
 *   streamMuxers: [yamux()]
 * })
 * ```
 */
import { VERSION_CURRENT } from '@stephanfeb/udx'
import { UdxTransport, type UdxComponents, type UdxTransportInit } from './transport.js'
import type { Transport } from '@libp2p/interface'

export { UdxTransport, type UdxComponents, type UdxTransportInit } from './transport.js'
export { UdxMultiaddrConnection } from './connection.js'
export { UdxListener } from './listener.js'
export {
  UDX,
  UDX_PROTOCOL_CODE,
  UDX_PROTOCOL_NAME,
  UDX_PROTOCOL_SIZE,
  isUdxMultiaddr,
  parseUdxMultiaddr,
  registerUdxProtocol,
  toUdxMultiaddr,
  type UdxAddress
} from './multiaddr.js'

export const UDX_WIRE_VERSION = VERSION_CURRENT

export function udx (init: UdxTransportInit = {}): (components: UdxComponents) => Transport {
  return (components) => new UdxTransport(components, init)
}
