import { CODE_IP4, CODE_IP6, CODE_P2P, CODE_UDP, multiaddr, registry, type Multiaddr } from '@multiformats/multiaddr'
import { code, fmt, optional, or, value } from '@multiformats/multiaddr-matcher/utils'

/** Multiaddr protocol code for `udx` (private-use range), as registered by go-libp2p-udx-transport and dart-libp2p. */
export const UDX_PROTOCOL_CODE = 0x0300

/** The `udx` component carries no value: `/ip4/<ip>/udp/<port>/udx`. */
export const UDX_PROTOCOL_SIZE = 0

export const UDX_PROTOCOL_NAME = 'udx'

/**
 * Teaches @multiformats/multiaddr the `udx` protocol, so `/udp/<port>/udx`
 * parses and prints. Runs when this package is imported; safe to repeat.
 */
export function registerUdxProtocol (): void {
  try {
    registry.getProtocol(UDX_PROTOCOL_CODE)
  } catch {
    registry.addProtocol({ code: UDX_PROTOCOL_CODE, name: UDX_PROTOCOL_NAME, size: UDX_PROTOCOL_SIZE })
  }
}

registerUdxProtocol()

/** Matches `/ip4|ip6/<host>/udp/<port>/udx`, optionally followed by `/p2p/<peer>`. */
export const UDX = fmt(or(value(CODE_IP4), value(CODE_IP6)), value(CODE_UDP), code(UDX_PROTOCOL_CODE), optional(value(CODE_P2P)))

export interface UdxAddress {
  host: string
  port: number
  family: 4 | 6
}

/**
 * The UDP endpoint of `/ip4|ip6/<host>/udp/<port>/udx`, optionally followed
 * by `/p2p/<peer>`, or undefined for anything else.
 */
export function parseUdxMultiaddr (ma: Multiaddr): UdxAddress | undefined {
  const c = ma.getComponents()
  if (c.length !== 3 && !(c.length === 4 && c[3]?.code === CODE_P2P)) return undefined
  const [ip, udp, udx] = c
  if (ip === undefined || udp === undefined || udx?.code !== UDX_PROTOCOL_CODE || udp.code !== CODE_UDP) return undefined
  if ((ip.code !== CODE_IP4 && ip.code !== CODE_IP6) || ip.value === undefined || udp.value === undefined) return undefined
  return { host: ip.value, port: Number(udp.value), family: ip.code === CODE_IP4 ? 4 : 6 }
}

export function isUdxMultiaddr (ma: Multiaddr): boolean {
  return parseUdxMultiaddr(ma) !== undefined
}

export function toUdxMultiaddr (host: string, port: number): Multiaddr {
  // Node reports IPv4 peers of a dual-stack socket as IPv4-mapped IPv6.
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(host)
  const ip = mapped?.[1] ?? host
  return multiaddr(`/${ip.includes(':') ? 'ip6' : 'ip4'}/${ip}/udp/${port}/udx`)
}
