import { VERSION_CURRENT } from '@stephanfeb/udx'

/** Multiaddr protocol code for `udx` (private-use range), as registered by go-libp2p-udx-transport and dart-libp2p. */
export const UDX_PROTOCOL_CODE = 0x0300

/** The `udx` component carries no value: `/ip4/<ip>/udp/<port>/udx`. */
export const UDX_PROTOCOL_SIZE = 0

export const UDX_PROTOCOL_NAME = 'udx'

export const UDX_WIRE_VERSION = VERSION_CURRENT
