// Protocol constants. Values mirror go-udx constants.go (wire v3), which is the
// wire authority for this port.

export const VERSION_V1 = 0x00000001
export const VERSION_V2 = 0x00000002
export const VERSION_V3 = 0x00000003

/** The only version accepted on the data path; any other is dropped. */
export const VERSION_CURRENT = VERSION_V3

/** Versions this build reports in version negotiation, in preference order. */
export const SUPPORTED_VERSIONS: readonly number[] = [VERSION_V3, VERSION_V2, VERSION_V1]

export const MIN_CID_LENGTH = 0
export const MAX_CID_LENGTH = 20
export const DEFAULT_CID_LENGTH = 8
