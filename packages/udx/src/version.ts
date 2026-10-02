import { U32_MAX, checkUint, view } from './bytes.js'
import { ConnectionId } from './cid.js'
import { MAX_CID_LENGTH, SUPPORTED_VERSIONS } from './constants.js'
import { CodecError } from './errors.js'

export function isVersionSupported (version: number): boolean {
  return SUPPORTED_VERSIONS.includes(version)
}

/** The highest version both sides support, in our preference order, or 0 if none. */
export function negotiateVersion (peerVersions: readonly number[]): number {
  return SUPPORTED_VERSIONS.find(v => peerVersions.includes(v)) ?? 0
}

/**
 * Sent in reply to a packet with an unsupported version. Wire layout:
 *
 *   version u32 = 0 | dcidLen u8 | dcid | scidLen u8 | scid | supported versions u32…
 *
 * It has no sequence or stream ID fields.
 */
export interface VersionNegotiationPacket {
  destinationCid: ConnectionId
  sourceCid: ConnectionId
  supportedVersions: number[]
}

export function encodeVersionNegotiation (p: VersionNegotiationPacket): Uint8Array {
  const buf = new Uint8Array(6 + p.destinationCid.length + p.sourceCid.length + p.supportedVersions.length * 4)
  const dv = view(buf)
  let o = 4 // version 0
  buf[o++] = p.destinationCid.length
  buf.set(p.destinationCid.bytes, o)
  o += p.destinationCid.length
  buf[o++] = p.sourceCid.length
  buf.set(p.sourceCid.bytes, o)
  o += p.sourceCid.length
  for (const v of p.supportedVersions) {
    checkUint(v, U32_MAX, 'supported version')
    dv.setUint32(o, v)
    o += 4
  }
  return buf
}

/** Parses a version negotiation packet. Trailing bytes short of a whole u32 are ignored, as in go-udx. */
export function decodeVersionNegotiation (data: Uint8Array): VersionNegotiationPacket {
  if (data.length < 6) throw new CodecError('too-short', 'packet too short: version negotiation packet')
  const dv = view(data)
  if (dv.getUint32(0) !== 0) throw new CodecError('value-out-of-range', 'invalid version negotiation packet: version != 0')
  let o = 4
  const dcidLen = data[o++] as number
  if (dcidLen > MAX_CID_LENGTH || o + dcidLen > data.length) throw new CodecError('invalid-cid-length', 'version negotiation destination CID')
  const destinationCid = new ConnectionId(data.subarray(o, o + dcidLen))
  o += dcidLen
  if (o >= data.length) throw new CodecError('too-short', 'packet too short: version negotiation packet')
  const scidLen = data[o++] as number
  if (scidLen > MAX_CID_LENGTH || o + scidLen > data.length) throw new CodecError('invalid-cid-length', 'version negotiation source CID')
  const sourceCid = new ConnectionId(data.subarray(o, o + scidLen))
  o += scidLen
  const supportedVersions: number[] = []
  while (o + 4 <= data.length) {
    supportedVersions.push(dv.getUint32(o))
    o += 4
  }
  return { destinationCid, sourceCid, supportedVersions }
}
