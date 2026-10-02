import { U32_MAX, checkUint, view } from './bytes.js'
import { ConnectionId } from './cid.js'
import { MAX_CID_LENGTH, MIN_CID_LENGTH } from './constants.js'
import { CodecError } from './errors.js'
import { type Frame, decodeFrame, encodeFrame, frameLength } from './frames.js'

/**
 * A UDX packet. Wire layout (big-endian):
 *
 *   version u32 | dcidLen u8 | dcid | scidLen u8 | scid | seq u32 | dstStreamId u32 | srcStreamId u32 | frames…
 */
export interface Packet {
  version: number
  destinationCid: ConnectionId
  sourceCid: ConnectionId
  sequence: number
  destinationStreamId: number
  sourceStreamId: number
  frames: Frame[]
}

/** version(4) + dcidLen(1) + scidLen(1) + seq(4) + dstStream(4) + srcStream(4) */
export const MIN_PACKET_HEADER_LENGTH = 18

export function packetHeaderLength (dcidLength: number, scidLength: number): number {
  return MIN_PACKET_HEADER_LENGTH + dcidLength + scidLength
}

export function packetLength (packet: Packet): number {
  let n = packetHeaderLength(packet.destinationCid.length, packet.sourceCid.length)
  for (const f of packet.frames) n += frameLength(f)
  return n
}

/** Serializes a packet into a single new buffer. */
export function encodePacket (packet: Packet): Uint8Array {
  checkUint(packet.version, U32_MAX, 'packet version')
  checkUint(packet.sequence, U32_MAX, 'packet sequence')
  checkUint(packet.destinationStreamId, U32_MAX, 'packet destinationStreamId')
  checkUint(packet.sourceStreamId, U32_MAX, 'packet sourceStreamId')

  const buf = new Uint8Array(packetLength(packet))
  const dv = view(buf)
  let p = 0
  dv.setUint32(p, packet.version)
  p += 4
  p = writeCid(buf, p, packet.destinationCid)
  p = writeCid(buf, p, packet.sourceCid)
  dv.setUint32(p, packet.sequence)
  dv.setUint32(p + 4, packet.destinationStreamId)
  dv.setUint32(p + 8, packet.sourceStreamId)
  p += 12
  for (const f of packet.frames) p += encodeFrame(f, buf, p)
  return buf
}

function writeCid (buf: Uint8Array, offset: number, cid: ConnectionId): number {
  buf[offset] = cid.length
  buf.set(cid.bytes, offset + 1)
  return offset + 1 + cid.length
}

/**
 * Parses a datagram. The version is not checked here; the receive path drops
 * packets whose version isn't current. Frame payloads are views into `data`.
 */
export function decodePacket (data: Uint8Array): Packet {
  if (data.length < MIN_PACKET_HEADER_LENGTH) {
    throw new CodecError('too-short', `packet too short: need at least ${MIN_PACKET_HEADER_LENGTH} bytes, got ${data.length}`)
  }
  const dv = view(data)
  const version = dv.getUint32(0)
  let p = 4

  const [destinationCid, afterDcid] = readCid(data, p, 'destination')
  p = afterDcid
  if (p >= data.length) throw new CodecError('too-short', 'packet too short: source CID length')
  const [sourceCid, afterScid] = readCid(data, p, 'source')
  p = afterScid

  if (p + 12 > data.length) throw new CodecError('too-short', 'packet too short: sequence and stream IDs')
  const sequence = dv.getUint32(p)
  const destinationStreamId = dv.getUint32(p + 4)
  const sourceStreamId = dv.getUint32(p + 8)
  p += 12

  const frames: Frame[] = []
  while (p < data.length) {
    const [frame, consumed] = decodeFrame(data, p)
    frames.push(frame)
    p += consumed
  }

  return { version, destinationCid, sourceCid, sequence, destinationStreamId, sourceStreamId, frames }
}

function readCid (data: Uint8Array, offset: number, which: string): [ConnectionId, number] {
  const len = data[offset] as number
  if (len < MIN_CID_LENGTH || len > MAX_CID_LENGTH) {
    throw new CodecError('invalid-cid-length', `${which} CID length ${len}`)
  }
  const start = offset + 1
  if (start + len > data.length) throw new CodecError('too-short', `packet too short: ${which} CID data`)
  return [new ConnectionId(data.subarray(start, start + len)), start + len]
}

/** The fields a datagram's header gives before its frames are parsed. */
export interface PacketHeader {
  version: number
  destinationCid: ConnectionId
  sourceCid: ConnectionId
}

/**
 * Reads the version and connection IDs without parsing frames, or returns
 * undefined if even those don't fit. Works for any version, and for version
 * negotiation packets (version 0), whose header ends after the source CID.
 */
export function peekHeader (data: Uint8Array): PacketHeader | undefined {
  if (data.length < 6) return undefined
  const dcidLen = data[4] as number
  if (dcidLen > MAX_CID_LENGTH || 5 + dcidLen >= data.length) return undefined
  const scidLen = data[5 + dcidLen] as number
  if (scidLen > MAX_CID_LENGTH || 6 + dcidLen + scidLen > data.length) return undefined
  return {
    version: view(data).getUint32(0),
    destinationCid: new ConnectionId(data.subarray(5, 5 + dcidLen)),
    sourceCid: new ConnectionId(data.subarray(6 + dcidLen, 6 + dcidLen + scidLen))
  }
}
