import { U16_MAX, U32_MAX, U64_MAX_SAFE, U8_MAX, checkUint, readU64, view, writeU64 } from './bytes.js'
import { ConnectionId } from './cid.js'
import { MAX_CID_LENGTH, STATELESS_RESET_TOKEN_LENGTH } from './constants.js'
import { CodecError } from './errors.js'

/**
 * Frame type bytes. These follow go-udx (frame.go). 0x0c is unused; dart-udx
 * numbers 0x0c–0x10 one lower, which is a dart-udx bug.
 */
export const FrameType = {
  Padding: 0x00,
  Ping: 0x01,
  Ack: 0x02,
  Stream: 0x03,
  WindowUpdate: 0x04,
  MaxData: 0x05,
  ResetStream: 0x06,
  MaxStreams: 0x07,
  MtuProbe: 0x08,
  PathChallenge: 0x09,
  PathResponse: 0x0a,
  ConnectionClose: 0x0b,
  StopSending: 0x0d,
  DataBlocked: 0x0e,
  StreamDataBlocked: 0x0f,
  NewConnectionId: 0x10,
  RetireConnectionId: 0x11
} as const

export type FrameType = typeof FrameType[keyof typeof FrameType]

export const STREAM_FLAG_FIN = 0x01
export const STREAM_FLAG_SYN = 0x02

/** type(1) + flags(1) + offset(8) + dataLen(2) */
export const STREAM_FRAME_HEADER_LENGTH = 12

export const PATH_DATA_LENGTH = 8

export interface PaddingFrame { type: typeof FrameType.Padding }
export interface PingFrame { type: typeof FrameType.Ping }

/** One further ACK range below the previous one: `gap` missing sequences, then `length` received ones. */
export interface AckRange { gap: number, length: number }

/**
 * Acknowledges received data packets. `firstAckRangeLength` is the count of
 * contiguous sequences ending at `largestAcked`. Gaps and lengths are raw counts,
 * not QUIC's minus-one encoding.
 */
export interface AckFrame {
  type: typeof FrameType.Ack
  largestAcked: number
  /** Milliseconds since the largest packet arrived. */
  ackDelay: number
  firstAckRangeLength: number
  ranges: AckRange[]
}

/** Stream data at `offset` within the stream. On FIN, `offset + data.length` is the final size. */
export interface StreamFrame {
  type: typeof FrameType.Stream
  fin: boolean
  syn: boolean
  offset: number
  data: Uint8Array
}

/** Absolute stream offset the peer may send up to, modulo 2^32 (not a window size). */
export interface WindowUpdateFrame { type: typeof FrameType.WindowUpdate, limit: number }
export interface MaxDataFrame { type: typeof FrameType.MaxData, maxData: number }
/** The stream is identified by the packet header's stream IDs. */
export interface ResetStreamFrame { type: typeof FrameType.ResetStream, errorCode: number }
export interface MaxStreamsFrame { type: typeof FrameType.MaxStreams, maxStreams: number }
/** Zero padding that runs to the end of the datagram. `size` includes the type byte. */
export interface MtuProbeFrame { type: typeof FrameType.MtuProbe, size: number }
export interface PathChallengeFrame { type: typeof FrameType.PathChallenge, data: Uint8Array }
export interface PathResponseFrame { type: typeof FrameType.PathResponse, data: Uint8Array }
export interface ConnectionCloseFrame {
  type: typeof FrameType.ConnectionClose
  errorCode: number
  /** The frame type that caused the error, or 0. */
  frameType: number
  reason: string
}
export interface StopSendingFrame { type: typeof FrameType.StopSending, streamId: number, errorCode: number }
export interface DataBlockedFrame { type: typeof FrameType.DataBlocked, limit: number }
export interface StreamDataBlockedFrame { type: typeof FrameType.StreamDataBlocked, streamId: number, limit: number }
export interface NewConnectionIdFrame {
  type: typeof FrameType.NewConnectionId
  sequence: number
  retirePriorTo: number
  connectionId: ConnectionId
  resetToken: Uint8Array
}
export interface RetireConnectionIdFrame { type: typeof FrameType.RetireConnectionId, sequence: number }

export type Frame =
  | PaddingFrame
  | PingFrame
  | AckFrame
  | StreamFrame
  | WindowUpdateFrame
  | MaxDataFrame
  | ResetStreamFrame
  | MaxStreamsFrame
  | MtuProbeFrame
  | PathChallengeFrame
  | PathResponseFrame
  | ConnectionCloseFrame
  | StopSendingFrame
  | DataBlockedFrame
  | StreamDataBlockedFrame
  | NewConnectionIdFrame
  | RetireConnectionIdFrame

const utf8Encoder = new TextEncoder()
const utf8Decoder = new TextDecoder()

/** Encoded size of a frame in bytes. */
export function frameLength (frame: Frame): number {
  switch (frame.type) {
    case FrameType.Padding:
    case FrameType.Ping:
      return 1
    case FrameType.Ack:
      return 12 + frame.ranges.length * 5
    case FrameType.Stream:
      return STREAM_FRAME_HEADER_LENGTH + frame.data.length
    case FrameType.WindowUpdate:
    case FrameType.ResetStream:
    case FrameType.MaxStreams:
      return 5
    case FrameType.MaxData:
    case FrameType.DataBlocked:
    case FrameType.RetireConnectionId:
    case FrameType.PathChallenge:
    case FrameType.PathResponse:
    case FrameType.StopSending:
      return 9
    case FrameType.MtuProbe:
      return Math.max(1, frame.size)
    case FrameType.ConnectionClose:
      return 11 + utf8Encoder.encode(frame.reason).length
    case FrameType.StreamDataBlocked:
      return 13
    case FrameType.NewConnectionId:
      return 18 + frame.connectionId.length + STATELESS_RESET_TOKEN_LENGTH
  }
}

/**
 * Writes `frame` into `buf` at `offset` and returns the number of bytes written.
 * `buf` must have `frameLength(frame)` bytes available. Throws CodecError for
 * values that don't fit their wire field.
 */
export function encodeFrame (frame: Frame, buf: Uint8Array, offset: number): number {
  const dv = view(buf)
  buf[offset] = frame.type
  switch (frame.type) {
    case FrameType.Padding:
    case FrameType.Ping:
      return 1
    case FrameType.Ack: {
      checkUint(frame.largestAcked, U32_MAX, 'ACK largestAcked')
      checkUint(frame.ackDelay, U16_MAX, 'ACK ackDelay')
      checkUint(frame.ranges.length, U8_MAX, 'ACK range count')
      checkUint(frame.firstAckRangeLength, U32_MAX, 'ACK firstAckRangeLength')
      dv.setUint32(offset + 1, frame.largestAcked)
      dv.setUint16(offset + 5, frame.ackDelay)
      buf[offset + 7] = frame.ranges.length
      dv.setUint32(offset + 8, frame.firstAckRangeLength)
      let p = offset + 12
      for (const r of frame.ranges) {
        checkUint(r.gap, U8_MAX, 'ACK range gap')
        checkUint(r.length, U32_MAX, 'ACK range length')
        buf[p] = r.gap
        dv.setUint32(p + 1, r.length)
        p += 5
      }
      return p - offset
    }
    case FrameType.Stream: {
      checkUint(frame.data.length, U16_MAX, 'STREAM data length')
      checkUint(frame.offset, U64_MAX_SAFE, 'STREAM offset')
      buf[offset + 1] = (frame.fin ? STREAM_FLAG_FIN : 0) | (frame.syn ? STREAM_FLAG_SYN : 0)
      writeU64(dv, offset + 2, frame.offset)
      dv.setUint16(offset + 10, frame.data.length)
      buf.set(frame.data, offset + STREAM_FRAME_HEADER_LENGTH)
      return STREAM_FRAME_HEADER_LENGTH + frame.data.length
    }
    case FrameType.WindowUpdate:
      return writeU32Frame(dv, offset, frame.limit, 'WINDOW_UPDATE limit')
    case FrameType.ResetStream:
      return writeU32Frame(dv, offset, frame.errorCode, 'RESET_STREAM errorCode')
    case FrameType.MaxStreams:
      return writeU32Frame(dv, offset, frame.maxStreams, 'MAX_STREAMS maxStreams')
    case FrameType.MaxData:
      return writeU64Frame(dv, offset, frame.maxData, 'MAX_DATA maxData')
    case FrameType.DataBlocked:
      return writeU64Frame(dv, offset, frame.limit, 'DATA_BLOCKED limit')
    case FrameType.RetireConnectionId:
      return writeU64Frame(dv, offset, frame.sequence, 'RETIRE_CONNECTION_ID sequence')
    case FrameType.MtuProbe: {
      const size = Math.max(1, frame.size)
      buf.fill(0, offset + 1, offset + size)
      return size
    }
    case FrameType.PathChallenge:
    case FrameType.PathResponse:
      if (frame.data.length !== PATH_DATA_LENGTH) {
        throw new CodecError('value-out-of-range', `PATH_CHALLENGE/RESPONSE data must be ${PATH_DATA_LENGTH} bytes, got ${frame.data.length}`)
      }
      buf.set(frame.data, offset + 1)
      return 9
    case FrameType.ConnectionClose: {
      const reason = utf8Encoder.encode(frame.reason)
      checkUint(frame.errorCode, U32_MAX, 'CONNECTION_CLOSE errorCode')
      checkUint(frame.frameType, U32_MAX, 'CONNECTION_CLOSE frameType')
      checkUint(reason.length, U16_MAX, 'CONNECTION_CLOSE reason length')
      dv.setUint32(offset + 1, frame.errorCode)
      dv.setUint32(offset + 5, frame.frameType)
      dv.setUint16(offset + 9, reason.length)
      buf.set(reason, offset + 11)
      return 11 + reason.length
    }
    case FrameType.StopSending:
      checkUint(frame.streamId, U32_MAX, 'STOP_SENDING streamId')
      checkUint(frame.errorCode, U32_MAX, 'STOP_SENDING errorCode')
      dv.setUint32(offset + 1, frame.streamId)
      dv.setUint32(offset + 5, frame.errorCode)
      return 9
    case FrameType.StreamDataBlocked:
      checkUint(frame.streamId, U32_MAX, 'STREAM_DATA_BLOCKED streamId')
      checkUint(frame.limit, U64_MAX_SAFE, 'STREAM_DATA_BLOCKED limit')
      dv.setUint32(offset + 1, frame.streamId)
      writeU64(dv, offset + 5, frame.limit)
      return 13
    case FrameType.NewConnectionId: {
      if (frame.resetToken.length !== STATELESS_RESET_TOKEN_LENGTH) {
        throw new CodecError('value-out-of-range', `NEW_CONNECTION_ID reset token must be ${STATELESS_RESET_TOKEN_LENGTH} bytes, got ${frame.resetToken.length}`)
      }
      checkUint(frame.sequence, U64_MAX_SAFE, 'NEW_CONNECTION_ID sequence')
      checkUint(frame.retirePriorTo, U64_MAX_SAFE, 'NEW_CONNECTION_ID retirePriorTo')
      writeU64(dv, offset + 1, frame.sequence)
      writeU64(dv, offset + 9, frame.retirePriorTo)
      const cid = frame.connectionId.bytes
      buf[offset + 17] = cid.length
      buf.set(cid, offset + 18)
      buf.set(frame.resetToken, offset + 18 + cid.length)
      return 18 + cid.length + STATELESS_RESET_TOKEN_LENGTH
    }
  }
}

function writeU32Frame (dv: DataView, offset: number, value: number, what: string): number {
  checkUint(value, U32_MAX, what)
  dv.setUint32(offset + 1, value)
  return 5
}

function writeU64Frame (dv: DataView, offset: number, value: number, what: string): number {
  checkUint(value, U64_MAX_SAFE, what)
  writeU64(dv, offset + 1, value)
  return 9
}

/** Encodes a single frame into a new buffer. */
export function encodeFrameToBytes (frame: Frame): Uint8Array {
  const buf = new Uint8Array(frameLength(frame))
  encodeFrame(frame, buf, 0)
  return buf
}

function tooShort (what: string): CodecError {
  return new CodecError('too-short', `packet too short: ${what}`)
}

/**
 * Decodes one frame at `offset`. Returns the frame and the bytes consumed.
 *
 * STREAM data, PATH data and reset tokens are views into `data`, not copies;
 * callers that keep them past the life of the datagram buffer must copy.
 */
export function decodeFrame (data: Uint8Array, offset: number): [Frame, number] {
  if (offset >= data.length) throw tooShort('no data for frame')
  const dv = view(data)
  const type = data[offset] as number
  const need = (n: number, what: string): void => {
    if (offset + n > data.length) throw tooShort(what)
  }

  switch (type) {
    case FrameType.Padding:
      return [{ type: FrameType.Padding }, 1]
    case FrameType.Ping:
      return [{ type: FrameType.Ping }, 1]
    case FrameType.Ack: {
      need(12, 'ACK frame')
      const largestAcked = dv.getUint32(offset + 1)
      const ackDelay = dv.getUint16(offset + 5)
      const rangeCount = data[offset + 7] as number
      const firstAckRangeLength = dv.getUint32(offset + 8)
      need(12 + rangeCount * 5, 'ACK frame ranges')
      const ranges: AckRange[] = new Array(rangeCount)
      let p = offset + 12
      for (let i = 0; i < rangeCount; i++) {
        ranges[i] = { gap: data[p] as number, length: dv.getUint32(p + 1) }
        p += 5
      }
      return [{ type: FrameType.Ack, largestAcked, ackDelay, firstAckRangeLength, ranges }, p - offset]
    }
    case FrameType.Stream: {
      need(STREAM_FRAME_HEADER_LENGTH, 'STREAM frame header')
      const flags = data[offset + 1] as number
      const streamOffset = readU64(dv, offset + 2, 'STREAM offset')
      const len = dv.getUint16(offset + 10)
      need(STREAM_FRAME_HEADER_LENGTH + len, 'STREAM frame data')
      const start = offset + STREAM_FRAME_HEADER_LENGTH
      return [{
        type: FrameType.Stream,
        fin: (flags & STREAM_FLAG_FIN) !== 0,
        syn: (flags & STREAM_FLAG_SYN) !== 0,
        offset: streamOffset,
        data: data.subarray(start, start + len)
      }, STREAM_FRAME_HEADER_LENGTH + len]
    }
    case FrameType.WindowUpdate:
      need(5, 'WINDOW_UPDATE frame')
      return [{ type: FrameType.WindowUpdate, limit: dv.getUint32(offset + 1) }, 5]
    case FrameType.MaxData:
      need(9, 'MAX_DATA frame')
      return [{ type: FrameType.MaxData, maxData: readU64(dv, offset + 1, 'MAX_DATA') }, 9]
    case FrameType.ResetStream:
      need(5, 'RESET_STREAM frame')
      return [{ type: FrameType.ResetStream, errorCode: dv.getUint32(offset + 1) }, 5]
    case FrameType.MaxStreams:
      need(5, 'MAX_STREAMS frame')
      return [{ type: FrameType.MaxStreams, maxStreams: dv.getUint32(offset + 1) }, 5]
    case FrameType.MtuProbe: {
      const size = data.length - offset
      return [{ type: FrameType.MtuProbe, size }, size]
    }
    case FrameType.PathChallenge:
      need(9, 'PATH_CHALLENGE frame')
      return [{ type: FrameType.PathChallenge, data: data.subarray(offset + 1, offset + 9) }, 9]
    case FrameType.PathResponse:
      need(9, 'PATH_RESPONSE frame')
      return [{ type: FrameType.PathResponse, data: data.subarray(offset + 1, offset + 9) }, 9]
    case FrameType.ConnectionClose: {
      need(11, 'CONNECTION_CLOSE frame header')
      const errorCode = dv.getUint32(offset + 1)
      const frameType = dv.getUint32(offset + 5)
      const reasonLen = dv.getUint16(offset + 9)
      need(11 + reasonLen, 'CONNECTION_CLOSE frame reason')
      const reason = utf8Decoder.decode(data.subarray(offset + 11, offset + 11 + reasonLen))
      return [{ type: FrameType.ConnectionClose, errorCode, frameType, reason }, 11 + reasonLen]
    }
    case FrameType.StopSending:
      need(9, 'STOP_SENDING frame')
      return [{
        type: FrameType.StopSending,
        streamId: dv.getUint32(offset + 1),
        errorCode: dv.getUint32(offset + 5)
      }, 9]
    case FrameType.DataBlocked:
      need(9, 'DATA_BLOCKED frame')
      return [{ type: FrameType.DataBlocked, limit: readU64(dv, offset + 1, 'DATA_BLOCKED') }, 9]
    case FrameType.StreamDataBlocked:
      need(13, 'STREAM_DATA_BLOCKED frame')
      return [{
        type: FrameType.StreamDataBlocked,
        streamId: dv.getUint32(offset + 1),
        limit: readU64(dv, offset + 5, 'STREAM_DATA_BLOCKED')
      }, 13]
    case FrameType.NewConnectionId: {
      need(18, 'NEW_CONNECTION_ID frame header')
      const sequence = readU64(dv, offset + 1, 'NEW_CONNECTION_ID sequence')
      const retirePriorTo = readU64(dv, offset + 9, 'NEW_CONNECTION_ID retirePriorTo')
      const cidLen = data[offset + 17] as number
      // go-udx reports an over-long CID here as a short packet; either way the packet is rejected.
      if (cidLen > MAX_CID_LENGTH) throw tooShort('NEW_CONNECTION_ID frame CID/token')
      need(18 + cidLen + STATELESS_RESET_TOKEN_LENGTH, 'NEW_CONNECTION_ID frame CID/token')
      const cidStart = offset + 18
      const tokenStart = cidStart + cidLen
      return [{
        type: FrameType.NewConnectionId,
        sequence,
        retirePriorTo,
        connectionId: new ConnectionId(data.subarray(cidStart, tokenStart)),
        resetToken: data.subarray(tokenStart, tokenStart + STATELESS_RESET_TOKEN_LENGTH)
      }, 18 + cidLen + STATELESS_RESET_TOKEN_LENGTH]
    }
    case FrameType.RetireConnectionId:
      need(9, 'RETIRE_CONNECTION_ID frame')
      return [{ type: FrameType.RetireConnectionId, sequence: readU64(dv, offset + 1, 'RETIRE_CONNECTION_ID sequence') }, 9]
    default:
      throw new CodecError('unknown-frame-type', `unknown frame type 0x${type.toString(16).padStart(2, '0')}`)
  }
}
