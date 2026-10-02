import { describe, expect, it } from 'vitest'
import {
  CodecError,
  ConnectionId,
  type Frame,
  FrameType,
  type Packet,
  VERSION_CURRENT,
  decodeFrame,
  decodePacket,
  encodeFrameToBytes,
  encodePacket,
  frameLength,
  packetLength
} from '../src/index.js'

const dcid = new ConnectionId(Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8))
const scid = new ConnectionId(Uint8Array.of(9, 10, 11, 12, 13, 14, 15, 16))

function packet (frames: Frame[]): Packet {
  return { version: VERSION_CURRENT, destinationCid: dcid, sourceCid: scid, sequence: 1, destinationStreamId: 2, sourceStreamId: 3, frames }
}

function codecErrorKind (fn: () => unknown): string | undefined {
  try {
    fn()
  } catch (err) {
    if (err instanceof CodecError) return err.kind
    throw err
  }
  return undefined
}

describe('encoder range checks', () => {
  const cases: Array<[string, Frame]> = [
    ['STREAM data over 65535 bytes', { type: FrameType.Stream, fin: false, syn: false, offset: 0, data: new Uint8Array(65536) }],
    ['STREAM offset above 2^53-1', { type: FrameType.Stream, fin: false, syn: false, offset: 2 ** 53, data: new Uint8Array(0) }],
    ['negative STREAM offset', { type: FrameType.Stream, fin: false, syn: false, offset: -1, data: new Uint8Array(0) }],
    ['fractional ACK largest', { type: FrameType.Ack, largestAcked: 1.5, ackDelay: 0, firstAckRangeLength: 1, ranges: [] }],
    ['ACK delay over u16', { type: FrameType.Ack, largestAcked: 1, ackDelay: 65536, firstAckRangeLength: 1, ranges: [] }],
    ['ACK gap over u8', { type: FrameType.Ack, largestAcked: 1, ackDelay: 0, firstAckRangeLength: 1, ranges: [{ gap: 256, length: 1 }] }],
    ['256 ACK ranges', { type: FrameType.Ack, largestAcked: 1, ackDelay: 0, firstAckRangeLength: 1, ranges: Array.from({ length: 256 }, () => ({ gap: 1, length: 1 })) }],
    ['WINDOW_UPDATE over u32', { type: FrameType.WindowUpdate, limit: 2 ** 32 }],
    ['PATH_CHALLENGE of 7 bytes', { type: FrameType.PathChallenge, data: new Uint8Array(7) }],
    ['reset token of 15 bytes', { type: FrameType.NewConnectionId, sequence: 0, retirePriorTo: 0, connectionId: dcid, resetToken: new Uint8Array(15) }],
    ['close reason over 65535 bytes', { type: FrameType.ConnectionClose, errorCode: 0, frameType: 0, reason: 'x'.repeat(65536) }]
  ]

  it.each(cases)('rejects %s', (_name, frame) => {
    expect(codecErrorKind(() => encodeFrameToBytes(frame))).toBe('value-out-of-range')
  })

  it('rejects header fields over u32', () => {
    expect(codecErrorKind(() => encodePacket({ ...packet([]), sequence: 2 ** 32 }))).toBe('value-out-of-range')
  })
})

describe('decoder', () => {
  it('decodes from a pooled Buffer that starts mid-ArrayBuffer', () => {
    const data = Uint8Array.of(1, 2, 3)
    const bytes = encodePacket(packet([{ type: FrameType.Stream, fin: true, syn: true, offset: 5_000_000_000, data }]))
    const pool = Buffer.alloc(bytes.length + 100, 0xee)
    pool.set(bytes, 37)
    const decoded = decodePacket(pool.subarray(37, 37 + bytes.length))
    expect(decoded.sequence).toBe(1)
    const frame = decoded.frames[0]
    if (frame?.type !== FrameType.Stream) throw new Error('expected STREAM')
    expect({ ...frame, data: Uint8Array.from(frame.data) }).toEqual({ type: FrameType.Stream, fin: true, syn: true, offset: 5_000_000_000, data })
  })

  it('returns STREAM data as a view, not a copy', () => {
    const bytes = encodePacket(packet([{ type: FrameType.Stream, fin: false, syn: false, offset: 0, data: Uint8Array.of(7, 7) }]))
    const frame = decodePacket(bytes).frames[0]
    if (frame?.type !== FrameType.Stream) throw new Error('expected STREAM')
    expect(frame.data.buffer).toBe(bytes.buffer)
  })

  it('rejects a u64 above 2^53-1 rather than lose precision', () => {
    const bytes = encodeFrameToBytes({ type: FrameType.MaxData, maxData: 0 })
    bytes[1] = 0x00
    bytes[2] = 0x20 // 2^53
    expect(codecErrorKind(() => decodeFrame(bytes, 0))).toBe('value-out-of-range')
  })

  it('rejects unknown frame types, including the unused 0x0c', () => {
    for (const t of [0x0c, 0x12, 0xff]) {
      expect(codecErrorKind(() => decodeFrame(Uint8Array.of(t), 0))).toBe('unknown-frame-type')
    }
  })

  it('rejects a CID length over 20', () => {
    const bytes = encodePacket(packet([]))
    bytes[4] = 21
    expect(codecErrorKind(() => decodePacket(bytes))).toBe('invalid-cid-length')
  })

  it('lets an MTU probe consume the rest of the datagram', () => {
    const bytes = encodePacket(packet([{ type: FrameType.Ping }, { type: FrameType.MtuProbe, size: 50 }]))
    expect(bytes.length).toBe(packetLength(packet([])) + 1 + 50)
    expect(decodePacket(bytes).frames).toEqual([{ type: FrameType.Ping }, { type: FrameType.MtuProbe, size: 50 }])
  })

  it('round-trips a multi-frame packet', () => {
    const frames: Frame[] = [
      { type: FrameType.Ack, largestAcked: 10, ackDelay: 2, firstAckRangeLength: 3, ranges: [{ gap: 2, length: 4 }] },
      { type: FrameType.Stream, fin: false, syn: true, offset: 0, data: new TextEncoder().encode('hi') },
      { type: FrameType.ConnectionClose, errorCode: 4, frameType: 3, reason: 'nope ✓' }
    ]
    const bytes = encodePacket(packet(frames))
    expect(bytes.length).toBe(packetLength(packet(frames)))
    expect(decodePacket(bytes)).toEqual(packet(frames))
    expect(frames.reduce((n, f) => n + frameLength(f), 0)).toBe(bytes.length - 34)
  })
})

describe('ConnectionId', () => {
  it('generates 8 random bytes by default', () => {
    const a = ConnectionId.random()
    const b = ConnectionId.random()
    expect(a.length).toBe(8)
    expect(a.equals(b)).toBe(false)
  })

  it('accepts lengths 0..20 only', () => {
    expect(ConnectionId.random(0).isEmpty()).toBe(true)
    expect(ConnectionId.random(20).length).toBe(20)
    expect(codecErrorKind(() => ConnectionId.random(21))).toBe('invalid-cid-length')
    expect(codecErrorKind(() => new ConnectionId(new Uint8Array(21)))).toBe('invalid-cid-length')
  })

  it('copies its input and compares by value', () => {
    const raw = Uint8Array.of(1, 2, 3)
    const cid = new ConnectionId(raw)
    raw[0] = 9
    expect(cid.key).toBe('010203')
    expect(cid.equals(new ConnectionId(Uint8Array.of(1, 2, 3)))).toBe(true)
    expect(cid.toString()).toBe('CID(010203)')
  })
})
