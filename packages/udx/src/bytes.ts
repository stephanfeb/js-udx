import { CodecError } from './errors.js'

// Big-endian integer helpers over DataView. u64 values are plain numbers: no
// peer sends anything near 2^53, so values above Number.MAX_SAFE_INTEGER are
// rejected rather than paying for BigInt on every packet.

const TWO_POW_32 = 0x1_0000_0000

export function view (data: Uint8Array): DataView {
  return new DataView(data.buffer, data.byteOffset, data.byteLength)
}

export function readU64 (dv: DataView, offset: number, what: string): number {
  const hi = dv.getUint32(offset)
  const lo = dv.getUint32(offset + 4)
  if (hi > 0x1F_FFFF) {
    throw new CodecError('value-out-of-range', `${what}: u64 exceeds 2^53-1`)
  }
  return hi * TWO_POW_32 + lo
}

export function writeU64 (dv: DataView, offset: number, value: number): void {
  const hi = Math.floor(value / TWO_POW_32)
  dv.setUint32(offset, hi)
  dv.setUint32(offset + 4, value - hi * TWO_POW_32)
}

export function checkUint (value: number, max: number, what: string): void {
  if (!Number.isInteger(value) || value < 0 || value > max) {
    throw new CodecError('value-out-of-range', `${what}: ${value} not in 0..${max}`)
  }
}

export const U8_MAX = 0xFF
export const U16_MAX = 0xFFFF
export const U32_MAX = 0xFFFF_FFFF
export const U64_MAX_SAFE = Number.MAX_SAFE_INTEGER

export function toHex (data: Uint8Array): string {
  let s = ''
  for (const b of data) s += b.toString(16).padStart(2, '0')
  return s
}

export function fromHex (hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || /[^0-9a-f]/i.test(hex)) {
    throw new TypeError(`invalid hex string: ${hex}`)
  }
  const out = new Uint8Array(hex.length / 2)
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  return out
}
