import { randomFillSync } from 'node:crypto'
import { toHex } from './bytes.js'
import { DEFAULT_CID_LENGTH, MAX_CID_LENGTH, MIN_CID_LENGTH } from './constants.js'
import { CodecError } from './errors.js'

/** A UDX connection ID: 0–20 opaque bytes. Immutable; `key` is its hex form, for map lookups. */
export class ConnectionId {
  readonly bytes: Uint8Array
  readonly key: string

  constructor (bytes: Uint8Array) {
    if (bytes.length < MIN_CID_LENGTH || bytes.length > MAX_CID_LENGTH) {
      throw new CodecError('invalid-cid-length', `connection ID length ${bytes.length} not in ${MIN_CID_LENGTH}..${MAX_CID_LENGTH}`)
    }
    this.bytes = Uint8Array.from(bytes)
    this.key = toHex(this.bytes)
  }

  static random (length: number = DEFAULT_CID_LENGTH): ConnectionId {
    if (!Number.isInteger(length) || length < MIN_CID_LENGTH || length > MAX_CID_LENGTH) {
      throw new CodecError('invalid-cid-length', `connection ID length ${length} not in ${MIN_CID_LENGTH}..${MAX_CID_LENGTH}`)
    }
    return new ConnectionId(randomFillSync(new Uint8Array(length)))
  }

  static readonly EMPTY = new ConnectionId(new Uint8Array(0))

  get length (): number {
    return this.bytes.length
  }

  isEmpty (): boolean {
    return this.bytes.length === 0
  }

  equals (other: ConnectionId): boolean {
    return this.key === other.key
  }

  toString (): string {
    return `CID(${this.key})`
  }
}
