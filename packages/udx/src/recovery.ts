import type { Clock } from './clock.js'
import { CongestionController } from './congestion.js'
import { BLACKHOLE_PTOS, PTO_PROBES } from './constants.js'
import type { AckFrame, Frame } from './frames.js'
import type { MtuDiscovery } from './pmtud.js'
import { type SentPacket, SentPacketManager } from './sent-packets.js'

export interface LossRecoveryOptions {
  clock: Clock
  /**
   * Puts a PTO probe back on the wire under its fresh `sequence`. Encode
   * `packet.frames` with `packet.destinationStreamId`/`sourceStreamId`.
   */
  retransmit: (packet: SentPacket, sequence: number) => void
  /** Lost packets were queued for re-sending: the connection should flush. */
  onLostQueued?: () => void
  /** Path MTU discovery, told of every acknowledged and lost packet. */
  mtu?: MtuDiscovery
  /**
   * Re-chunks a packet larger than the path now carries into packets that
   * fit, dropping the original from tracking; a packet that fits comes back
   * alone. Default: as is.
   */
  fitToPath?: (packet: SentPacket) => SentPacket[]
}

export interface DataPacketInfo {
  sequence: number
  size: number
  frames: Frame[]
  destinationStreamId: number
  sourceStreamId: number
}

/**
 * The sender half of reliability, wired as go-udx v0.3.0's Connection wires
 * it (handleAckFrame, onPacketsLost, onPTO, flushLost): sequence allocation,
 * the sent-packet manager and the congestion controller.
 *
 * An ACK books what it acknowledged, then loss detection runs over everything
 * outstanding. Lost packets leave flight, contract the window once per
 * epoch, and queue to be re-sent under fresh sequences through the window
 * and the pacer, ahead of new data (the connection's flush drains the queue
 * first). A loss spanning three PTOs with no ACK since the oldest went out is
 * persistent congestion: the window collapses to the minimum. The PTO
 * re-sends the two oldest outstanding packets as probes, their bytes still
 * in flight.
 *
 * Only data-bearing packets (STREAM frames with data, SYN or FIN) and
 * path-MTU probes take a sequence and come through here. Control packets go
 * out with sequence 0 and are never tracked, re-sent or counted in flight.
 */
export class LossRecovery {
  readonly congestion: CongestionController
  readonly sentPackets: SentPacketManager
  private readonly retransmitFn: (packet: SentPacket, sequence: number) => void
  private readonly onLostQueued: (() => void) | undefined
  private readonly mtu: MtuDiscovery | undefined
  private readonly fitToPath: (packet: SentPacket) => SentPacket[]
  private readonly base: number

  /** Lost packets waiting to be re-sent, oldest first. */
  private readonly lost: SentPacket[] = []
  /** Whether the last ACK acknowledged data larger than the base size. */
  private ackedLarge = false

  constructor (opts: LossRecoveryOptions) {
    this.retransmitFn = opts.retransmit
    this.onLostQueued = opts.onLostQueued
    this.mtu = opts.mtu
    this.fitToPath = opts.fitToPath ?? (p => [p])
    this.base = opts.mtu?.base ?? Infinity
    this.congestion = new CongestionController(opts.clock, () => this.sentPackets.lastSentSequence)
    this.congestion.onProbe = () => this.onPto()
    this.sentPackets = new SentPacketManager({
      clock: opts.clock,
      congestion: this.congestion,
      onLost: (lost) => this.onLost(lost)
    })
  }

  /** Allocates the sequence for a packet about to be sent. */
  nextSequence (): number {
    return this.sentPackets.nextSequence()
  }

  /** Records a data-bearing packet that was just sent: tracked for ACK, charged to the window, timed by the PTO. */
  onDataPacketSent (info: DataPacketInfo): void {
    this.sentPackets.onPacketSent(info)
    this.congestion.onPacketSent(info.size)
  }

  /** Records a path-MTU probe that was just sent: tracked for its ACK or loss, nothing more. */
  onProbeSent (info: DataPacketInfo): void {
    this.sentPackets.onPacketSent({ ...info, probe: true })
  }

  /** How many lost packets wait to be re-sent. */
  get lostCount (): number {
    return this.lost.length
  }

  /**
   * Processes a received ACK frame. Returns the data bytes it newly
   * acknowledged, so the caller can wake writers waiting for window.
   */
  onAckFrame (frame: AckFrame): number {
    const acked = this.sentPackets.onAckFrame(frame)
    if (acked.length === 0) return 0

    // One controller update per frame with every byte newly acknowledged
    // (RFC 9002 §7.3.1). The frame's largest, if newly acknowledged,
    // supplies the RTT sample (§5.1), a probe's as good as any.
    let bytes = 0
    let largest: SentPacket | undefined
    let large = false
    for (const pkt of acked) {
      if (pkt.sequence === frame.largestAcked) largest = pkt
      if (pkt.probe) {
        this.mtu?.onProbeAcked(pkt.size)
        continue
      }
      bytes += pkt.size
      if (pkt.size > this.base) {
        large = true
        this.mtu?.onDataAcked(pkt.size)
      }
    }
    this.ackedLarge = large
    this.congestion.onPacketsAcked(bytes, largest?.sentTime, frame.ackDelay, frame.largestAcked)

    // Then loss detection over everything outstanding (§6.1, after the ACK
    // is booked: appendix A.7).
    this.sentPackets.detectLost()
    return bytes
  }

  /**
   * What loss detection declared lost, oldest first. A lost probe says its
   * size does not cross the path, and no more. Lost data larger than the base
   * counts towards a black hole, unless the ACK that revealed the loss
   * acknowledged full-size data too: then the path carries the size and the
   * loss is congestion.
   */
  private onLost (lost: SentPacket[]): void {
    const data: SentPacket[] = []
    const countLarge = !this.ackedLarge
    for (const pkt of lost) {
      if (pkt.probe) {
        this.mtu?.onProbeLost(pkt.size)
        continue
      }
      if (countLarge) this.mtu?.onDataLost(pkt.size)
      data.push(pkt)
    }
    if (data.length === 0) return

    let bytes = 0
    for (const pkt of data) bytes += pkt.size
    const cc = this.congestion
    cc.onPacketLost(bytes)

    const oldest = data[0] as SentPacket
    const newest = data[data.length - 1] as SentPacket
    const ackBefore = this.sentPackets.ackBeforeLoss
    if (cc.hasRttSample &&
      newest.sentTime - oldest.sentTime >= cc.persistentCongestionDuration() &&
      (ackBefore === undefined || ackBefore < oldest.sentTime)) {
      cc.onPersistentCongestion()
    }

    this.lost.push(...data)
    this.onLostQueued?.()
  }

  /**
   * The probe timeout (RFC 9002 §6.2.4): the oldest outstanding packets go
   * out again under fresh sequences, up to PTO_PROBES of them. They are not
   * declared lost (an ACK will say), their bytes stay in flight, and the
   * window and pacer are not consulted (§7.7 exempts probes), though they
   * are drawn from the pacing budget.
   *
   * BLACKHOLE_PTOS expirations in a row with nothing acknowledged is read as
   * the path having stopped carrying the validated size: it falls back to
   * the base, and the probes, like every re-send after, are re-chunked to
   * fit, so they are the first packets that can get through.
   */
  private onPto (): void {
    if (this.congestion.ptoCount >= BLACKHOLE_PTOS) this.mtu?.blackhole()
    for (const pkt of this.sentPackets.oldestUnacked(PTO_PROBES)) {
      for (const piece of this.fitToPath(pkt)) {
        const seq = this.sentPackets.resend(piece)
        if (seq === undefined) continue
        this.retransmitFn(piece, seq)
        this.congestion.pacer.onPacketSent(piece.size)
      }
    }
  }

  /** The next lost packet to re-send, or undefined; acknowledged ones are skipped. */
  nextLost (): SentPacket | undefined {
    while (this.lost.length > 0 && (this.lost[0] as SentPacket).acked) this.lost.shift()
    return this.lost[0]
  }

  /** Replaces the next lost packet with the pieces it was re-chunked into. */
  replaceNextLost (pieces: SentPacket[]): void {
    this.lost.splice(0, 1, ...pieces)
  }

  /**
   * Takes the next lost packet off the queue under a fresh sequence, booked
   * in flight, and returns the sequence; undefined if it must not go out.
   * The caller has checked the window and the pacer, and sends it.
   */
  resendNextLost (): number | undefined {
    const pkt = this.lost.shift()
    if (pkt === undefined) return undefined
    const seq = this.sentPackets.resend(pkt)
    if (seq === undefined) return undefined
    this.congestion.onPacketSent(pkt.size)
    return seq
  }

  destroy (): void {
    this.congestion.destroy()
    this.sentPackets.destroy()
    this.lost.length = 0
  }
}
