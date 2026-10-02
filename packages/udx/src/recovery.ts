import type { Clock } from './clock.js'
import { CongestionController } from './congestion.js'
import type { AckFrame, Frame } from './frames.js'
import { type SentPacket, SentPacketManager } from './sent-packets.js'

export interface LossRecoveryOptions {
  clock: Clock
  /**
   * Puts a packet back on the wire under its fresh `sequence`, whether the
   * RTO timer or SACK loss detection triggered it. Encode `packet.frames`
   * with `packet.destinationStreamId`/`sourceStreamId`.
   */
  retransmit: (packet: SentPacket, sequence: number) => void
}

export interface DataPacketInfo {
  sequence: number
  size: number
  frames: Frame[]
  destinationStreamId: number
  sourceStreamId: number
}

/**
 * The sender half of reliability: sequence allocation, the sent-packet
 * manager and the congestion controller, wired as go-udx's Connection wires
 * them (sendPacket, handleAckFrame).
 *
 * Only data-bearing packets (STREAM frames with data, SYN or FIN) take a
 * sequence and come through here. Control packets go out with sequence 0 and
 * are never tracked, retransmitted or counted in flight.
 */
export class LossRecovery {
  readonly congestion: CongestionController
  readonly sentPackets: SentPacketManager
  private readonly retransmitFn: (packet: SentPacket, sequence: number) => void

  constructor (opts: LossRecoveryOptions) {
    this.retransmitFn = opts.retransmit
    this.congestion = new CongestionController(opts.clock, () => this.sentPackets.lastSentSequence)
    this.sentPackets = new SentPacketManager({
      clock: opts.clock,
      congestion: this.congestion,
      onRetransmit: (pkt, seq) => this.retransmitFn(pkt, seq)
    })
  }

  /** Allocates the sequence for a data-bearing packet about to be sent. */
  nextSequence (): number {
    return this.sentPackets.nextSequence()
  }

  /** Records a data-bearing packet that was just sent: tracked for ACK, charged to the window. */
  onDataPacketSent (info: DataPacketInfo): void {
    this.sentPackets.onPacketSent(info)
    this.congestion.onPacketSent(info.size)
  }

  /**
   * Processes a received ACK frame. Returns the bytes it newly acknowledged,
   * so the caller can wake writers waiting for window.
   */
  onAckFrame (frame: AckFrame): number {
    const acked = this.sentPackets.onAckFrame(frame)
    let bytes = 0
    if (acked.length > 0) {
      // One controller update per frame with every byte newly acknowledged
      // (RFC 9002 §7.3.1). The frame's largest, if newly acknowledged,
      // supplies the RTT sample (§5.1).
      let largest: SentPacket | undefined
      for (const pkt of acked) {
        bytes += pkt.size
        if (pkt.sequence === frame.largestAcked) largest = pkt
      }
      this.congestion.onPacketsAcked(bytes, largest?.sentTime, frame.ackDelay, frame.largestAcked)
    }

    // SACK loss: contract the window once per epoch and resend under a fresh
    // sequence. The bytes stay in flight; they are being recovered, not abandoned.
    for (const pkt of this.sentPackets.detectLost(frame)) {
      this.congestion.onCongestionEvent()
      const seq = this.sentPackets.retransmit(pkt)
      if (seq !== undefined) this.retransmitFn(pkt, seq)
    }
    return bytes
  }

  destroy (): void {
    this.sentPackets.destroy()
  }
}
