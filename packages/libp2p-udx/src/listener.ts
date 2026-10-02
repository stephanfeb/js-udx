import { AlreadyStartedError, InvalidParametersError } from '@libp2p/interface'
import { getThinWaistAddresses } from '@libp2p/utils'
import { UdxMultiplexer, type UdxConnection, type UdxMultiplexerOptions } from '@stephanfeb/udx'
import { TypedEventEmitter } from 'main-event'
import { UdxMultiaddrConnection } from './connection.js'
import { parseUdxMultiaddr, toUdxMultiaddr } from './multiaddr.js'
import type { ComponentLogger, Listener, ListenerEvents, Logger, Upgrader } from '@libp2p/interface'
import type { Multiaddr } from '@multiformats/multiaddr'

export interface UdxListenerInit {
  upgrader: Upgrader
  logger: ComponentLogger
  udx: UdxMultiplexerOptions
  closeTimeout: number
  inactivityTimeout?: number
}

/**
 * Accepts UDX connections on one UDP socket. Each connection's first stream
 * is handed to the upgrader, as go-libp2p-udx-transport's listener does.
 */
export class UdxListener extends TypedEventEmitter<ListenerEvents> implements Listener {
  private readonly init: UdxListenerInit
  private readonly log: Logger
  private readonly shutdown = new AbortController()
  private mux: UdxMultiplexer | undefined
  private listeningAddr: Multiaddr | undefined
  private readonly connections = new Set<UdxConnection>()

  constructor (init: UdxListenerInit) {
    super()
    this.init = init
    this.log = init.logger.forComponent('libp2p:udx:listener')
  }

  async listen (ma: Multiaddr): Promise<void> {
    if (this.mux !== undefined) throw new AlreadyStartedError('already listening')
    const addr = parseUdxMultiaddr(ma)
    if (addr === undefined) throw new InvalidParametersError(`not a UDX address: ${ma.toString()}`)
    const mux = await UdxMultiplexer.create({
      ...this.init.udx,
      host: addr.host,
      port: addr.port,
      type: addr.family === 6 ? 'udp6' : 'udp4'
    })
    this.mux = mux
    this.listeningAddr = ma
    mux.on('connection', conn => { this.onConnection(conn) })
    this.log('listening on %s:%d', mux.address().address, mux.address().port)
    this.safeDispatchEvent('listening')
  }

  private onConnection (conn: UdxConnection): void {
    this.connections.add(conn)
    conn.once('close', () => this.connections.delete(conn))
    const remoteAddr = toUdxMultiaddr(conn.remoteAddress, conn.remotePort)
    const signal = this.init.upgrader.createInboundAbortSignal(this.shutdown.signal)
    const onAbort = (): void => { conn.close() }
    signal.addEventListener('abort', onAbort, { once: true })

    void (async () => {
      let maConn: UdxMultiaddrConnection | undefined
      try {
        const stream = await conn.acceptStream()
        maConn = new UdxMultiaddrConnection({
          stream,
          connection: conn,
          remoteAddr,
          direction: 'inbound',
          localAddr: this.listeningAddr,
          inactivityTimeout: this.init.inactivityTimeout,
          closeTimeout: this.init.closeTimeout,
          log: this.init.logger.forComponent('libp2p:udx:connection')
        })
        this.log('new inbound connection %a', remoteAddr)
        await this.init.upgrader.upgradeInbound(maConn, { signal })
      } catch (e) {
        const err = e instanceof Error ? e : new Error(String(e))
        this.log.error('inbound connection from %a failed - %e', remoteAddr, err)
        if (maConn !== undefined) maConn.abort(err)
        else conn.close()
      } finally {
        signal.removeEventListener('abort', onAbort)
        signal.clear()
      }
    })()
  }

  getAddrs (): Multiaddr[] {
    if (this.mux === undefined || this.listeningAddr === undefined) return []
    return getThinWaistAddresses(this.listeningAddr, this.mux.address().port)
      .map(ma => ma.encapsulate('/udx'))
  }

  updateAnnounceAddrs (): void {}

  async close (): Promise<void> {
    const mux = this.mux
    if (mux === undefined) return
    this.mux = undefined
    this.shutdown.abort()
    await mux.close()
    this.safeDispatchEvent('close')
  }
}
