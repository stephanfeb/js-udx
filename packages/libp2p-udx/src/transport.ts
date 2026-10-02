import { AbortError, InvalidParametersError, serviceCapabilities, transportSymbol } from '@libp2p/interface'
import { UdxMultiplexer, type UdxConnection, type UdxMultiplexerOptions } from '@stephanfeb/udx'
import { UdxMultiaddrConnection } from './connection.js'
import { UdxListener } from './listener.js'
import { isUdxMultiaddr, parseUdxMultiaddr } from './multiaddr.js'
import type { ComponentLogger, Connection, CreateListenerOptions, DialTransportOptions, Listener, Logger, Startable, Transport } from '@libp2p/interface'
import type { Multiaddr } from '@multiformats/multiaddr'

export interface UdxTransportInit {
  /**
   * Ping a connection after this many milliseconds without hearing from the
   * peer, so it outlives UDX's 30 s idle timeout at both ends while libp2p
   * keeps it open. Default 10 000; 0 disables, leaving idle connections to
   * time out unless something above (Yamux keep-alive) sends often enough.
   */
  keepAliveInterval?: number
  /** How long a graceful close waits for the peer to acknowledge our data. Default 5000 ms. */
  closeTimeout?: number
  /** Passed to every MultiaddrConnection; see `MessageStream.inactivityTimeout`. */
  inactivityTimeout?: number
  /** Options for every UDX multiplexer the transport creates. */
  udx?: Omit<UdxMultiplexerOptions, 'keepAliveInterval'>
}

export interface UdxComponents {
  logger: ComponentLogger
}

const DEFAULT_KEEP_ALIVE_INTERVAL = 10_000
const DEFAULT_CLOSE_TIMEOUT = 5_000

/**
 * libp2p over UDX: `/ip4|ip6/<host>/udp/<port>/udx`. Each libp2p connection
 * is one UDX connection whose first stream is upgraded with the configured
 * encrypter and muxer (Noise and Yamux, as go-libp2p-udx-transport and
 * dart-libp2p use). Dials share one socket per address family, like Go's.
 */
export class UdxTransport implements Transport, Startable {
  readonly [transportSymbol] = true
  readonly [Symbol.toStringTag] = '@stephanfeb/libp2p-udx'
  readonly [serviceCapabilities]: string[] = ['@libp2p/transport']

  private readonly components: UdxComponents
  private readonly log: Logger
  private readonly udxOptions: UdxMultiplexerOptions
  private readonly closeTimeout: number
  private readonly inactivityTimeout: number | undefined
  private readonly outbound = new Map<4 | 6, Promise<UdxMultiplexer>>()

  constructor (components: UdxComponents, init: UdxTransportInit = {}) {
    this.components = components
    this.log = components.logger.forComponent('libp2p:udx')
    this.udxOptions = { ...init.udx, keepAliveInterval: init.keepAliveInterval ?? DEFAULT_KEEP_ALIVE_INTERVAL }
    this.closeTimeout = init.closeTimeout ?? DEFAULT_CLOSE_TIMEOUT
    this.inactivityTimeout = init.inactivityTimeout
  }

  start (): void {}

  /** Closes the shared dial sockets, and with them every outbound connection. */
  async stop (): Promise<void> {
    const muxes = [...this.outbound.values()]
    this.outbound.clear()
    await Promise.all(muxes.map(async m => { await (await m).close() }))
  }

  async dial (ma: Multiaddr, options: DialTransportOptions): Promise<Connection> {
    options.signal.throwIfAborted()
    const addr = parseUdxMultiaddr(ma)
    if (addr === undefined) throw new InvalidParametersError(`not a UDX address: ${ma.toString()}`)

    const mux = await this.outboundMux(addr.family)
    this.log('dialing %a', ma)
    const conn = mux.dial(addr.port, addr.host)
    try {
      await established(conn, options.signal)
    } catch (err) {
      conn.close()
      throw err
    }

    const maConn = new UdxMultiaddrConnection({
      stream: conn.openStream(),
      connection: conn,
      remoteAddr: ma,
      direction: 'outbound',
      inactivityTimeout: this.inactivityTimeout,
      closeTimeout: this.closeTimeout,
      log: this.components.logger.forComponent('libp2p:udx:connection')
    })
    try {
      return await options.upgrader.upgradeOutbound(maConn, options)
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e))
      this.log.error('error upgrading outbound connection - %e', err)
      maConn.abort(err)
      throw err
    }
  }

  createListener (options: CreateListenerOptions): Listener {
    return new UdxListener({
      upgrader: options.upgrader,
      logger: this.components.logger,
      udx: this.udxOptions,
      closeTimeout: this.closeTimeout,
      inactivityTimeout: this.inactivityTimeout
    })
  }

  listenFilter (multiaddrs: Multiaddr[]): Multiaddr[] {
    return multiaddrs.filter(isUdxMultiaddr)
  }

  dialFilter (multiaddrs: Multiaddr[]): Multiaddr[] {
    return multiaddrs.filter(isUdxMultiaddr)
  }

  private async outboundMux (family: 4 | 6): Promise<UdxMultiplexer> {
    let mux = this.outbound.get(family)
    if (mux === undefined) {
      mux = UdxMultiplexer.create({ ...this.udxOptions, type: family === 6 ? 'udp6' : 'udp4' })
      mux.catch(() => { this.outbound.delete(family) })
      this.outbound.set(family, mux)
    }
    return await mux
  }
}

/** Resolves when the peer first answers; rejects if the dial is aborted or the connection closes first. */
async function established (conn: UdxConnection, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onAbort = (): void => { done(new AbortError('dial aborted')) }
    const onClose = (err?: Error): void => { done(err ?? new Error('connection closed before it was established')) }
    const done = (err?: Error): void => {
      signal.removeEventListener('abort', onAbort)
      conn.off('close', onClose)
      if (err !== undefined) reject(err)
      else resolve()
    }
    signal.addEventListener('abort', onAbort, { once: true })
    conn.once('close', onClose)
    void conn.established.then(() => { done() })
  })
}
