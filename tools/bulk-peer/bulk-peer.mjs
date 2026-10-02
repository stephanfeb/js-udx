#!/usr/bin/env node
// Bulk-transfer js-udx peer for go-udx's interop suite: the same CLI and stderr
// protocol as go-udx/interop/dartpeer/bin/bulk_peer.dart, so the suite can run
// against JS by setting UDX_BULK_PEER (see go-udx/interop/bulk_interop_test.go):
//
//   UDX_BULK_PEER="node $PWD/tools/bulk-peer/bulk-peer.mjs" go test ./interop -run 'TestBulk|TestMultiStream'
//
// Modes:
//   send      <port> <bytes>            connect and upload <bytes>
//   recv      <port> <bytes>            connect and download <bytes>
//   recvslow  <port> <ms>               download while reading slowly for <ms>, then report consumed
//   sendmulti <port> <bytes> <streams>  upload <bytes> on each of <streams> concurrent streams
//   recvmulti <port> <bytes> <streams>  download <bytes> on each of <streams> concurrent streams
//
// Reports on stderr: READY once connected, PROGRESS, STREAM_DONE, WINDOW, a
// final RESULT <bytes>, and CORRUPT if a payload didn't match. The payload is
// byte i == (i*31 + i/251) & 0xff; in the multi modes every 512th byte is
// overwritten with the stream's index, so a receiver can tell streams apart.
// Requires `npm run build`.
import { dial } from '../../packages/udx/dist/index.js'

const [mode, portArg, totalArg, streamsArg] = process.argv.slice(2)
if (mode === undefined || portArg === undefined || totalArg === undefined) {
  process.stderr.write('usage: bulk-peer.mjs <send|recv|recvslow|sendmulti|recvmulti> <port> <bytes> [streams]\n')
  process.exit(2)
}
const port = Number(portArg)
const total = Number(totalArg)
const streamCount = streamsArg !== undefined ? Number(streamsArg) : 1
const log = line => process.stderr.write(`${line}\n`)

function markedPattern (n, offset, marker) {
  const b = new Uint8Array(n)
  for (let i = 0; i < n; i++) {
    const k = offset + i
    b[i] = marker !== undefined && k % 512 === 0 ? marker : (k * 31 + Math.floor(k / 251)) & 0xff
  }
  return b
}

function matches (chunk, offset, marker) {
  const want = markedPattern(chunk.length, offset, marker)
  for (let i = 0; i < chunk.length; i++) if (chunk[i] !== want[i]) return false
  return true
}

/** Writes `bytes` of the pattern in 32 KiB chunks, honouring backpressure. */
async function upload (s, bytes, marker) {
  const chunk = 32 * 1024
  let sent = 0
  while (sent < bytes) {
    const n = Math.min(chunk, bytes - sent)
    if (!s.write(markedPattern(n, sent, marker))) {
      await new Promise((resolve, reject) => {
        const onClose = e => { s.off('drain', onDrain); reject(e ?? new Error('closed before drain')) }
        const onDrain = () => { s.off('close', onClose); resolve() }
        s.once('drain', onDrain)
        s.once('close', onClose)
      })
    }
    sent += n
    if (marker === undefined && sent % (1 << 20) === 0) log(`PROGRESS ${sent}`)
  }
  return sent
}

/**
 * Reads until `bytes` arrive or the stream ends. In the multi modes the marker
 * isn't known until the first byte: identity is proven by the data, not by the
 * order streams were opened.
 */
async function download (s, bytes, multi) {
  let got = 0
  let marker
  let corrupt = false
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out after ${got} bytes`)), 120_000)
    const finish = () => { clearTimeout(timer); resolve() }
    s.on('data', chunk => {
      if (multi && got === 0) marker = chunk[0]
      if (!matches(chunk, got, marker)) corrupt = true
      got += chunk.length
      if (!multi && got % (1 << 20) < chunk.length) log(`PROGRESS ${got}`)
      if (got >= bytes) finish()
    })
    s.on('end', finish)
    s.on('close', e => { if (e !== undefined) { clearTimeout(timer); reject(e) } else finish() })
  })
  return { got, marker, corrupt }
}

let code = 0
const conn = await dial(port, '127.0.0.1')
// Like the Dart peer, open the first stream before the handshake completes.
const streams = [conn.openStream()]
await Promise.race([
  conn.established,
  new Promise((_resolve, reject) => setTimeout(() => reject(new Error('handshake timed out')), 10_000))
])
log('READY')

try {
  if (mode === 'send') {
    const started = Date.now()
    const sent = await upload(streams[0], total)
    log(`RESULT ${sent}`)
    log(`ELAPSED_MS ${Date.now() - started}`)
  } else if (mode === 'recv') {
    const { got, corrupt } = await download(streams[0], total, false)
    log(`RESULT ${got}`)
    if (corrupt) { log('CORRUPT'); code = 1 }
  } else if (mode === 'recvslow') {
    // Under consumption-anchored flow control the sender must stall rather
    // than push its whole payload into our buffer. `total` is milliseconds here.
    const s = streams[0]
    let got = 0
    s.on('data', chunk => {
      got += chunk.length
      s.pause()
      setTimeout(() => s.resume(), 25)
    })
    await new Promise(resolve => setTimeout(resolve, total))
    s.pause()
    log(`RESULT ${got}`)
    log(`WINDOW ${s.recvWindow}`)
  } else if (mode === 'sendmulti' || mode === 'recvmulti') {
    for (let i = 1; i < streamCount; i++) streams.push(conn.openStream())
    let sum = 0
    let corrupt = false
    if (mode === 'sendmulti') {
      await Promise.all(streams.map(async (s, i) => {
        const n = await upload(s, total, i)
        sum += n
        log(`STREAM_DONE ${i} ${n}`)
      }))
    } else {
      const seen = new Set()
      await Promise.all(streams.map(async s => {
        const r = await download(s, total, true)
        sum += r.got
        if (r.corrupt) corrupt = true
        if (r.marker !== undefined && seen.has(r.marker)) {
          corrupt = true
          log(`DUPLICATE_MARKER ${r.marker}`)
        }
        seen.add(r.marker)
        log(`STREAM_DONE ${r.marker} ${r.got}`)
      }))
      if (seen.size !== streamCount) {
        corrupt = true
        log(`MARKERS ${seen.size} want ${streamCount}`)
      }
    }
    log(`RESULT ${sum}`)
    if (corrupt) { log('CORRUPT'); code = 1 }
  } else {
    log(`unknown mode: ${mode}`)
    code = 2
  }
} catch (e) {
  log(`ERROR ${e?.message ?? e}`)
  code = 1
}

// End our side and give the FINs and their ACKs a moment, as the Dart peer
// closes its streams before exiting.
for (const s of streams) s.end()
await new Promise(resolve => setTimeout(resolve, 200))
conn.close()
await new Promise(resolve => setTimeout(resolve, 50))
process.exit(code)
