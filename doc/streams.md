# Streams

A `UdxStream` is an ordered, reliable byte stream in both directions. A
connection carries many of them, and each is reassembled on its own, so a lost
packet only holds up its own stream.

## Opening and accepting

`conn.openStream()` opens a stream; the peer gets it as a `'stream'` event on
its connection, or from `await conn.acceptStream()`. Opening is immediate: a
stream announces itself to the peer at once, even before you write to it.

Either side of a connection can open streams. The peer limits how many of
ours can be open at once: 100 until it says otherwise, and js-udx peers allow
1024. Past the limit, `openStream()` throws a `UdxError` with code
`ErrorCode.StreamLimitError`.

## Writing

`stream.write(bytes)` queues bytes and returns `false` once 256 KiB are queued
unsent. Stop writing then, and carry on after `'drain'`. Without that, the
queue grows as fast as you write, however fast the network is.

<!-- example: examples/backpressure.ts#writer -->
```ts
/** Writes `total` bytes in 64 KiB chunks, honouring backpressure. */
async function send (stream: UdxStream, total: number): Promise<void> {
  const chunk = new Uint8Array(64 * 1024).fill(0x2a)
  for (let sent = 0; sent < total; sent += chunk.length) {
    if (!stream.write(chunk)) await drained(stream)
  }
  stream.end()
}

/** Resolves on 'drain', rejects if the stream closes first. */
async function drained (stream: UdxStream): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onDrain = (): void => { stream.off('close', onClose); resolve() }
    const onClose = (err?: Error): void => { stream.off('drain', onDrain); reject(err ?? new Error('stream closed')) }
    stream.once('drain', onDrain)
    stream.once('close', onClose)
  })
}
```
<!-- /example -->

`write` copies the bytes, so you can reuse the buffer straight away. Set the
threshold with the `streamOptions.writeHighWaterMark` option on the
multiplexer (see [Connections](connections.md#options)).

A connection sends from its streams in turn, one packet each, so one busy
stream doesn't starve the others.

## Reading

There are two ways to read, and both start the stream flowing:

- Iterate it: `for await (const chunk of stream)`. The loop ends when the
  peer ends its side, and throws if the stream is reset or its connection
  fails.
- Listen for `'data'` chunks and the `'end'` event.

A stream starts paused and buffers what arrives until you start reading.

Reading paces the sender. The peer can only send as much as our receive
window allows, and the window reopens only as we consume bytes. If you stop
reading (`stream.pause()`, or a slow loop body), arriving bytes are buffered,
the window closes, and the sender's `write` starts returning `false`.
`resume()` restarts a paused stream.

This reader takes a 10 ms break after every MiB. The sender above slows down
to match:

<!-- example: examples/backpressure.ts#reader -->
```ts
/** Counts what arrives, pausing for 10 ms after every MiB, then ends our side. */
async function receive (stream: UdxStream): Promise<number> {
  let got = 0
  let nextPause = 1024 * 1024
  for await (const chunk of stream) {
    got += chunk.length
    if (got >= nextPause) {
      nextPause += 1024 * 1024
      // While we're not iterating, the stream is paused: arriving bytes are
      // buffered and the sender's window stops reopening.
      await new Promise(resolve => setTimeout(resolve, 10))
    }
  }
  stream.end()
  return got
}
```
<!-- /example -->

The receive window starts at 64 KiB and grows (up to 4 MiB) when the reader
keeps up, so a fast reader on a long path isn't held back by a small window.

## Ending and closing

A stream has two halves, and each side ends its own:

- `stream.end()` sends FIN once everything queued has been sent. You can
  still read.
- The peer's FIN arrives as `'end'` (or as the end of iteration), after
  every byte before it.
- Once both halves have ended, the stream emits `'close'` with no argument.

A FIN only says the peer has finished sending. It doesn't say the peer has
read what you sent. If you need to know that, have the peer tell you. In
this pattern the receiver ends its side after reading the last byte, and the
sender waits for that before it closes the connection:

<!-- example: examples/backpressure.ts#close -->
```ts
// `send` returns once everything is queued, not delivered. The receiver ends
// its side after reading the last byte, so its end tells us it has all of it.
for await (const _chunk of stream) { /* the receiver sends nothing back */ }
// Wait for the peer to acknowledge our last packets (here, the ACK of its
// FIN), then close: CONNECTION_CLOSE discards anything still in flight.
await conn.flushed()
conn.close()
```
<!-- /example -->

This matters because closing a connection throws away everything on its
streams that hasn't been read yet, including bytes the peer already has
(see [Closing](connections.md#closing)).

## Resetting

`stream.reset(code)` aborts both directions at once: queued and buffered bytes
are dropped, and the peer gets RESET_STREAM with your error code (a number
you choose; 0 by default). Both sides' streams close with a
`StreamResetError`, whose `remote` says which side reset:

<!-- example: examples/reset.ts#stream-reset -->
```ts
const stream = conn.openStream()
stream.write(new TextEncoder().encode('may I?'))
try {
  for await (const _chunk of stream) { /* never reached */ }
} catch (err) {
  if (err instanceof StreamResetError) {
    console.log(`stream reset by ${err.remote ? 'peer' : 'us'}, code 0x${err.code.toString(16)}`)
  }
}
```
<!-- /example -->

This prints `stream reset by peer, code 0x42`.

js-udx resets a stream itself, with `ErrorCode.FlowControlError` (3), if the
peer ignores the stream's window and sends more than 8 MiB ahead of the
bytes that have arrived in order.

A `write` after `end()` throws `WriteAfterEndError`. A write after a reset or
a connection failure throws the error the stream closed with.

## Events and properties

| Event | When |
|---|---|
| `'data'` `(chunk)` | Bytes from the peer, in order, while the stream is flowing |
| `'end'` | The peer ended its side and every byte has been emitted |
| `'drain'` | The send queue went back under the high-water mark after `write` returned `false` |
| `'close'` `(error?)` | Both halves are done, or the stream was reset or its connection closed. Emitted once. `error` is unset for a normal finish. |

| Property | Meaning |
|---|---|
| `writableLength` | Bytes queued and not yet sent |
| `bufferedBytes` | Bytes received and not yet read, including out-of-order bytes |
| `recvWindow` | The current receive window |
| `bytesWritten`, `bytesRead` | Totals so far |
| `writableEnded`, `readableEnded` | `end()` was called; the peer's end was reached |
| `isPaused`, `destroyed`, `error` | Flow state; whether the stream closed, and why |
