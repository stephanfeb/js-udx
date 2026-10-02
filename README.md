# js-udx

TypeScript port of the UDX protocol (wire v3), a QUIC-inspired reliable UDP
transport, plus a js-libp2p transport built on it. Node.js only. Browsers have
no raw UDP.

| Package | Purpose |
|---|---|
| [`packages/udx`](packages/udx) | The UDX protocol: packets, reliability, congestion and flow control, connections and streams. No libp2p dependency. |
| [`packages/libp2p-udx`](packages/libp2p-udx) | js-libp2p transport for `/ip4\|ip6/<ip>/udp/<port>/udx`. |

Interoperates with [go-udx](../go-udx) and [dart-udx](../dart-udx). go-udx is the wire
authority. See [PLAN.md](PLAN.md) for the porting plan and the protocol rules.

## Development

```sh
npm install
npm test           # vitest, runs against sources
npm run build      # tsc -b, both packages
npm run ci         # typecheck + build + lint + test
```

Issues are tracked with beads (`bd ready`).

### Wire conformance vectors

`packages/udx/test/vectors/go-v3.json` is generated from go-udx (expected at
`../go-udx`) by `tools/gen-vectors`. The codec tests require byte-identical
encoding and the same accept/reject verdict on truncated and fuzzed input.
Regenerate after a go-udx wire change with `npm run vectors` (needs Go).
