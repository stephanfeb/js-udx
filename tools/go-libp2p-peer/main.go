// Command go-libp2p-peer is a go-libp2p host on go-libp2p-udx-transport, for
// js-udx's libp2p interop tests. Noise and Yamux, as dart-libp2p's interop
// go-peer and go-libp2p-udx-transport's own tests configure it.
//
//	go-libp2p-peer listen                  echo /echo/1.0.0 streams; prints "READY <multiaddr>/p2p/<id>" on stderr
//	go-libp2p-peer dial <multiaddr> <bytes>
//	    connect, ping, wait for identify, echo <bytes> on /echo/1.0.0; prints
//	    "PING <ms>", "PROTOCOLS <ids…>", "AGENT <version>", then "RESULT <bytes>"
//	    or "CORRUPT <detail>" on stderr. It reads exactly <bytes> back, not to EOF.
//
// The payload pattern is go-udx's interop pattern, byte(i*31 + i/251).
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/multiformats/go-multiaddr"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"
)

const echoProtocol = "/echo/1.0.0"

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func newHost(listen bool) host.Host {
	opts := []libp2p.Option{
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.DisableRelay(),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	}
	if listen {
		opts = append(opts, libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/udx"))
	} else {
		opts = append(opts, libp2p.NoListenAddrs)
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		fail("host: %v", err)
	}
	return h
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/251)
	}
	return b
}

func listen() {
	h := newHost(true)
	h.SetStreamHandler(echoProtocol, func(s network.Stream) {
		io.Copy(s, s)
		s.CloseWrite()
	})
	if len(h.Addrs()) == 0 {
		fail("not listening")
	}
	fmt.Fprintf(os.Stderr, "READY %s/p2p/%s\n", h.Addrs()[0], h.ID())
	select {}
}

func dial(target string, size int) {
	ma, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		fail("multiaddr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		fail("addr info: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHost(false)
	defer h.Close()
	if err := h.Connect(ctx, *info); err != nil {
		fail("connect: %v", err)
	}

	res := <-ping.Ping(ctx, h, info.ID)
	if res.Error != nil {
		fail("ping: %v", res.Error)
	}
	fmt.Fprintf(os.Stderr, "PING %d\n", res.RTT.Milliseconds())

	// Identify runs on connect; wait for it to record the peer's protocols.
	if ids, ok := h.(interface{ IDService() identify.IDService }); ok {
		for _, c := range h.Network().ConnsToPeer(info.ID) {
			select {
			case <-ids.IDService().IdentifyWait(c):
			case <-ctx.Done():
				fail("identify: %v", ctx.Err())
			}
		}
	}
	protos, _ := h.Peerstore().GetProtocols(info.ID)
	names := make([]string, len(protos))
	for i, p := range protos {
		names[i] = string(p)
	}
	fmt.Fprintf(os.Stderr, "PROTOCOLS %s\n", strings.Join(names, " "))
	agent, _ := h.Peerstore().Get(info.ID, "AgentVersion")
	fmt.Fprintf(os.Stderr, "AGENT %v\n", agent)

	s, err := h.NewStream(ctx, info.ID, echoProtocol)
	if err != nil {
		fail("open echo stream: %v", err)
	}
	want := pattern(size)
	go func() {
		s.SetWriteDeadline(time.Now().Add(60 * time.Second))
		if _, err := s.Write(want); err != nil {
			fmt.Fprintf(os.Stderr, "CORRUPT write: %v\n", err)
		}
		s.CloseWrite()
	}()
	// Read exactly what was sent rather than to EOF: a dart-libp2p without
	// its WINDOW_UPDATE FIN fix never sees go-yamux's FIN, so its echo never
	// ends its side (jsudx-aof).
	s.SetReadDeadline(time.Now().Add(60 * time.Second))
	got := make([]byte, size)
	n, err := io.ReadFull(s, got)
	got = got[:n]
	if err != nil {
		fail("CORRUPT read: %v after %d bytes", err, len(got))
	}
	if !bytes.Equal(got, want) {
		fail("CORRUPT got %d bytes, want %d (or content differs)", len(got), len(want))
	}
	fmt.Fprintf(os.Stderr, "RESULT %d\n", len(got))
	s.Close()
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: go-libp2p-peer listen | dial <multiaddr> <bytes>")
	}
	switch os.Args[1] {
	case "listen":
		listen()
	case "dial":
		if len(os.Args) != 4 {
			fail("usage: go-libp2p-peer dial <multiaddr> <bytes>")
		}
		size, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fail("bytes: %v", err)
		}
		dial(os.Args[2], size)
	default:
		fail("unknown mode %q", os.Args[1])
	}
}
