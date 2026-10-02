// Command go-peer is a go-udx peer for js-udx interop tests.
//
//	go-peer listen [host:port]                      echo every stream; prints "READY <port>" on stderr
//	go-peer dial <host:port> <bytes> <streams>      send a pattern on each stream, verify the echo;
//	                                                prints "RESULT <bytes>" or "CORRUPT <detail>"
//
// The payload pattern is go-udx's interop pattern, byte((i+seed)*31 + (i+seed)/251),
// with seed = stream index * 1000.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	udx "github.com/stephanfeb/go-udx"
)

func pattern(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		j := i + seed
		b[i] = byte(j*31 + j/251)
	}
	return b
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func listen(addr string) {
	mux, err := udx.Listen(addr)
	if err != nil {
		fail("listen: %v", err)
	}
	_, port, _ := splitPort(mux.Addr().String())
	fmt.Fprintf(os.Stderr, "READY %s\n", port)
	for {
		conn, err := mux.Accept(context.Background())
		if err != nil {
			return
		}
		go func() {
			for {
				s, err := conn.AcceptStream(context.Background())
				if err != nil {
					return
				}
				go func() {
					io.Copy(s, s)
					s.CloseWrite()
				}()
			}
		}()
	}
}

func dial(addr string, size, streams int) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Not udx.Dial: it binds a dual-stack socket, and go-udx's batched send
	// path (ipv4.PacketConn) drops every multi-datagram write from one to an
	// IPv4 peer. Bind the address family explicitly, as go-libp2p-udx-transport does.
	raddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		fail("resolve: %v", err)
	}
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		fail("bind: %v", err)
	}
	mux := udx.NewMultiplexer(pc, udx.RealClock{})
	defer mux.Close()
	conn, err := mux.Dial(ctx, raddr)
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	errs := make(chan string, streams)
	for i := 0; i < streams; i++ {
		s, err := conn.OpenStream(ctx)
		if err != nil {
			fail("open stream: %v", err)
		}
		want := pattern(size, i*1000)
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.SetWriteDeadline(time.Now().Add(60 * time.Second))
			if _, err := s.Write(want); err != nil {
				errs <- fmt.Sprintf("write: %v", err)
			}
			s.CloseWrite()
		}()
		go func(i int) {
			defer wg.Done()
			s.SetReadDeadline(time.Now().Add(60 * time.Second))
			got, err := io.ReadAll(s)
			if err != nil {
				errs <- fmt.Sprintf("stream %d read: %v after %d bytes", i, err, len(got))
				return
			}
			if !bytes.Equal(got, want) {
				errs <- fmt.Sprintf("stream %d: got %d bytes, want %d (or content differs)", i, len(got), len(want))
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		fmt.Fprintf(os.Stderr, "CORRUPT %s\n", e)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "RESULT %d\n", size*streams)
}

func splitPort(hostport string) (string, string, error) {
	for i := len(hostport) - 1; i >= 0; i-- {
		if hostport[i] == ':' {
			return hostport[:i], hostport[i+1:], nil
		}
	}
	return hostport, "", fmt.Errorf("no port in %q", hostport)
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: go-peer listen [addr] | dial <addr> <bytes> <streams>")
	}
	switch os.Args[1] {
	case "listen":
		addr := "127.0.0.1:0"
		if len(os.Args) > 2 {
			addr = os.Args[2]
		}
		listen(addr)
	case "dial":
		if len(os.Args) != 5 {
			fail("usage: go-peer dial <addr> <bytes> <streams>")
		}
		size, _ := strconv.Atoi(os.Args[3])
		streams, _ := strconv.Atoi(os.Args[4])
		dial(os.Args[2], size, streams)
	default:
		fail("unknown mode %q", os.Args[1])
	}
}
