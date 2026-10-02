// Command gen-vectors writes wire-format test vectors produced by go-udx, the
// wire authority, for the js-udx codec tests.
//
//	go run . > ../../packages/udx/test/vectors/go-v3.json
//
// The output has four sections:
//   - packets: hand-built packets, their go-udx encoding and a JSON description
//     the JS test rebuilds and encodes, expecting identical bytes;
//   - versionNegotiation: the same for version negotiation packets;
//   - truncations and fuzz: arbitrary datagrams with go-udx's verdict (accepted
//     or rejected) and, when accepted, its re-encoding. The JS decoder must agree.
//
// Everything is deterministic: rerunning produces the same file.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"unicode/utf8"

	udx "github.com/stephanfeb/go-udx"
)

const maxSafeInteger = 1<<53 - 1

type packetVector struct {
	Name   string     `json:"name"`
	Hex    string     `json:"hex"`
	Packet jsonPacket `json:"packet"`
}

type jsonPacket struct {
	Version             uint32           `json:"version"`
	DestinationCid      string           `json:"destinationCid"`
	SourceCid           string           `json:"sourceCid"`
	Sequence            uint32           `json:"sequence"`
	DestinationStreamId uint32           `json:"destinationStreamId"`
	SourceStreamId      uint32           `json:"sourceStreamId"`
	Frames              []map[string]any `json:"frames"`
}

type versionNegotiationVector struct {
	Name              string   `json:"name"`
	Hex               string   `json:"hex"`
	DestinationCid    string   `json:"destinationCid"`
	SourceCid         string   `json:"sourceCid"`
	SupportedVersions []uint32 `json:"supportedVersions"`
}

type verdictVector struct {
	Input string `json:"input"`
	OK    bool   `json:"ok"`
	// Reencoded is go-udx's re-encoding of an accepted packet.
	Reencoded string `json:"reencoded,omitempty"`
	// UnsafeU64 marks an accepted packet with a u64 field above 2^53-1, which
	// the JS decoder deliberately rejects.
	UnsafeU64 bool `json:"unsafeU64,omitempty"`
	// LossyReason marks a CONNECTION_CLOSE reason that isn't valid UTF-8, so a
	// string round trip can't reproduce its bytes.
	LossyReason bool `json:"lossyReason,omitempty"`
}

type output struct {
	Generator          string                     `json:"generator"`
	Packets            []packetVector             `json:"packets"`
	VersionNegotiation []versionNegotiationVector `json:"versionNegotiation"`
	Truncations        []verdictVector            `json:"truncations"`
	Fuzz               []verdictVector            `json:"fuzz"`
}

func cid(b ...byte) udx.ConnectionID {
	c, err := udx.NewConnectionID(b)
	if err != nil {
		panic(err)
	}
	return c
}

func seqBytes(start, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(start + i)
	}
	return b
}

func cidHex(c udx.ConnectionID) string { return hex.EncodeToString(c.Bytes()) }

func describeFrame(f udx.Frame) map[string]any {
	m := map[string]any{"type": int(f.Type())}
	switch f := f.(type) {
	case *udx.PaddingFrame, *udx.PingFrame:
	case *udx.AckFrame:
		ranges := make([]map[string]any, 0, len(f.AckRanges))
		for _, r := range f.AckRanges {
			ranges = append(ranges, map[string]any{"gap": r.Gap, "length": r.AckRangeLength})
		}
		m["largestAcked"] = f.LargestAcked
		m["ackDelay"] = f.AckDelay
		m["firstAckRangeLength"] = f.FirstAckRangeLength
		m["ranges"] = ranges
	case *udx.StreamFrame:
		m["fin"] = f.IsFin
		m["syn"] = f.IsSyn
		m["offset"] = f.Offset
		m["data"] = hex.EncodeToString(f.Data)
	case *udx.WindowUpdateFrame:
		m["limit"] = f.WindowSize
	case *udx.MaxDataFrame:
		m["maxData"] = f.MaxData
	case *udx.ResetStreamFrame:
		m["errorCode"] = f.ErrorCode
	case *udx.MaxStreamsFrame:
		m["maxStreams"] = f.MaxStreamCount
	case *udx.MTUProbeFrame:
		m["size"] = f.Len()
	case *udx.PathChallengeFrame:
		m["data"] = hex.EncodeToString(f.Data[:])
	case *udx.PathResponseFrame:
		m["data"] = hex.EncodeToString(f.Data[:])
	case *udx.ConnectionCloseFrame:
		m["errorCode"] = f.ErrorCode
		m["frameType"] = f.FrameTypeVal
		m["reason"] = f.ReasonPhrase
	case *udx.StopSendingFrame:
		m["streamId"] = f.StreamID
		m["errorCode"] = f.ErrorCode
	case *udx.DataBlockedFrame:
		m["limit"] = f.MaxData
	case *udx.StreamDataBlockedFrame:
		m["streamId"] = f.StreamID
		m["limit"] = f.MaxStreamData
	case *udx.NewConnectionIDFrame:
		m["sequence"] = f.SequenceNumber
		m["retirePriorTo"] = f.RetirePriorTo
		m["connectionId"] = cidHex(f.ConnectionID)
		m["resetToken"] = hex.EncodeToString(f.ResetToken[:])
	case *udx.RetireConnectionIDFrame:
		m["sequence"] = f.SequenceNumber
	default:
		panic(fmt.Sprintf("unhandled frame %T", f))
	}
	return m
}

func describePacket(p *udx.Packet) jsonPacket {
	frames := make([]map[string]any, 0, len(p.Frames))
	for _, f := range p.Frames {
		frames = append(frames, describeFrame(f))
	}
	return jsonPacket{
		Version:             p.Version,
		DestinationCid:      cidHex(p.DestinationCID),
		SourceCid:           cidHex(p.SourceCID),
		Sequence:            p.Sequence,
		DestinationStreamId: p.DestinationStreamID,
		SourceStreamId:      p.SourceStreamID,
		Frames:              frames,
	}
}

var (
	dcid8 = cid(1, 2, 3, 4, 5, 6, 7, 8)
	scid8 = cid(0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18)
	token = func() (t [udx.StatelessResetTokenLength]byte) {
		copy(t[:], seqBytes(0xa0, udx.StatelessResetTokenLength))
		return
	}()
)

func pkt(seq, dst, src uint32, frames ...udx.Frame) *udx.Packet {
	return &udx.Packet{
		Version:             udx.VersionCurrent,
		DestinationCID:      dcid8,
		SourceCID:           scid8,
		Sequence:            seq,
		DestinationStreamID: dst,
		SourceStreamID:      src,
		Frames:              frames,
	}
}

func buildPackets() []packetVector {
	type named struct {
		name string
		p    *udx.Packet
	}
	maxRanges := make([]udx.AckRange, 255)
	for i := range maxRanges {
		maxRanges[i] = udx.AckRange{Gap: uint8(i), AckRangeLength: uint32(i * 7)}
	}

	cases := []named{
		{"connection SYN (dialer, stream ids 0/0)", pkt(0, 0, 0, &udx.StreamFrame{IsSyn: true})},
		{"stream open: SYN with first data", pkt(1, 0, 1, &udx.StreamFrame{IsSyn: true, Data: []byte("hello from go-udx")})},
		{"stream data mid-stream", pkt(7, 2, 1, &udx.StreamFrame{Offset: 1372, Data: seqBytes(0, 64)})},
		{"stream data above 2^32 offset", pkt(8, 2, 1, &udx.StreamFrame{Offset: 5_000_000_000, Data: []byte{0xde, 0xad}})},
		{"stream data at max safe offset", pkt(9, 2, 1, &udx.StreamFrame{Offset: maxSafeInteger - 1, Data: []byte{1}})},
		{"FIN with final size, no data", pkt(10, 2, 1, &udx.StreamFrame{IsFin: true, Offset: 123456})},
		{"SYN+FIN with data", pkt(11, 0, 3, &udx.StreamFrame{IsSyn: true, IsFin: true, Data: []byte("x")})},
		{"full-size stream payload (1372 bytes)", pkt(12, 2, 1, &udx.StreamFrame{Offset: 2744, Data: seqBytes(3, 1372)})},
		{"padding", pkt(0, 0, 0, &udx.PaddingFrame{})},
		{"ping", pkt(0, 0, 0, &udx.PingFrame{})},
		{"ack, nothing received yet", pkt(0, 1, 2, &udx.AckFrame{LargestAcked: 0, FirstAckRangeLength: 1})},
		{"ack, contiguous", pkt(0, 1, 2, &udx.AckFrame{LargestAcked: 41, AckDelay: 3, FirstAckRangeLength: 42})},
		{"ack, two gaps", pkt(0, 1, 2, &udx.AckFrame{LargestAcked: 100, AckDelay: 25, FirstAckRangeLength: 10,
			AckRanges: []udx.AckRange{{Gap: 2, AckRangeLength: 5}, {Gap: 1, AckRangeLength: 80}}})},
		{"ack, five ranges, max gap and delay", pkt(0, 1, 2, &udx.AckFrame{LargestAcked: 0xffffffff, AckDelay: 0xffff, FirstAckRangeLength: 3,
			AckRanges: []udx.AckRange{{Gap: 255, AckRangeLength: 1}, {Gap: 1, AckRangeLength: 1}, {Gap: 7, AckRangeLength: 2}, {Gap: 9, AckRangeLength: 3}, {Gap: 0, AckRangeLength: 0xffffffff}}})},
		{"ack, 255 ranges", pkt(0, 1, 2, &udx.AckFrame{LargestAcked: 5000, FirstAckRangeLength: 1, AckRanges: maxRanges})},
		{"window update (absolute offset mod 2^32)", pkt(0, 1, 2, &udx.WindowUpdateFrame{WindowSize: 131072})},
		{"window update, wrapped", pkt(0, 1, 2, &udx.WindowUpdateFrame{WindowSize: 0xfffffff0})},
		{"max data 1 MiB", pkt(0, 0, 0, &udx.MaxDataFrame{MaxData: 1 << 20})},
		{"max data, max safe integer", pkt(0, 0, 0, &udx.MaxDataFrame{MaxData: maxSafeInteger})},
		{"reset stream", pkt(0, 2, 1, &udx.ResetStreamFrame{ErrorCode: udx.ErrorFlowControlError})},
		{"max streams", pkt(0, 0, 0, &udx.MaxStreamsFrame{MaxStreamCount: 100})},
		{"mtu probe to 1280 bytes", pkt(13, 0, 0, &udx.MTUProbeFrame{ProbeSize: 1280 - 34})},
		{"ping then mtu probe", pkt(14, 0, 0, &udx.PingFrame{}, &udx.MTUProbeFrame{ProbeSize: 40})},
		{"mtu probe, type byte only", pkt(15, 0, 0, &udx.MTUProbeFrame{ProbeSize: 1})},
		{"path challenge", pkt(0, 0, 0, &udx.PathChallengeFrame{Data: [8]byte{1, 2, 3, 4, 5, 6, 7, 8}})},
		{"path response", pkt(0, 0, 0, &udx.PathResponseFrame{Data: [8]byte{8, 7, 6, 5, 4, 3, 2, 1}})},
		{"connection close, empty reason", pkt(0, 0, 0, &udx.ConnectionCloseFrame{})},
		{"connection close with reason", pkt(0, 0, 0, &udx.ConnectionCloseFrame{ErrorCode: udx.ErrorProtocolViolation, FrameTypeVal: 3, ReasonPhrase: "bad frame"})},
		{"connection close, multibyte UTF-8 reason", pkt(0, 0, 0, &udx.ConnectionCloseFrame{ErrorCode: 1, ReasonPhrase: "über ✓ 🙂"})},
		{"stop sending", pkt(0, 2, 1, &udx.StopSendingFrame{StreamID: 1, ErrorCode: 9})},
		{"data blocked", pkt(0, 0, 0, &udx.DataBlockedFrame{MaxData: 1 << 20})},
		{"stream data blocked", pkt(0, 2, 1, &udx.StreamDataBlockedFrame{StreamID: 1, MaxStreamData: 65536})},
		{"new connection id, 8-byte cid", pkt(0, 0, 0, &udx.NewConnectionIDFrame{SequenceNumber: 1, RetirePriorTo: 0, ConnectionID: cid(seqBytes(0x40, 8)...), ResetToken: token})},
		{"new connection id, empty cid", pkt(0, 0, 0, &udx.NewConnectionIDFrame{SequenceNumber: 2, RetirePriorTo: 1, ConnectionID: cid(), ResetToken: token})},
		{"new connection id, 20-byte cid", pkt(0, 0, 0, &udx.NewConnectionIDFrame{SequenceNumber: 1 << 40, RetirePriorTo: 1 << 39, ConnectionID: cid(seqBytes(0x50, 20)...), ResetToken: token})},
		{"retire connection id", pkt(0, 0, 0, &udx.RetireConnectionIDFrame{SequenceNumber: 3})},
		{"multiple frames in one packet", pkt(20, 2, 1,
			&udx.AckFrame{LargestAcked: 9, FirstAckRangeLength: 10},
			&udx.StreamFrame{Offset: 10, Data: []byte("abc")},
			&udx.WindowUpdateFrame{WindowSize: 70000},
			&udx.PaddingFrame{}, &udx.PingFrame{})},
		{"header only, no frames", pkt(0, 0, 0)},
		{"max header field values", pkt(0xffffffff, 0xffffffff, 0xffffffff, &udx.PingFrame{})},
	}

	// Header variations.
	zero := pkt(0, 0, 0, &udx.PingFrame{})
	zero.DestinationCID, zero.SourceCID = cid(), cid()
	cases = append(cases, named{"zero-length CIDs", zero})
	long := pkt(5, 6, 7, &udx.PingFrame{})
	long.DestinationCID, long.SourceCID = cid(seqBytes(0x60, 20)...), cid(seqBytes(0x80, 20)...)
	cases = append(cases, named{"20-byte CIDs", long})
	mixed := pkt(5, 6, 7, &udx.PingFrame{})
	mixed.SourceCID = cid(0xff)
	cases = append(cases, named{"asymmetric CID lengths", mixed})
	v2 := pkt(0, 0, 0, &udx.PingFrame{})
	v2.Version = udx.VersionV2
	cases = append(cases, named{"version 2 (the codec does not check version)", v2})
	odd := pkt(0, 0, 0, &udx.PingFrame{})
	odd.Version = 0xdeadbeef
	cases = append(cases, named{"unknown version", odd})

	out := make([]packetVector, 0, len(cases))
	for _, c := range cases {
		b := udx.MarshalPacket(c.p)
		back, err := udx.UnmarshalPacket(b)
		if err != nil {
			panic(fmt.Sprintf("%s: go-udx rejects its own encoding: %v", c.name, err))
		}
		out = append(out, packetVector{Name: c.name, Hex: hex.EncodeToString(b), Packet: describePacket(back)})
	}
	return out
}

func buildVersionNegotiation() []versionNegotiationVector {
	cases := []struct {
		name string
		p    udx.VersionNegotiationPacket
	}{
		{"supported versions", udx.VersionNegotiationPacket{DestinationCID: scid8, SourceCID: dcid8, SupportedVersions: udx.SupportedVersions}},
		{"no versions, empty CIDs", udx.VersionNegotiationPacket{DestinationCID: cid(), SourceCID: cid(), SupportedVersions: []uint32{}}},
		{"20-byte CIDs", udx.VersionNegotiationPacket{DestinationCID: cid(seqBytes(1, 20)...), SourceCID: cid(seqBytes(100, 20)...), SupportedVersions: []uint32{3, 0xffffffff}}},
	}
	out := make([]versionNegotiationVector, 0, len(cases))
	for _, c := range cases {
		b := c.p.Marshal()
		if _, err := udx.UnmarshalVersionNegotiation(b); err != nil {
			panic(err)
		}
		out = append(out, versionNegotiationVector{
			Name: c.name, Hex: hex.EncodeToString(b),
			DestinationCid: cidHex(c.p.DestinationCID), SourceCid: cidHex(c.p.SourceCID),
			SupportedVersions: c.p.SupportedVersions,
		})
	}
	return out
}

func verdict(input []byte) verdictVector {
	v := verdictVector{Input: hex.EncodeToString(input)}
	p, err := udx.UnmarshalPacket(input)
	if err != nil {
		return v
	}
	v.OK = true
	v.Reencoded = hex.EncodeToString(udx.MarshalPacket(p))
	for _, f := range p.Frames {
		switch f := f.(type) {
		case *udx.StreamFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.Offset > maxSafeInteger
		case *udx.MaxDataFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.MaxData > maxSafeInteger
		case *udx.DataBlockedFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.MaxData > maxSafeInteger
		case *udx.StreamDataBlockedFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.MaxStreamData > maxSafeInteger
		case *udx.NewConnectionIDFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.SequenceNumber > maxSafeInteger || f.RetirePriorTo > maxSafeInteger
		case *udx.RetireConnectionIDFrame:
			v.UnsafeU64 = v.UnsafeU64 || f.SequenceNumber > maxSafeInteger
		case *udx.ConnectionCloseFrame:
			v.LossyReason = v.LossyReason || !utf8.ValidString(f.ReasonPhrase)
		}
	}
	return v
}

// buildTruncations feeds every proper prefix of each (small) valid packet to go-udx.
func buildTruncations(packets []packetVector) []verdictVector {
	var out []verdictVector
	for _, pv := range packets {
		b, _ := hex.DecodeString(pv.Hex)
		if len(b) > 200 {
			continue
		}
		for n := 0; n < len(b); n++ {
			out = append(out, verdict(b[:n]))
		}
	}
	return out
}

// buildFuzz mutates valid packets with a fixed seed and records go-udx's verdict.
func buildFuzz(packets []packetVector, n int) []verdictVector {
	rng := rand.New(rand.NewSource(0x75647833))
	interesting := []byte{0x00, 0x01, 0x02, 0x03, 0x0b, 0x0c, 0x0d, 0x11, 0x12, 0x14, 0x15, 0x7f, 0x80, 0xff}
	var bases [][]byte
	for _, pv := range packets {
		b, _ := hex.DecodeString(pv.Hex)
		if len(b) <= 200 {
			bases = append(bases, b)
		}
	}
	out := make([]verdictVector, 0, n)
	for i := 0; i < n; i++ {
		base := bases[rng.Intn(len(bases))]
		b := append([]byte(nil), base...)
		switch rng.Intn(5) {
		case 0: // flip random bytes
			for k := 1 + rng.Intn(4); k > 0; k-- {
				b[rng.Intn(len(b))] = byte(rng.Intn(256))
			}
		case 1: // plant an interesting byte
			b[rng.Intn(len(b))] = interesting[rng.Intn(len(interesting))]
		case 2: // truncate
			b = b[:rng.Intn(len(b))]
		case 3: // append junk
			for k := 1 + rng.Intn(20); k > 0; k-- {
				b = append(b, byte(rng.Intn(256)))
			}
		case 4: // corrupt the frame region only, keeping the header valid
			hdr := 34
			if len(b) > hdr {
				for k := 1 + rng.Intn(3); k > 0; k-- {
					b[hdr+rng.Intn(len(b)-hdr)] = byte(rng.Intn(256))
				}
			}
		}
		out = append(out, verdict(b))
	}
	return out
}

func goUdxRevision() string {
	cmd := exec.Command("git", "-C", "../../../go-udx", "rev-parse", "--short", "HEAD")
	b, err := cmd.Output()
	if err != nil {
		return "go-udx (unknown revision)"
	}
	return "go-udx " + strings.TrimSpace(string(b))
}

func main() {
	packets := buildPackets()
	out := output{
		Generator:          goUdxRevision() + " via tools/gen-vectors",
		Packets:            packets,
		VersionNegotiation: buildVersionNegotiation(),
		Truncations:        buildTruncations(packets),
		Fuzz:               buildFuzz(packets, 2000),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}
