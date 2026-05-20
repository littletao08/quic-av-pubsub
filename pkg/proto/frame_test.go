package proto

import (
	"bytes"
	"testing"
)

func BenchmarkWriteFrame(b *testing.B) {
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       12345,
		Timestamp: Now(),
		Flags:     0x01,
		Payload:   make([]byte, 15000),
	}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteFrame(&buf, f)
	}
}

func BenchmarkReadFrame(b *testing.B) {
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       12345,
		Timestamp: Now(),
		Flags:     0x01,
		Payload:   make([]byte, 15000),
	}
	var buf bytes.Buffer
	WriteFrame(&buf, f)
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		ReadFrame(r)
	}
}

func BenchmarkWriteSmallFrame(b *testing.B) {
	f := &Frame{
		Type:      FrameAudio,
		ChannelID: 1,
		Seq:       12345,
		Timestamp: Now(),
		Payload:   make([]byte, 80),
	}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteFrame(&buf, f)
	}
}

func TestWriteReadRoundtrip(t *testing.T) {
	original := &Frame{
		Type:      FrameVideo,
		ChannelID: 42,
		Seq:       999,
		Timestamp: 1234567890,
		Flags:     0x01,
		Payload:   []byte("hello world"),
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != original.Type {
		t.Fatalf("type mismatch: %d != %d", decoded.Type, original.Type)
	}
	if decoded.ChannelID != original.ChannelID {
		t.Fatalf("channel id mismatch: %d != %d", decoded.ChannelID, original.ChannelID)
	}
	if decoded.Seq != original.Seq {
		t.Fatalf("seq mismatch: %d != %d", decoded.Seq, original.Seq)
	}
	if decoded.Timestamp != original.Timestamp {
		t.Fatalf("timestamp mismatch: %d != %d", decoded.Timestamp, original.Timestamp)
	}
	if decoded.Flags != original.Flags {
		t.Fatalf("flags mismatch: %d != %d", decoded.Flags, original.Flags)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("payload mismatch: %s != %s", decoded.Payload, original.Payload)
	}
}

func TestEmptyPayloadRoundtrip(t *testing.T) {
	original := &Frame{
		Type:      FrameHeartbeat,
		ChannelID: 0,
		Seq:       0,
		Timestamp: 0,
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != original.Type {
		t.Fatalf("type mismatch")
	}
}

func TestLargePayloadRejected(t *testing.T) {
	var header [FrameHeaderSize]byte
	// Set payload length to > 64MB
	header[0] = byte(FrameAudio)
	header[19] = 0x05
	// This makes payLen = 0x05000000 = 83886080 > 64MB

	r := bytes.NewReader(header[:])
	_, err := ReadFrame(r)
	if err == nil {
		t.Fatal("expected error for large payload")
	}
}
