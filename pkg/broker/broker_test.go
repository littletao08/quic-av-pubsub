package broker

import (
	"log/slog"
	"os"
	"testing"

	"github.com/yourorg/quic-pubsub/pkg/proto"
)

func BenchmarkPublish(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	// Register channel
	ch := brk.RegisterChannel("test")

	// Add subscriber
	sub := NewSubscriber("bench-sub", proto.TrackVideo, 1024)
	brk.Subscribe("test", sub)

	f := &proto.Frame{
		Type:      proto.FrameVideo,
		ChannelID: ch.ID,
		Seq:       1,
		Timestamp: proto.Now(),
		Payload:   make([]byte, 15000),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Publish(f)
	}
}

func BenchmarkPublishMultiSub(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	// Add 10 subscribers
	for i := 0; i < 10; i++ {
		sub := NewSubscriber("sub", proto.TrackVideo, 1024)
		brk.Subscribe("test", sub)
	}

	f := &proto.Frame{
		Type:      proto.FrameVideo,
		ChannelID: ch.ID,
		Seq:       1,
		Timestamp: proto.Now(),
		Payload:   make([]byte, 15000),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Publish(f)
	}
}

func TestChannelCreateAndPublish(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	received := make(chan *proto.Frame, 1)
	sub := &Subscriber{
		ID:       "sub-1",
		TrackTyp: proto.TrackVideo,
		Ch:       received,
		Done:     make(chan struct{}),
	}
	brk.Subscribe("test", sub)

	f := &proto.Frame{
		Type:      proto.FrameVideo,
		ChannelID: ch.ID,
		Seq:       42,
		Payload:   []byte("test"),
	}
	brk.Publish(f)

	select {
	case rf := <-received:
		if rf.Seq != 42 {
			t.Fatalf("wrong seq: %d", rf.Seq)
		}
	default:
		t.Fatal("frame not received")
	}
}

func TestChannelCleanup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	brk.RegisterChannel("test")

	// Cleanup non-existent connection should not panic
	brk.CleanupConn("non-existent")

	// Register, subscribe, cleanup
	sub := NewSubscriber("conn-1", proto.TrackAudio, 10)
	brk.Subscribe("test", sub)
	brk.CleanupConn("conn-1")

	select {
	case <-sub.Done:
		// correctly closed
	default:
		t.Fatal("subscriber should have been closed")
	}
}
