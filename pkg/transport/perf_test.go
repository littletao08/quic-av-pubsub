package transport

import (
	"math/rand"
	"testing"

	"github.com/yourorg/quic-pubsub/pkg/proto"
)

func BenchmarkJitterBufferPush(b *testing.B) {
	jb := newJitterBuffer(256)
	f := &proto.Frame{
		Type:      proto.FrameVideo,
		ChannelID: 1,
		Seq:       1,
		Timestamp: proto.Now(),
		Payload:   make([]byte, 15000),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seq = uint32(i)
		jb.push(f)
	}
}

func BenchmarkJitterBufferPushPop(b *testing.B) {
	jb := newJitterBuffer(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
			Payload:   make([]byte, 15000),
		}
		jb.push(f)
		for {
			popped := jb.pop()
			if popped == nil {
				break
			}
		}
	}
}

func BenchmarkJitterBufferOutOfOrder(b *testing.B) {
	jb := newJitterBuffer(256)
	frames := make([]*proto.Frame, 256)
	for i := 0; i < 256; i++ {
		frames[i] = &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
			Payload:   make([]byte, 15000),
		}
	}

	// Pre-shuffle
	shuffled := make([]int, 256)
	for i := 0; i < 256; i++ {
		shuffled[i] = i
	}
	rand.Shuffle(256, func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 256
		jb.push(frames[shuffled[idx]])
		for {
			popped := jb.pop()
			if popped == nil {
				break
			}
		}
	}
}

func BenchmarkJitterBufferFullDrop(b *testing.B) {
	jb := newJitterBuffer(256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
			Payload:   make([]byte, 15000),
		}
		jb.push(f)
		if i > 255 {
			jb.pop()
		}
	}
}

func BenchmarkJitterBufferHighLatency(b *testing.B) {
	jb := newJitterBuffer(512)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
			Payload:   make([]byte, 15000),
		}
		jb.push(f)
	}
}



func BenchmarkExpandTracks(b *testing.B) {
	tracks := []proto.TrackType{proto.TrackAll}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expandTracks(tracks)
	}
}

func BenchmarkExpandTracksAllCombos(b *testing.B) {
	combos := [][]proto.TrackType{
		{proto.TrackAll},
		{proto.TrackAudio, proto.TrackVideo},
		{proto.TrackVideo, proto.TrackMessage},
		{proto.TrackAudio, proto.TrackVideo, proto.TrackMessage},
		{proto.TrackAll, proto.TrackAudio},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expandTracks(combos[i%len(combos)])
	}
}

func BenchmarkMustMarshalJSON(b *testing.B) {
	data := map[string]string{"track": "video", "channel": "room:101"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustMarshalJSON(data)
	}
}

func TestJitterBufferSequential(t *testing.T) {
	jb := newJitterBuffer(256)
	for i := 0; i < 100; i++ {
		f := &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
			Payload:   make([]byte, 100),
		}
		jb.push(f)
	}
	count := 0
	for {
		f := jb.pop()
		if f == nil {
			break
		}
		if f.Seq != uint32(count) {
			t.Fatalf("seq mismatch: expected %d, got %d", count, f.Seq)
		}
		count++
	}
	if count != 100 {
		t.Fatalf("expected 100 frames, got %d", count)
	}
}

func TestJitterBufferOutOfOrder(t *testing.T) {
	jb := newJitterBuffer(64)
	seqs := []int{0, 2, 1, 4, 3, 6, 5, 8, 7, 9}
	for _, s := range seqs {
		jb.push(&proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(s),
			Timestamp: proto.Now(),
		})
	}
	var popped []uint32
	for {
		f := jb.pop()
		if f == nil {
			break
		}
		popped = append(popped, f.Seq)
	}
	if len(popped) != 10 {
		t.Fatalf("expected 10 frames, got %d: %v", len(popped), popped)
	}
	for i := 1; i < len(popped); i++ {
		if popped[i] <= popped[i-1] {
			t.Fatalf("not sorted: %v", popped)
		}
	}
}

func TestJitterBufferWrapAround(t *testing.T) {
	jb := newJitterBuffer(64)
	// Start near max uint32 to test wrap
	startSeq := ^uint32(0) - 5
	for i := uint32(0); i < 20; i++ {
		jb.push(&proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       startSeq + i + 1,
			Timestamp: proto.Now(),
		})
	}
}

func TestJitterBufferMaxSize(t *testing.T) {
	jb := newJitterBuffer(16)
	for i := 0; i < 100; i++ {
		jb.push(&proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: proto.Now(),
		})
	}
	if len(jb.buf) > 16 {
		t.Fatalf("buffer exceeded max size: %d", len(jb.buf))
	}
}
