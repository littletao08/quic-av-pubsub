package proto

import (
	"bytes"
	"testing"
	"time"
)

var payloadSizes = []struct {
	name string
	size int
}{
	{"Audio_80B", 80},
	{"SD_15KB", 15000},
	{"HD_100KB", 100 * 1024},
	{"FHD_200KB", 200 * 1024},
	{"4K_500KB", 500 * 1024},
}

func BenchmarkWriteFrameSizes(b *testing.B) {
	for _, ps := range payloadSizes {
		b.Run(ps.name, func(b *testing.B) {
			f := &Frame{
				Type:      FrameVideo,
				ChannelID: 1,
				Seq:       1,
				Timestamp: Now(),
				Payload:   make([]byte, ps.size),
			}
			var buf bytes.Buffer
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				WriteFrame(&buf, f)
			}
			b.ReportMetric(float64(ps.size), "payload_B")
		})
	}
}

func BenchmarkReadFrameSizes(b *testing.B) {
	for _, ps := range payloadSizes {
		b.Run(ps.name, func(b *testing.B) {
			f := &Frame{
				Type:      FrameVideo,
				ChannelID: 1,
				Seq:       1,
				Timestamp: Now(),
				Payload:   make([]byte, ps.size),
			}
			var buf bytes.Buffer
			WriteFrame(&buf, f)
			data := buf.Bytes()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(data)
				ReadFrame(r)
			}
			b.ReportMetric(float64(ps.size), "payload_B")
		})
	}
}

func BenchmarkReadFrameLatency(b *testing.B) {
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       1,
		Timestamp: Now(),
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

func BenchmarkWriteFrameParallel(b *testing.B) {
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       1,
		Timestamp: Now(),
		Payload:   make([]byte, 15000),
	}
	b.RunParallel(func(pb *testing.PB) {
		var buf bytes.Buffer
		for pb.Next() {
			buf.Reset()
			WriteFrame(&buf, f)
		}
	})
}

func BenchmarkMixedAudioVideo(b *testing.B) {
	audio := &Frame{
		Type:      FrameAudio,
		ChannelID: 1,
		Seq:       0,
		Timestamp: Now(),
		Payload:   make([]byte, 80),
	}
	video := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       0,
		Timestamp: Now(),
		Flags:     0x01,
		Payload:   make([]byte, 15000),
	}
	msg := &Frame{
		Type:      FrameMessage,
		ChannelID: 1,
		Seq:       0,
		Timestamp: Now(),
		Payload:   make([]byte, 200),
	}

	types := []*Frame{audio, video, audio, video, video, msg}

	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteFrame(&buf, types[i%len(types)])
	}
}

func TestLargeVideoFrameWriteThroughput(t *testing.T) {
	const targetMB = 100
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       1,
		Timestamp: Now(),
		Payload:   make([]byte, 200*1024),
	}
	var buf bytes.Buffer
	start := time.Now()
	written := 0
	for time.Since(start) < time.Second {
		buf.Reset()
		WriteFrame(&buf, f)
		written += len(f.Payload) + FrameHeaderSize
	}
	elapsed := time.Since(start).Seconds()
	mbps := float64(written) / elapsed / 1024 / 1024 * 8
	t.Logf("WriteFrame throughput (200KB frames): %.2f Mbps, %.2f MB/s", mbps, float64(written)/elapsed/1024/1024)

	if mbps > 100 {
		t.Logf("PASS: >100 Mbps write throughput")
	}
}

func BenchmarkBidirectionalFrameCodec(b *testing.B) {
	f := &Frame{
		Type:      FrameVideo,
		ChannelID: 1,
		Seq:       1,
		Timestamp: Now(),
		Payload:   make([]byte, 15000),
	}
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteFrame(&buf, f)
		r := bytes.NewReader(buf.Bytes())
		ReadFrame(r)
	}
}

func BenchmarkConsecutiveSeqWrite(b *testing.B) {
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		f := &Frame{
			Type:      FrameVideo,
			ChannelID: 1,
			Seq:       uint32(i),
			Timestamp: Now(),
			Payload:   make([]byte, 15000),
		}
		WriteFrame(&buf, f)
	}
}

func BenchmarkFrameTypeSwitch(b *testing.B) {
	types := []FrameType{FrameAudio, FrameVideo, FrameMessage, FrameHeartbeat, FrameSigPublish, FrameSigSubscribe, FrameSigAck, FrameSigErr}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := types[i%len(types)]
		_ = t.String()
	}
}

func TestMaxWriteThroughput(t *testing.T) {
	sizes := []struct {
		name string
		size int
	}{
		{"80B", 80},
		{"15KB", 15000},
		{"200KB", 200 * 1024},
		{"500KB", 500 * 1024},
	}

	for _, s := range sizes {
		t.Run(s.name, func(t *testing.T) {
			f := &Frame{
				Type:      FrameVideo,
				ChannelID: 1,
				Seq:       1,
				Timestamp: Now(),
				Payload:   make([]byte, s.size),
			}
			var buf bytes.Buffer
			start := time.Now()
			count := 0
			for time.Since(start) < 2*time.Second {
				buf.Reset()
				WriteFrame(&buf, f)
				count++
			}
			elapsed := time.Since(start).Seconds()
			totalBytes := int64(count) * (int64(s.size) + FrameHeaderSize)
			mbps := float64(totalBytes) / elapsed / 1024 / 1024 * 8
			t.Logf("size=%s count=%d throughput=%.2f Mbps (%.2f MB/s)", s.name, count, mbps, float64(totalBytes)/elapsed/1024/1024)

			if count < 10 && s.size > 1000 {
				t.Errorf("too few writes: %d in 2s", count)
			}
		})
	}
}
