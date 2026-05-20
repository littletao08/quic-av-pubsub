package broker

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yourorg/quic-pubsub/pkg/proto"
)

func BenchmarkPublishFanout(b *testing.B) {
	for _, numSubs := range []int{1, 5, 10, 50, 100, 500} {
		b.Run(fmt.Sprintf("%d_subs", numSubs), func(b *testing.B) {
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
			brk := NewBroker(logger)
			ch := brk.RegisterChannel("test")

			for i := 0; i < numSubs; i++ {
				sub := NewSubscriber(fmt.Sprintf("sub-%d", i), proto.TrackVideo, 1024)
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
		})
	}
}

func BenchmarkPublishAllTracks(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	for i := 0; i < 5; i++ {
		for _, t := range []proto.TrackType{proto.TrackAudio, proto.TrackVideo, proto.TrackMessage} {
			sub := NewSubscriber(fmt.Sprintf("sub-%d-%s", i, t), t, 256)
			brk.Subscribe("test", sub)
		}
	}

	audioFrame := &proto.Frame{Type: proto.FrameAudio, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 80)}
	videoFrame := &proto.Frame{Type: proto.FrameVideo, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 15000)}
	msgFrame := &proto.Frame{Type: proto.FrameMessage, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 200)}

	frames := []*proto.Frame{audioFrame, videoFrame, msgFrame}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Publish(frames[i%3])
	}
}

func BenchmarkPublishConcurrent(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")
	sub := NewSubscriber("sub-1", proto.TrackVideo, 100000)
	brk.Subscribe("test", sub)

	b.RunParallel(func(pb *testing.PB) {
		f := &proto.Frame{
			Type:      proto.FrameVideo,
			ChannelID: ch.ID,
			Seq:       1,
			Timestamp: proto.Now(),
			Payload:   make([]byte, 15000),
		}
		for pb.Next() {
			brk.Publish(f)
		}
	})
}

func BenchmarkConcurrentChannels(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	for i := 0; i < 100; i++ {
		brk.RegisterChannel(fmt.Sprintf("ch-%d", i))
		sub := NewSubscriber(fmt.Sprintf("sub-%d", i), proto.TrackVideo, 256)
		brk.Subscribe(fmt.Sprintf("ch-%d", i), sub)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var idx int
		for pb.Next() {
			idx++
			channelName := fmt.Sprintf("ch-%d", idx%100)
			ch, _ := brk.GetChannel(channelName)
			f := &proto.Frame{
				Type:      proto.FrameVideo,
				ChannelID: ch.ID,
				Seq:       uint32(idx),
				Timestamp: proto.Now(),
				Payload:   make([]byte, 15000),
			}
			brk.Publish(f)
		}
	})
}

func TestBrokerScalability(t *testing.T) {
	for _, numSubs := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("%d_subs", numSubs), func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
			brk := NewBroker(logger)
			ch := brk.RegisterChannel("test")

			subs := make([]*Subscriber, numSubs)
			for i := 0; i < numSubs; i++ {
				subs[i] = NewSubscriber(fmt.Sprintf("sub-%d", i), proto.TrackVideo, 1024)
				brk.Subscribe("test", subs[i])
			}

			// drain subscribers
			for _, s := range subs {
				go func(sub *Subscriber) {
					for range sub.Ch {
					}
				}(s)
			}

			f := &proto.Frame{
				Type:      proto.FrameVideo,
				ChannelID: ch.ID,
				Seq:       1,
				Timestamp: proto.Now(),
				Payload:   make([]byte, 15000),
			}

			start := time.Now()
			numFrames := 10000
			for i := 0; i < numFrames; i++ {
				brk.Publish(f)
			}
			elapsed := time.Since(start)
			fps := float64(numFrames) / elapsed.Seconds()
			fanoutFps := fps * float64(numSubs)

			t.Logf("subs=%d frames=%d elapsed=%v fps=%.0f fanout_fps=%.0f",
				numSubs, numFrames, elapsed, fps, fanoutFps)
		})
	}
}

func TestConcurrentPubSubStress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	for i := 0; i < 50; i++ {
		brk.RegisterChannel(fmt.Sprintf("ch-%d", i))
	}

	var wg sync.WaitGroup
	start := time.Now()

	// 10 publishers
	for p := 0; p < 10; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			ch, _ := brk.GetChannel(fmt.Sprintf("ch-%d", pid%50))
			for i := 0; i < 5000; i++ {
				f := &proto.Frame{
					Type:      proto.FrameVideo,
					ChannelID: ch.ID,
					Seq:       uint32(i),
					Timestamp: proto.Now(),
					Payload:   make([]byte, 15000),
				}
				brk.Publish(f)
			}
		}(p)
	}

	// 20 subscribers
	for s := 0; s < 20; s++ {
		wg.Add(1)
		go func(sid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				ch := fmt.Sprintf("ch-%d", i%50)
				sub := NewSubscriber(fmt.Sprintf("stress-sub-%d-%d", sid, i), proto.TrackVideo, 10000)
				brk.Subscribe(ch, sub)
				go func(s *Subscriber) {
					for range s.Ch {
					}
				}(sub)
			}
		}(s)
	}

	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("Concurrent stress test completed in %v", elapsed)
	t.Logf("Broker handled %d frames across %d channels with 20 concurrent subscribers",
		10*5000, 50)
}

func BenchmarkRegisterManyChannels(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.RegisterChannel(fmt.Sprintf("bench-ch-%d", i))
	}
}

func BenchmarkCleanupConn(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	brk.RegisterChannel("test")
	for i := 0; i < 100; i++ {
		sub := NewSubscriber(fmt.Sprintf("conn-%d", i), proto.TrackVideo, 16)
		brk.Subscribe("test", sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.CleanupConn(fmt.Sprintf("conn-%d", i%100))
	}
}

func BenchmarkMultiTrackPublish(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	// Subscribe to all 3 tracks
	for _, t := range []proto.TrackType{proto.TrackAudio, proto.TrackVideo, proto.TrackMessage} {
		sub := NewSubscriber("sub-"+string(t), t, 1024)
		brk.Subscribe("test", sub)
	}

	f := &proto.Frame{Type: proto.FrameVideo, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 15000)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Publish(f)
	}
}

func BenchmarkPublishSubscriberBackpressure(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	// tiny buffer to simulate backpressure
	sub := NewSubscriber("slow-sub", proto.TrackVideo, 4)
	brk.Subscribe("test", sub)

	f := &proto.Frame{Type: proto.FrameVideo, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 15000)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Publish(f)
	}
}

func BenchmarkUnregisterChannel(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)

	for i := 0; i < 1000; i++ {
		brk.RegisterChannel(fmt.Sprintf("ch-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.UnregisterChannel(fmt.Sprintf("ch-%d", i%1000))
		brk.RegisterChannel(fmt.Sprintf("ch-%d", i%1000))
	}
}

func TestChannelPublishDroppedMetric(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	brk := NewBroker(logger)
	ch := brk.RegisterChannel("test")

	// subscriber with 1 slot buffer
	sub := NewSubscriber("slow-sub", proto.TrackVideo, 1)
	brk.Subscribe("test", sub)
	// Don't drain - so frames will be dropped

	f := &proto.Frame{Type: proto.FrameVideo, ChannelID: ch.ID, Seq: 1, Timestamp: proto.Now(), Payload: make([]byte, 15000)}

	_ = sub.Send(f) // fill the 1 slot
	for i := 0; i < 1000; i++ {
		brk.Publish(f)
	}

	stats := ch.Stats()
	t.Logf("Dropped frames: %d", stats.Dropped)
	if stats.Dropped == 0 {
		t.Error("expected dropped frames > 0")
	}
}
