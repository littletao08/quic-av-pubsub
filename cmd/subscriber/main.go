package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/yourorg/quic-pubsub/pkg/proto"
	"github.com/yourorg/quic-pubsub/pkg/transport"
)

var (
	audioFrames atomic.Int64
	videoFrames atomic.Int64
	msgFrames   atomic.Int64
	audioBytes  atomic.Int64
	videoBytes  atomic.Int64
	lastVideoTs atomic.Int64
)

func main() {
	server := flag.String("server", "127.0.0.1:4430", "服务端地址")
	channel := flag.String("ch", "room:101", "频道名称")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	sub := transport.NewSubscriber(transport.ClientConfig{
		ServerAddr: *server,
		Insecure:   true,
	}, onFrame, logger)

	ctx := context.Background()
	if err := sub.Connect(ctx); err != nil {
		logger.Error("connect failed", "err", err)
		os.Exit(1)
	}
	defer sub.Close()

	if err := sub.Subscribe(ctx, *channel, []proto.TrackType{proto.TrackAll}); err != nil {
		logger.Error("subscribe failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Subscribed to channel %q\n", *channel)

	go printStats()

	select {}
}

func onFrame(track proto.TrackType, f *proto.Frame) {
	switch track {
	case proto.TrackAudio:
		audioFrames.Add(1)
		audioBytes.Add(int64(len(f.Payload)))

	case proto.TrackVideo:
		videoFrames.Add(1)
		videoBytes.Add(int64(len(f.Payload)))
		latency := time.Now().UnixMicro() - f.Timestamp
		lastVideoTs.Store(latency)
		if f.IsKeyFrame() {
			fmt.Printf("  KeyFrame seq=%d latency=%.1fms\n",
				f.Seq, float64(latency)/1000)
		}

	case proto.TrackMessage:
		msgFrames.Add(1)
		var msg map[string]any
		if err := json.Unmarshal(f.Payload, &msg); err == nil {
			fmt.Printf("  Message: %v\n", msg["text"])
		}
	}
}

func printStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	prevAudio := int64(0)
	prevVideo := int64(0)

	for range ticker.C {
		af := audioFrames.Load()
		vf := videoFrames.Load()
		mf := msgFrames.Load()
		vb := videoBytes.Load()
		latency := lastVideoTs.Load()

		videoBpsMbps := float64(vb-prevVideo) / 5 / 1024 * 8

		fmt.Printf("\n--- Stats ---\n")
		fmt.Printf("  Audio  : %d frames  (%.1f kbps)\n", af, float64(af-prevAudio)*80*8/5/1024)
		fmt.Printf("  Video  : %d frames  (%.2f Mbps)\n", vf, videoBpsMbps/1024)
		fmt.Printf("  Message: %d msgs\n", mf)
		fmt.Printf("  Video latency (last): %.1f ms\n", float64(latency)/1000)
		fmt.Println("-------------")

		prevAudio = af
		prevVideo = vb
	}
}
