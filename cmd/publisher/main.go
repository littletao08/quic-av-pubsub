package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/yourorg/quic-pubsub/pkg/proto"
	"github.com/yourorg/quic-pubsub/pkg/transport"
)

func main() {
	server := flag.String("server", "127.0.0.1:4430", "服务端地址")
	channel := flag.String("ch", "room:101", "频道名称")
	mbps := flag.Float64("bw", 10.0, "Brutal 目标带宽 Mbps")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	pub := transport.NewPublisher(transport.ClientConfig{
		ServerAddr:    *server,
		Insecure:      true,
		TargetMbps:    *mbps,
		HeartbeatSec:  5,
	}, logger)

	ctx := context.Background()
	if err := pub.Connect(ctx); err != nil {
		logger.Error("connect failed", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	channelID, err := pub.Publish(ctx, *channel, []proto.TrackType{
		proto.TrackAudio, proto.TrackVideo, proto.TrackMessage,
	})
	if err != nil {
		logger.Error("publish failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Channel %q registered, ID=%d\n", *channel, channelID)

	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		fakeAudio := make([]byte, 80)
		for range ticker.C {
			if err := pub.SendAudio(*channel, fakeAudio); err != nil {
				logger.Warn("send audio error", "err", err)
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(33 * time.Millisecond)
		defer ticker.Stop()
		frameIdx := 0
		for range ticker.C {
			isKey := frameIdx%30 == 0
			pkt := make([]byte, 15000)
			if isKey {
				pkt = make([]byte, 80000)
			}
			if err := pub.SendVideo(*channel, pkt, isKey); err != nil {
				logger.Warn("send video error", "err", err)
				return
			}
			frameIdx++
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		idx := 0
		for t := range ticker.C {
			msg, _ := json.Marshal(map[string]any{
				"type":   "chat",
				"sender": "publisher-1",
				"text":   fmt.Sprintf("Hello from publisher, ts=%d", t.UnixMilli()),
				"seq":    idx,
			})
			if err := pub.SendMessage(*channel, msg); err != nil {
				logger.Warn("send message error", "err", err)
				return
			}
			idx++
		}
	}()

	time.Sleep(60 * time.Second)
	fmt.Println("Publisher done.")
}
