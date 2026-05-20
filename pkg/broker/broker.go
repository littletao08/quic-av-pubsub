package broker

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourorg/quic-pubsub/pkg/proto"
)

type Broker struct {
	mu       sync.RWMutex
	channels map[string]*Channel
	byID     map[uint16]*Channel
	nextID   atomic.Uint32

	logger *slog.Logger
}

func NewBroker(logger *slog.Logger) *Broker {
	return &Broker{
		channels: make(map[string]*Channel),
		byID:     make(map[uint16]*Channel),
		logger:   logger,
	}
}

func (b *Broker) RegisterChannel(name string) *Channel {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.channels[name]; ok {
		b.logger.Info("channel reused", "name", name, "id", ch.ID)
		return ch
	}

	id := uint16(b.nextID.Add(1))
	ch := newChannel(name, id)
	b.channels[name] = ch
	b.byID[id] = ch

	b.logger.Info("channel created", "name", name, "id", id)
	return ch
}

func (b *Broker) GetChannel(name string) (*Channel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.channels[name]
	return ch, ok
}

func (b *Broker) GetChannelByID(id uint16) (*Channel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.byID[id]
	return ch, ok
}

func (b *Broker) UnregisterChannel(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.channels[name]
	if !ok {
		return
	}

	ch.mu.Lock()
	for _, m := range ch.subs {
		for _, sub := range m {
			sub.Close()
		}
	}
	ch.mu.Unlock()

	delete(b.channels, name)
	delete(b.byID, ch.ID)
	b.logger.Info("channel removed", "name", name)
}

func (b *Broker) Subscribe(channelName string, sub *Subscriber) error {
	ch, ok := b.GetChannel(channelName)
	if !ok {
		return fmt.Errorf("channel %q not found", channelName)
	}
	ch.AddSubscriber(sub)
	b.logger.Info("subscriber added",
		"channel", channelName,
		"conn", sub.ID,
		"track", sub.TrackTyp,
	)
	return nil
}

func (b *Broker) Unsubscribe(channelName, connID string, track proto.TrackType) {
	if ch, ok := b.GetChannel(channelName); ok {
		ch.RemoveSubscriber(connID, track)
	}
}

func (b *Broker) CleanupConn(connID string) {
	b.mu.RLock()
	chs := make([]*Channel, 0, len(b.channels))
	for _, ch := range b.channels {
		chs = append(chs, ch)
	}
	b.mu.RUnlock()

	for _, ch := range chs {
		ch.RemoveAllSubscribers(connID)
	}
	b.logger.Info("connection cleaned up", "conn", connID)
}

func (b *Broker) Publish(f *proto.Frame) {
	ch, ok := b.GetChannelByID(f.ChannelID)
	if !ok {
		return
	}
	ch.Publish(f)
}

func (b *Broker) AllStats() []ChannelStats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stats := make([]ChannelStats, 0, len(b.channels))
	for _, ch := range b.channels {
		stats = append(stats, ch.Stats())
	}
	return stats
}

func (b *Broker) StartStatsLogger(interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			for _, s := range b.AllStats() {
				b.logger.Info("channel stats",
					"name", s.Name, "id", s.ID,
					"subs", s.Subscribers,
					"audio", s.AudioFrames,
					"video", s.VideoFrames,
					"msg", s.MsgFrames,
					"dropped", s.Dropped,
				)
			}
		}
	}()
}
