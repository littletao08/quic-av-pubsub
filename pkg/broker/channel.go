package broker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/littletao08/quic-av-pubsub/pkg/proto"
)

type Subscriber struct {
	ID       string
	TrackTyp proto.TrackType
	Ch       chan *proto.Frame
	Done     chan struct{}
}

func NewSubscriber(id string, track proto.TrackType, bufSize int) *Subscriber {
	return &Subscriber{
		ID:       id,
		TrackTyp: track,
		Ch:       make(chan *proto.Frame, bufSize),
		Done:     make(chan struct{}),
	}
}

func (s *Subscriber) Send(f *proto.Frame) bool {
	select {
	case s.Ch <- f:
		return true
	default:
		return false
	}
}

func (s *Subscriber) Close() {
	select {
	case <-s.Done:
	default:
		close(s.Done)
	}
}

type Channel struct {
	Name      string
	ID        uint16
	CreatedAt time.Time

	mu   sync.RWMutex
	subs map[proto.TrackType]map[string]*Subscriber

	audioFrames atomic.Int64
	videoFrames atomic.Int64
	msgFrames   atomic.Int64
	dropped     atomic.Int64
}

func newChannel(name string, id uint16) *Channel {
	c := &Channel{
		Name:      name,
		ID:        id,
		CreatedAt: time.Now(),
		subs:      make(map[proto.TrackType]map[string]*Subscriber),
	}
	for _, t := range []proto.TrackType{
		proto.TrackAudio, proto.TrackVideo, proto.TrackMessage,
	} {
		c.subs[t] = make(map[string]*Subscriber)
	}
	return c
}

func (c *Channel) AddSubscriber(sub *Subscriber) {
	c.mu.Lock()
	defer c.mu.Unlock()
	track := sub.TrackTyp
	if _, ok := c.subs[track]; !ok {
		c.subs[track] = make(map[string]*Subscriber)
	}
	c.subs[track][sub.ID] = sub
}

func (c *Channel) RemoveSubscriber(connID string, track proto.TrackType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.subs[track]; ok {
		if sub, exists := m[connID]; exists {
			sub.Close()
			delete(m, connID)
		}
	}
}

func (c *Channel) RemoveAllSubscribers(connID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.subs {
		if sub, ok := m[connID]; ok {
			sub.Close()
			delete(m, connID)
		}
	}
}

func (c *Channel) Publish(f *proto.Frame) {
	var track proto.TrackType
	switch f.Type {
	case proto.FrameAudio:
		track = proto.TrackAudio
		c.audioFrames.Add(1)
	case proto.FrameVideo:
		track = proto.TrackVideo
		c.videoFrames.Add(1)
	case proto.FrameMessage:
		track = proto.TrackMessage
		c.msgFrames.Add(1)
	default:
		return
	}

	c.mu.RLock()
	subs := c.subs[track]
	c.mu.RUnlock()

	for _, sub := range subs {
		if !sub.Send(f) {
			c.dropped.Add(1)
		}
	}
}

type ChannelStats struct {
	Name        string
	ID          uint16
	Subscribers int
	AudioFrames int64
	VideoFrames int64
	MsgFrames   int64
	Dropped     int64
}

func (c *Channel) Stats() ChannelStats {
	c.mu.RLock()
	subCount := 0
	for _, m := range c.subs {
		subCount += len(m)
	}
	c.mu.RUnlock()

	return ChannelStats{
		Name:        c.Name,
		ID:          c.ID,
		Subscribers: subCount,
		AudioFrames: c.audioFrames.Load(),
		VideoFrames: c.videoFrames.Load(),
		MsgFrames:   c.msgFrames.Load(),
		Dropped:     c.dropped.Load(),
	}
}
