package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/littletao08/quic-av-pubsub/pkg/brutal"
	"github.com/littletao08/quic-av-pubsub/pkg/proto"
)

type ClientConfig struct {
	ServerAddr   string
	Insecure     bool
	TargetMbps   float64
	HeartbeatSec int
}

type Publisher struct {
	cfg    ClientConfig
	conn   quic.Connection
	logger *slog.Logger
	pacer  *brutal.BrutalPacer

	mu      sync.Mutex
	streams map[string]*publishStream
}

type publishStream struct {
	stream    quic.Stream
	channelID uint16
	mu        sync.Mutex
	seq       uint32
}

func (ps *publishStream) nextSeq() uint32 {
	return atomic.AddUint32(&ps.seq, 1) - 1
}

func NewPublisher(cfg ClientConfig, logger *slog.Logger) *Publisher {
	p := &Publisher{
		cfg:     cfg,
		logger:  logger,
		streams: make(map[string]*publishStream),
	}
	if cfg.TargetMbps > 0 {
		p.pacer = brutal.NewBrutalPacer(cfg.TargetMbps)
	}
	return p
}

func (p *Publisher) Connect(ctx context.Context) error {
	tlsConf := &tls.Config{
		InsecureSkipVerify: p.cfg.Insecure,
		NextProtos:         []string{"quic-pubsub/1"},
	}
	quicConf := &quic.Config{
		EnableDatagrams: true,
		KeepAlivePeriod: 10,
	}

	conn, err := quic.DialAddr(ctx, p.cfg.ServerAddr, tlsConf, quicConf)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	p.conn = conn
	p.logger.Info("publisher connected", "server", p.cfg.ServerAddr)

	if p.cfg.HeartbeatSec > 0 {
		go p.heartbeatLoop(ctx)
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, channelName string, tracks []proto.TrackType) (uint16, error) {
	p.mu.Lock()
	if ps, ok := p.streams[channelName]; ok {
		p.mu.Unlock()
		return ps.channelID, nil
	}
	p.mu.Unlock()

	stream, err := p.conn.OpenStreamSync(ctx)
	if err != nil {
		return 0, fmt.Errorf("open stream: %w", err)
	}

	req := proto.SigPublish{Channel: channelName, Tracks: tracks}
	sigFrame, _ := proto.MakeSigFrame(proto.FrameSigPublish, req)
	if err := proto.WriteFrame(stream, sigFrame); err != nil {
		stream.Close()
		return 0, fmt.Errorf("send sig_pub: %w", err)
	}

	ackFrame, err := proto.ReadFrame(stream)
	if err != nil {
		stream.Close()
		return 0, fmt.Errorf("read ack: %w", err)
	}
	if ackFrame.Type == proto.FrameSigErr {
		var e proto.SigErr
		proto.ParseSigPayload(ackFrame, &e)
		stream.Close()
		return 0, fmt.Errorf("server error: %s", e.Error)
	}
	var ack proto.SigAck
	proto.ParseSigPayload(ackFrame, &ack)
	if !ack.OK {
		stream.Close()
		return 0, fmt.Errorf("publish rejected: %s", ack.Message)
	}

	ps := &publishStream{
		stream:    stream,
		channelID: ack.ChannelID,
	}

	p.mu.Lock()
	p.streams[channelName] = ps
	p.mu.Unlock()

	p.logger.Info("channel published",
		"channel", channelName,
		"channel_id", ack.ChannelID,
	)
	return ack.ChannelID, nil
}

func (p *Publisher) SendAudio(channelName string, payload []byte) error {
	return p.sendFrame(channelName, proto.FrameAudio, 0, payload)
}

func (p *Publisher) SendVideo(channelName string, payload []byte, isKeyFrame bool) error {
	var flags uint8
	if isKeyFrame {
		flags = 0x01
	}
	return p.sendFrame(channelName, proto.FrameVideo, flags, payload)
}

func (p *Publisher) SendMessage(channelName string, payload []byte) error {
	return p.sendFrame(channelName, proto.FrameMessage, 0, payload)
}

func (p *Publisher) sendFrame(channelName string, typ proto.FrameType, flags uint8, payload []byte) error {
	p.mu.Lock()
	ps, ok := p.streams[channelName]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("channel %q not published", channelName)
	}

	if p.pacer != nil {
		p.pacer.Wait(proto.FrameHeaderSize + len(payload))
	}

	f := &proto.Frame{
		Type:      typ,
		ChannelID: ps.channelID,
		Seq:       ps.nextSeq(),
		Timestamp: proto.Now(),
		Flags:     flags,
		Payload:   payload,
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	return proto.WriteFrame(ps.stream, f)
}

func (p *Publisher) Close() {
	p.conn.CloseWithError(0, "publisher close")
}

func (p *Publisher) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(time.Duration(p.cfg.HeartbeatSec) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.mu.Lock()
			for _, ps := range p.streams {
				ps.mu.Lock()
				proto.WriteFrame(ps.stream, proto.HeartbeatFrame)
				ps.mu.Unlock()
			}
			p.mu.Unlock()
		}
	}
}

type FrameCallback func(track proto.TrackType, frame *proto.Frame)

type Subscriber struct {
	cfg      ClientConfig
	conn     quic.Connection
	logger   *slog.Logger
	callback FrameCallback
}

func NewSubscriber(cfg ClientConfig, cb FrameCallback, logger *slog.Logger) *Subscriber {
	return &Subscriber{cfg: cfg, callback: cb, logger: logger}
}

func (s *Subscriber) Connect(ctx context.Context) error {
	tlsConf := &tls.Config{
		InsecureSkipVerify: s.cfg.Insecure,
		NextProtos:         []string{"quic-pubsub/1"},
	}
	conn, err := quic.DialAddr(ctx, s.cfg.ServerAddr, tlsConf, &quic.Config{
		EnableDatagrams: true,
		KeepAlivePeriod: 10,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	s.conn = conn
	s.logger.Info("subscriber connected", "server", s.cfg.ServerAddr)
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, channelName string, tracks []proto.TrackType) error {
	sigStream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open sig stream: %w", err)
	}

	req := proto.SigSubscribe{Channel: channelName, Tracks: tracks}
	sigFrame, _ := proto.MakeSigFrame(proto.FrameSigSubscribe, req)
	if err := proto.WriteFrame(sigStream, sigFrame); err != nil {
		sigStream.Close()
		return fmt.Errorf("send sig_sub: %w", err)
	}

	ackFrame, err := proto.ReadFrame(sigStream)
	if err != nil {
		sigStream.Close()
		return err
	}
	if ackFrame.Type == proto.FrameSigErr {
		var e proto.SigErr
		proto.ParseSigPayload(ackFrame, &e)
		sigStream.Close()
		return fmt.Errorf("server error: %s", e.Error)
	}
	var ack proto.SigAck
	proto.ParseSigPayload(ackFrame, &ack)

	s.logger.Info("subscribe acked",
		"channel", ack.Channel,
		"channel_id", ack.ChannelID,
	)

	go s.acceptPushStreams(ctx)
	return nil
}

func (s *Subscriber) acceptPushStreams(ctx context.Context) {
	for {
		uniStream, err := s.conn.AcceptUniStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("accept uni stream error", "err", err)
			return
		}
		go s.readPushStream(ctx, uniStream)
	}
}

func (s *Subscriber) readPushStream(ctx context.Context, stream quic.ReceiveStream) {
	announceFrame, err := proto.ReadFrame(stream)
	if err != nil {
		return
	}
	var meta map[string]string
	json.Unmarshal(announceFrame.Payload, &meta)
	track := proto.TrackType(meta["track"])

	s.logger.Info("push stream started",
		"stream_id", stream.StreamID(),
		"track", track,
	)

	jb := newJitterBuffer(256)

	for {
		f, err := proto.ReadFrame(stream)
		if err != nil {
			if err != io.EOF {
				s.logger.Debug("push stream closed", "track", track, "err", err)
			}
			return
		}
		if f.Type == proto.FrameHeartbeat {
			continue
		}
		jb.push(f)
		for {
			ordered := jb.pop()
			if ordered == nil {
				break
			}
			s.callback(track, ordered)
		}
	}
}

func (s *Subscriber) Close() {
	s.conn.CloseWithError(0, "subscriber close")
}

type jitterBuffer struct {
	buf     []*proto.Frame
	maxSize int
	nextSeq uint32
	started bool
}

func newJitterBuffer(maxSize int) *jitterBuffer {
	return &jitterBuffer{
		buf:     make([]*proto.Frame, 0, maxSize),
		maxSize: maxSize,
	}
}

func (jb *jitterBuffer) push(f *proto.Frame) {
	if len(jb.buf) >= jb.maxSize {
		jb.nextSeq = jb.buf[0].Seq + 1
		jb.buf = jb.buf[1:]
	}
	i := len(jb.buf)
	jb.buf = append(jb.buf, f)
	for i > 0 && jb.buf[i-1].Seq > jb.buf[i].Seq {
		jb.buf[i-1], jb.buf[i] = jb.buf[i], jb.buf[i-1]
		i--
	}
}

func (jb *jitterBuffer) pop() *proto.Frame {
	if len(jb.buf) == 0 {
		return nil
	}
	if !jb.started {
		jb.nextSeq = jb.buf[0].Seq
		jb.started = true
	}
	head := jb.buf[0]
	if head.Seq == jb.nextSeq {
		jb.buf = jb.buf[1:]
		jb.nextSeq++
		return head
	}
	if len(jb.buf) > jb.maxSize/2 {
		jb.nextSeq = head.Seq
		jb.buf = jb.buf[1:]
		jb.nextSeq++
		return head
	}
	return nil
}
