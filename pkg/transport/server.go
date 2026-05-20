package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"

	"github.com/yourorg/quic-pubsub/pkg/broker"
	"github.com/yourorg/quic-pubsub/pkg/proto"
)

type ServerConfig struct {
	ListenAddr string
	TLSCert    string
	TLSKey     string
}

type Server struct {
	cfg    ServerConfig
	broker *broker.Broker
	logger *slog.Logger

	listener *quic.Listener
	conns    sync.Map
	connSeq  atomic.Uint64
}

func NewServer(cfg ServerConfig, b *broker.Broker, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, broker: b, logger: logger}
}

func (s *Server) Start(ctx context.Context) error {
	cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
	if err != nil {
		return fmt.Errorf("load cert: %w", err)
	}

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-pubsub/1"},
	}
	quicConf := &quic.Config{
		EnableDatagrams:      true,
		MaxIncomingStreams:    1000,
		MaxIncomingUniStreams: 1000,
		KeepAlivePeriod:      10,
	}

	ln, err := quic.ListenAddr(s.cfg.ListenAddr, tlsConf, quicConf)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = ln
	s.logger.Info("QUIC server listening", "addr", s.cfg.ListenAddr)

	go s.acceptLoop(ctx)
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error("accept error", "err", err)
			continue
		}
		id := fmt.Sprintf("conn-%d", s.connSeq.Add(1))
		cs := &connState{
			id:     id,
			conn:   conn,
			broker: s.broker,
			logger: s.logger.With("conn", id),
		}
		s.conns.Store(id, cs)
		go func() {
			cs.handle(ctx)
			s.conns.Delete(id)
			s.broker.CleanupConn(id)
		}()
	}
}

func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

type connState struct {
	id     string
	conn   quic.Connection
	broker *broker.Broker
	logger *slog.Logger

	mu       sync.Mutex
	channels map[string]*broker.Channel
}

func (cs *connState) handle(ctx context.Context) {
	cs.channels = make(map[string]*broker.Channel)
	defer cs.conn.CloseWithError(0, "bye")

	for {
		stream, err := cs.conn.AcceptStream(ctx)
		if err != nil {
			cs.logger.Info("connection closed", "err", err)
			return
		}
		go cs.handleStream(ctx, stream)
	}
}

func (cs *connState) handleStream(ctx context.Context, stream quic.Stream) {
	f, err := proto.ReadFrame(stream)
	if err != nil {
		cs.logger.Warn("read first frame failed", "err", err)
		stream.Close()
		return
	}

	switch f.Type {
	case proto.FrameSigPublish:
		cs.handlePublishSignal(ctx, stream, f)
	case proto.FrameSigSubscribe:
		cs.handleSubscribeSignal(ctx, stream, f)
	case proto.FrameAudio, proto.FrameVideo, proto.FrameMessage:
		cs.handleDataStream(ctx, stream, f)
	case proto.FrameHeartbeat:
		proto.WriteFrame(stream, proto.HeartbeatFrame)
	default:
		cs.logger.Warn("unknown frame type on new stream", "type", f.Type)
		stream.Close()
	}
}

func (cs *connState) handlePublishSignal(ctx context.Context, stream quic.Stream, sigFrame *proto.Frame) {
	var req proto.SigPublish
	if err := proto.ParseSigPayload(sigFrame, &req); err != nil {
		writeErrAck(stream, "invalid publish payload")
		return
	}
	if req.Channel == "" {
		writeErrAck(stream, "channel name required")
		return
	}

	ch := cs.broker.RegisterChannel(req.Channel)

	cs.mu.Lock()
	cs.channels[req.Channel] = ch
	cs.mu.Unlock()

	ack := &proto.SigAck{
		OK:        true,
		Channel:   req.Channel,
		ChannelID: ch.ID,
		Message:   "channel ready",
	}
	ackFrame, _ := proto.MakeSigFrame(proto.FrameSigAck, ack)
	ackFrame.ChannelID = ch.ID
	if err := proto.WriteFrame(stream, ackFrame); err != nil {
		cs.logger.Error("write ack failed", "err", err)
		return
	}

	cs.logger.Info("publish registered",
		"channel", req.Channel,
		"channel_id", ch.ID,
		"tracks", req.Tracks,
	)

	cs.readDataLoop(ctx, stream, ch)
}

func (cs *connState) handleSubscribeSignal(ctx context.Context, stream quic.Stream, sigFrame *proto.Frame) {
	var req proto.SigSubscribe
	if err := proto.ParseSigPayload(sigFrame, &req); err != nil {
		writeErrAck(stream, "invalid subscribe payload")
		return
	}
	if req.Channel == "" {
		writeErrAck(stream, "channel name required")
		return
	}

	ch, ok := cs.broker.GetChannel(req.Channel)
	if !ok {
		writeErrAck(stream, fmt.Sprintf("channel %q not found", req.Channel))
		return
	}

	ack := &proto.SigAck{
		OK: true, Channel: req.Channel, ChannelID: ch.ID,
		Message: "subscribed",
	}
	ackFrame, _ := proto.MakeSigFrame(proto.FrameSigAck, ack)
	ackFrame.ChannelID = ch.ID
	proto.WriteFrame(stream, ackFrame)

	tracks := req.Tracks
	if len(tracks) == 0 {
		tracks = []proto.TrackType{proto.TrackAll}
	}

	expanded := expandTracks(tracks)

	for _, track := range expanded {
		sub := broker.NewSubscriber(cs.id+":"+string(track), track, 512)
		if err := cs.broker.Subscribe(req.Channel, sub); err != nil {
			cs.logger.Warn("subscribe failed", "err", err)
			continue
		}
		go cs.pushLoop(ctx, sub, ch.ID)
	}
}

func (cs *connState) handleDataStream(ctx context.Context, stream quic.Stream, firstFrame *proto.Frame) {
	cs.broker.Publish(firstFrame)
	cs.readDataLoop(ctx, stream, nil)
}

func (cs *connState) readDataLoop(ctx context.Context, stream quic.Stream, ch *broker.Channel) {
	for {
		f, err := proto.ReadFrame(stream)
		if err != nil {
			cs.logger.Debug("data stream closed", "err", err)
			return
		}
		if f.Type == proto.FrameHeartbeat {
			continue
		}
		cs.broker.Publish(f)
	}
}

func (cs *connState) pushLoop(ctx context.Context, sub *broker.Subscriber, channelID uint16) {
	defer sub.Close()

	pushStream, err := cs.conn.OpenUniStreamSync(ctx)
	if err != nil {
		cs.logger.Error("open push stream failed", "err", err)
		return
	}
	defer pushStream.Close()

	cs.logger.Info("push stream opened", "track", sub.TrackTyp, "stream_id", pushStream.StreamID())

	trackAnnounce := &proto.Frame{
		Type:      proto.FrameSigAck,
		ChannelID: channelID,
		Timestamp: proto.Now(),
		Payload:   mustMarshalJSON(map[string]string{"track": string(sub.TrackTyp)}),
	}
	if err := proto.WriteFrame(pushStream, trackAnnounce); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Done:
			return
		case f, ok := <-sub.Ch:
			if !ok {
				return
			}
			if err := proto.WriteFrame(pushStream, f); err != nil {
				cs.logger.Warn("push write error", "err", err)
				return
			}
		}
	}
}

func writeErrAck(stream quic.Stream, errMsg string) {
	errFrame, _ := proto.MakeSigFrame(proto.FrameSigErr, proto.SigErr{Error: errMsg})
	proto.WriteFrame(stream, errFrame)
}

func expandTracks(tracks []proto.TrackType) []proto.TrackType {
	result := make([]proto.TrackType, 0, 3)
	seen := map[proto.TrackType]bool{}
	for _, t := range tracks {
		if t == proto.TrackAll {
			for _, tt := range []proto.TrackType{
				proto.TrackAudio, proto.TrackVideo, proto.TrackMessage,
			} {
				if !seen[tt] {
					result = append(result, tt)
					seen[tt] = true
				}
			}
		} else {
			if !seen[t] {
				result = append(result, t)
				seen[t] = true
			}
		}
	}
	return result
}

func mustMarshalJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
