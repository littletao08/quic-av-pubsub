package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type FrameType uint8

const (
	FrameAudio       FrameType = 0x01
	FrameVideo       FrameType = 0x02
	FrameMessage     FrameType = 0x03
	FrameSigPublish  FrameType = 0x10
	FrameSigSubscribe FrameType = 0x11
	FrameSigUnsub    FrameType = 0x12
	FrameSigAck      FrameType = 0x13
	FrameSigErr      FrameType = 0x14
	FrameHeartbeat   FrameType = 0xFF
)

func (f FrameType) String() string {
	names := map[FrameType]string{
		FrameAudio: "AUDIO", FrameVideo: "VIDEO",
		FrameMessage: "MESSAGE", FrameSigPublish: "SIG_PUB",
		FrameSigSubscribe: "SIG_SUB", FrameSigUnsub: "SIG_UNSUB",
		FrameSigAck: "SIG_ACK", FrameSigErr: "SIG_ERR",
		FrameHeartbeat: "HEARTBEAT",
	}
	if n, ok := names[f]; ok {
		return n
	}
	return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(f))
}

const FrameHeaderSize = 23

type Frame struct {
	Type      FrameType
	ChannelID uint16
	Seq       uint32
	Timestamp int64
	Flags     uint8
	Payload   []byte
}

func (f *Frame) IsKeyFrame() bool {
	return f.Type == FrameVideo && f.Flags&0x01 != 0
}

func Now() int64 {
	return time.Now().UnixMicro()
}

func WriteFrame(w io.Writer, f *Frame) error {
	payLen := uint32(len(f.Payload))

	var header [FrameHeaderSize]byte
	header[0] = uint8(f.Type)
	binary.BigEndian.PutUint16(header[1:3], f.ChannelID)
	binary.BigEndian.PutUint32(header[3:7], f.Seq)
	binary.BigEndian.PutUint64(header[7:15], uint64(f.Timestamp))
	header[15] = f.Flags
	binary.BigEndian.PutUint32(header[19:23], payLen)

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if payLen > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

func ReadFrame(r io.Reader) (*Frame, error) {
	var header [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	f := &Frame{}
	f.Type = FrameType(header[0])
	f.ChannelID = binary.BigEndian.Uint16(header[1:3])
	f.Seq = binary.BigEndian.Uint32(header[3:7])
	f.Timestamp = int64(binary.BigEndian.Uint64(header[7:15]))
	f.Flags = header[15]
	payLen := binary.BigEndian.Uint32(header[19:23])

	if payLen > 0 {
		if payLen > 64*1024*1024 {
			return nil, fmt.Errorf("payload too large: %d bytes", payLen)
		}
		f.Payload = make([]byte, payLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}
	return f, nil
}

type TrackType string

const (
	TrackAudio   TrackType = "audio"
	TrackVideo   TrackType = "video"
	TrackMessage TrackType = "message"
	TrackAll     TrackType = "all"
)

type SigPublish struct {
	Channel string     `json:"channel"`
	Tracks  []TrackType `json:"tracks"`
}

type SigSubscribe struct {
	Channel string     `json:"channel"`
	Tracks  []TrackType `json:"tracks"`
}

type SigUnsub struct {
	Channel string `json:"channel"`
}

type SigAck struct {
	OK        bool   `json:"ok"`
	Channel   string `json:"channel"`
	ChannelID uint16 `json:"channel_id"`
	Message   string `json:"message,omitempty"`
}

type SigErr struct {
	Error   string `json:"error"`
	Channel string `json:"channel,omitempty"`
}

func MakeSigFrame(typ FrameType, payload any) (*Frame, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Frame{
		Type:      typ,
		Timestamp: Now(),
		Payload:   data,
	}, nil
}

func ParseSigPayload(f *Frame, dst any) error {
	return json.Unmarshal(f.Payload, dst)
}

var HeartbeatFrame = &Frame{Type: FrameHeartbeat}
