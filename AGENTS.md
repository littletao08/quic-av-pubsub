# AI Context for QUIC AV Pub/Sub System

## Project Overview

QUIC-native audio/video publish/subscribe system. Single UDP port, automatic channel creation, Brutal congestion control. Built on `quic-go` v0.44.

## Architecture

```
Publisher --[QUIC Streams]--> Server --[QUIC Push Streams]--> Subscriber
                                |
                             Broker (in-memory channel pub/sub)
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| Channel | A named pub/sub channel (e.g. "room:101"), auto-created on first publish |
| Track | Media type within a channel: audio, video, or message |
| Stream | QUIC stream (bidirectional for signaling, unidirectional for data push) |
| Frame | 23-byte header + payload; typed by FrameType |
| Jitter Buffer | Per-push-stream reorder buffer using insertion sort on Seq |

### QUIC Stream Allocation

**Publisher side** (client-initiated):
- Stream 0 (bidirectional, even): Signaling + data write for first channel
- Additional `OpenStreamSync()` calls for each channel

**Subscriber side** (server-initiated):
- Stream 0 (bidirectional, even): Signaling
- Streams 1,3,5,... (unidirectional, odd): Server push per track

### Protocol Frame (23-byte header)

```
[0]    Type       (1 byte)  - FrameType enum
[1-2]  ChannelID  (2 bytes) - Server-assigned channel ID
[3-6]  Seq        (4 bytes) - Auto-incrementing sequence number
[7-14] Timestamp  (8 bytes) - Unix microseconds
[15]   Flags      (1 byte)  - bit0=KeyFrame for video
[16-18] Reserved  (3 bytes) - zero-filled
[19-22] PayloadLen (4 bytes) - big-endian payload length
[23+]  Payload    (variable)
```

### FrameType Enum

| Value | Name | Use |
|-------|------|-----|
| 0x01 | AUDIO | Audio data (Opus) |
| 0x02 | VIDEO | Video data (H.264/AV1), Flags&0x01 = KeyFrame |
| 0x03 | MESSAGE | Chat/binary message |
| 0x10 | SIG_PUB | Publish request |
| 0x11 | SIG_SUB | Subscribe request |
| 0x12 | SIG_UNSUB | Unsubscribe |
| 0x13 | SIG_ACK | Server ACK |
| 0x14 | SIG_ERR | Server error |
| 0xFF | HEARTBEAT | Keep-alive (empty payload) |

## Directory Structure

```
cmd/
  server/main.go       - Server entry point
  publisher/main.go    - Publisher example
  subscriber/main.go   - Subscriber example
pkg/
  proto/frame.go       - Frame types, codec (ReadFrame/WriteFrame)
  broker/channel.go    - Channel & Subscriber types
  broker/broker.go     - Broker: register, subscribe, publish, stats
  transport/server.go  - QUIC server: accept, signal handling, push
  transport/client.go  - QUIC client: Publisher & Subscriber + JitterBuffer
  brutal/cc.go         - Application-layer Brutal token bucket pacer
certs/
  gen.sh               - Self-signed ECDSA TLS cert generator
  server.crt / server.key
```

## Dependencies

- `github.com/quic-go/quic-go` v0.44 (only dependency)
- Go 1.22+ (uses `atomic.Int64`, `atomic.Uint32`, `log/slog`)

## Build & Run

```bash
# Generate TLS certs (first time)
bash certs/gen.sh

# Build all binaries
go build -o bin/server ./cmd/server
go build -o bin/publisher ./cmd/publisher
go build -o bin/subscriber ./cmd/subscriber

# Run server
./bin/server -addr :4430 -v

# Run subscriber (separate terminal)
./bin/subscriber -server 127.0.0.1:4430 -ch room:101

# Run publisher (separate terminal)
./bin/publisher -server 127.0.0.1:4430 -ch room:101 -bw 15
```

## Key Flow

1. Publisher connects, sends `SIG_PUB` on stream, gets `SIG_ACK` with `channel_id`
2. Publisher writes media frames on same stream (audio 20ms, video 33ms, message 1s)
3. Subscriber connects, sends `SIG_SUB`, gets `SIG_ACK`
4. Server opens `UniStream`s per track, sends track announce frame, then pushes frames
5. Subscriber reads push streams through JitterBuffer for reordering

## Known Issues / Future Work

1. **No audio/video encoding** - publisher sends raw bytes; real encoding needed
2. **Brutal Pacer is application-level** - for production, use hysteria2's quic-go fork with kernel-level Brutal
3. **No TLS config in subscriber** - currently only `InsecureSkipVerify` is supported
4. **Single binary mode** - could add `--mode server|pub|sub` flag
5. **Prometheus metrics** - `AllStats()` is ready; wire up /metrics endpoint
6. **Graceful degradation** - on subscriber disconnect, server should detect via Stream error
7. **Multi-channel subscriber** - currently one `Subscribe()` call per channel; could batch
8. **Benchmark** - `pkg/proto/frame_test.go` could use `go test -bench=.`

## Current Development Status

- [x] Protocol frame definition and codec (ReadFrame/WriteFrame)
- [x] Broker: channel registry, pub/sub, cleanup
- [x] Brutal congestion control (application-layer token bucket)
- [x] QUIC server: accept connections, handle signaling, push streams
- [x] QUIC client: Publisher with heartbeat, Subscriber with jitter buffer
- [x] Server main, Publisher main, Subscriber main
- [x] TLS cert generation
- [x] Go module with quic-go dependency
- [ ] Performance tests / benchmarks
- [ ] Prometheus metrics endpoint
- [ ] Dockerfile / docker-compose
- [ ] Integration tests
