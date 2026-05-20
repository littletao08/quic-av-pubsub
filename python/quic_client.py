"""QUIC AV Pub/Sub Python Client

Requires: pip install aioquic

Usage:
    # Publish to public test server
    python quic_client.py publish --server arm2.pvpv.bid:4430 --ch room:101

    # Subscribe from public test server
    python quic_client.py subscribe --server arm2.pvpv.bid:4430 --ch room:101

    # Subscribe with stats
    python quic_client.py subscribe --server arm2.pvpv.bid:4430 --ch room:101 --stats
"""

import argparse
import asyncio
import json
import struct
import time
import sys
from dataclasses import dataclass
from typing import Optional, Callable

from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, HandshakeCompleted

# ── Protocol Constants ──────────────────────────────────────────
FRAME_HEADER_SIZE = 23

AUDIO       = 0x01
VIDEO       = 0x02
MESSAGE     = 0x03
SIG_PUB     = 0x10
SIG_SUB     = 0x11
SIG_UNSUB   = 0x12
SIG_ACK     = 0x13
SIG_ERR     = 0x14
HEARTBEAT   = 0xFF

TRACK_NAMES = {AUDIO: "audio", VIDEO: "video", MESSAGE: "message"}
FRAME_NAMES = {
    AUDIO: "AUDIO", VIDEO: "VIDEO", MESSAGE: "MESSAGE",
    SIG_PUB: "SIG_PUB", SIG_SUB: "SIG_SUB", SIG_ACK: "SIG_ACK",
    SIG_ERR: "SIG_ERR", HEARTBEAT: "HEARTBEAT",
}

# ── Frame Codec ─────────────────────────────────────────────────
@dataclass
class Frame:
    type: int
    channel_id: int
    seq: int
    timestamp: int
    flags: int
    payload: bytes

    def __repr__(self):
        name = FRAME_NAMES.get(self.type, f"0x{self.type:02X}")
        return f"Frame({name}, ch={self.channel_id}, seq={self.seq})"

    def encode(self) -> bytes:
        hdr = struct.pack("!BHIqB3sI",
            self.type, self.channel_id, self.seq,
            self.timestamp, self.flags, b"\x00\x00\x00",
            len(self.payload))
        return hdr + self.payload

    @staticmethod
    def decode(data: bytes) -> "Frame":
        if len(data) < FRAME_HEADER_SIZE:
            raise ValueError(f"frame too short: {len(data)}B")
        typ, ch_id, seq, ts, flags, _, pay_len = struct.unpack(
            "!BHIqB3sI", data[:FRAME_HEADER_SIZE])
        payload = data[FRAME_HEADER_SIZE:FRAME_HEADER_SIZE + pay_len]
        return Frame(typ, ch_id, seq, ts, flags, payload)

    @staticmethod
    def make_sig(typ: int, body: dict) -> "Frame":
        return Frame(typ, 0, 0, now_us(), 0, json.dumps(body).encode())

    @staticmethod
    def heartbeat() -> "Frame":
        return Frame(HEARTBEAT, 0, 0, 0, 0, b"")


def now_us() -> int:
    return int(time.time() * 1_000_000)


def parse_sig(frame: Frame) -> dict:
    return json.loads(frame.payload)


# ── Async Frame Reader ──────────────────────────────────────────
class FrameReader:
    """Reads frames from an asyncio.StreamReader."""

    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader

    async def read_frame(self) -> Frame:
        hdr = await self._reader.readexactly(FRAME_HEADER_SIZE)
        pay_len = struct.unpack("!I", hdr[19:23])[0]
        payload = b""
        if pay_len > 0:
            if pay_len > 64 * 1024 * 1024:
                raise ValueError(f"payload too large: {pay_len}")
            payload = await self._reader.readexactly(pay_len)
        typ, ch_id, seq, ts, flags, _, _ = struct.unpack(
            "!BHIqB3sI", hdr)
        return Frame(typ, ch_id, seq, ts, flags, payload)


# ── Publisher ───────────────────────────────────────────────────
async def run_publisher(server: str, channel: str, insecure: bool, bw: float):
    host, port = parse_addr(server)
    config = QuicConfiguration(is_client=True)
    config.verify_mode = False if insecure else True

    async with connect(host, port, configuration=config) as conn:
        print(f"[pub]  connected to {server}")

        stream = conn.create_stream()
        reader, writer = stream
        frame_reader = FrameReader(reader)

        # Send publish request
        sig = Frame.make_sig(SIG_PUB, {"channel": channel, "tracks": ["audio", "video", "message"]})
        writer.write(sig.encode())
        await writer.drain()

        # Read ACK
        ack = await frame_reader.read_frame()
        if ack.type == SIG_ERR:
            print(f"[pub]  server error: {parse_sig(ack)}")
            return
        print(f"[pub]  channel '{channel}' ready")

        # Publish mixed audio/video/message for 60s
        audio_payload = b"\x00" * 80       # Opus 20ms @ 32kbps
        video_payload = b"\x00" * 15000    # ~15KB per frame
        keyframe_payload = b"\x00" * 80000 # I-frame ~80KB
        msg_payload = json.dumps({
            "type": "chat", "sender": "py-pub",
            "text": "hello from python"
        }).encode()

        start = time.time()
        audio_seq = video_seq = msg_seq = 0
        frame_count = 0

        while time.time() - start < 60:
            now = now_us()

            # Audio: 50fps (every 20ms)
            audio_seq += 1
            f = Frame(AUDIO, ack.channel_id, audio_seq, now, 0, audio_payload)
            writer.write(f.encode())

            # Video: ~30fps (every 33ms)
            if frame_count % 33 == 0:
                video_seq += 1
                is_key = video_seq % 30 == 0
                payload = keyframe_payload if is_key else video_payload
                f = Frame(VIDEO, ack.channel_id, video_seq, now,
                          1 if is_key else 0, payload)
                writer.write(f.encode())

            # Message: every 1s
            if frame_count % 50 == 0:
                msg_seq += 1
                f = Frame(MESSAGE, ack.channel_id, msg_seq, now, 0, msg_payload)
                writer.write(f.encode())

            frame_count += 1

            if frame_count % 50 == 0:
                await writer.drain()

            await asyncio.sleep(0.02)

        await writer.drain()
        elapsed = time.time() - start
        mb_sent = (audio_seq * len(audio_payload) +
                   video_seq * 15000 * 0.8 +  # ~80% P-frames
                   video_seq * 0.033 * 80000 +  # ~3.3% I-frames
                   msg_seq * len(msg_payload)) / 1024 / 1024
        print(f"[pub]  done: {audio_seq} audio, {video_seq} video, "
              f"{msg_seq} msg frames in {elapsed:.1f}s "
              f"({mb_sent/elapsed:.1f} MB/s)")


# ── Subscriber ──────────────────────────────────────────────────
class PushStream:
    """Tracks a server-pushed unidirectional stream."""

    def __init__(self, stream_id: int):
        self.stream_id = stream_id
        self.buffer = b""
        self.track = "unknown"
        self.frames = 0
        self.bytes = 0


class SubscriberProtocol(asyncio.Protocol):
    """Protocol that collects push stream data."""

    def __init__(self, on_frame: Callable):
        super().__init__()
        self.on_frame = on_frame
        self.push_streams = {}
        self.transport = None
        self.handshake_event = asyncio.Event()

    def connection_made(self, transport):
        self.transport = transport

    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            self.handshake_event.set()
        elif isinstance(event, StreamDataReceived):
            stream_id = event.stream_id
            if stream_id not in self.push_streams:
                self.push_streams[stream_id] = PushStream(stream_id)
            ps = self.push_streams[stream_id]
            ps.buffer += event.data
            self._process_push_stream(ps)

    def _process_push_stream(self, ps: PushStream):
        while len(ps.buffer) >= FRAME_HEADER_SIZE:
            pay_len = struct.unpack("!I", ps.buffer[19:23])[0]
            total = FRAME_HEADER_SIZE + pay_len
            if len(ps.buffer) < total:
                break
            frame = Frame.decode(ps.buffer[:total])
            ps.buffer = ps.buffer[total:]
            ps.frames += 1
            ps.bytes += total

            if frame.type == SIG_ACK and not ps.track:
                meta = parse_sig(frame)
                ps.track = meta.get("track", "unknown")
                print(f"[sub]  push stream #{ps.stream_id} track={ps.track}")

            if frame.type in (AUDIO, VIDEO, MESSAGE):
                self.on_frame(ps.track, frame)


async def run_subscriber(server: str, channel: str, insecure: bool,
                         show_stats: bool):
    host, port = parse_addr(server)
    config = QuicConfiguration(is_client=True)
    config.verify_mode = False if insecure else True

    stats = {"audio": 0, "video": 0, "message": 0, "audio_b": 0, "video_b": 0}
    start = time.time()

    def on_frame(track: str, frame: Frame):
        if frame.type == AUDIO:
            stats["audio"] += 1
            stats["audio_b"] += len(frame.payload)
        elif frame.type == VIDEO:
            stats["video"] += 1
            stats["video_b"] += len(frame.payload)
            if frame.flags & 1:
                latency = (now_us() - frame.timestamp) / 1000
                print(f"[sub]  KEYFRAME seq={frame.seq} latency={latency:.1f}ms")
        elif frame.type == MESSAGE:
            stats["message"] += 1
            msg = parse_sig(frame)
            print(f"[sub]  message: {msg.get('text', msg)}")

    protocol = SubscriberProtocol(on_frame)

    async with connect(host, port, configuration=config,
                       create_protocol=lambda: protocol) as conn:
        await protocol.handshake_event.wait()

        stream = conn.create_stream()
        reader, writer = stream
        frame_reader = FrameReader(reader)

        sig = Frame.make_sig(SIG_SUB, {"channel": channel, "tracks": ["all"]})
        writer.write(sig.encode())
        await writer.drain()

        ack = await frame_reader.read_frame()
        if ack.type == SIG_ERR:
            print(f"[sub]  server error: {parse_sig(ack)}")
            return
        print(f"[sub]  subscribed to '{channel}'")

        # Receive push streams
        last_stats = time.time()
        while True:
            await asyncio.sleep(0.1)
            now = time.time()

            if show_stats and now - last_stats >= 5:
                elapsed = now - start
                af = stats["audio"]
                vf = stats["video"]
                ab = stats["audio_b"]
                vb = stats["video_b"]
                mbps = (vb / elapsed / 1024 / 1024 * 8) if elapsed > 0 else 0
                print(f"\n─── Stats ───")
                print(f"  Audio  : {af} frames ({ab/1024:.0f} KB)")
                print(f"  Video  : {vf} frames ({vb/1024/1024:.1f} MB, {mbps:.1f} Mbps)")
                print(f"  Message: {stats['message']} msgs")
                print(f"  Elapsed: {elapsed:.0f}s")
                print(f"─────────────")
                last_stats = now


# ── Utils ───────────────────────────────────────────────────────
def parse_addr(addr: str):
    if ":" in addr:
        host, port_str = addr.rsplit(":", 1)
        return host, int(port_str)
    return addr, 4430


# ── CLI ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="QUIC AV Pub/Sub Python Client")
    sub = parser.add_subparsers(dest="mode", required=True)

    pub = sub.add_parser("publish", help="Publish media to a channel")
    pub.add_argument("--server", default="arm2.pvpv.bid:4430")
    pub.add_argument("--ch", default="room:101")
    pub.add_argument("--insecure", action="store_true", default=True)
    pub.add_argument("--bw", type=float, default=10.0, help="Target Mbps")

    sub_ = sub.add_parser("subscribe", help="Subscribe to a channel")
    sub_.add_argument("--server", default="arm2.pvpv.bid:4430")
    sub_.add_argument("--ch", default="room:101")
    sub_.add_argument("--insecure", action="store_true", default=True)
    sub_.add_argument("--stats", action="store_true", help="Show stats every 5s")

    args = parser.parse_args()

    if args.mode == "publish":
        asyncio.run(run_publisher(args.server, args.ch, args.insecure, args.bw))
    else:
        asyncio.run(run_subscriber(args.server, args.ch, args.insecure, args.stats))


if __name__ == "__main__":
    main()
