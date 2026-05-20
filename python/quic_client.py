"""QUIC AV Pub/Sub Python Client

Requires: pip install aioquic

Usage:
    python quic_client.py publish --server arm2.pvpv.bid:4430 --ch room:101
    python quic_client.py subscribe --server arm2.pvpv.bid:4430 --ch room:101 --stats
"""

import argparse
import asyncio
import json
import struct
import time
from dataclasses import dataclass

from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, HandshakeCompleted

# ── Protocol Constants ──────────────────────────────────────────
FRAME_HEADER_SIZE = 23

AUDIO     = 0x01
VIDEO     = 0x02
MESSAGE   = 0x03
SIG_PUB   = 0x10
SIG_SUB   = 0x11
SIG_ACK   = 0x13
SIG_ERR   = 0x14

FRAME_NAMES = {
    AUDIO: "AUDIO", VIDEO: "VIDEO", MESSAGE: "MESSAGE",
    SIG_PUB: "SIG_PUB", SIG_SUB: "SIG_SUB",
    SIG_ACK: "SIG_ACK", SIG_ERR: "SIG_ERR",
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
        return f"Frame({FRAME_NAMES.get(self.type, f'0x{self.type:02X}')}, ch={self.channel_id}, seq={self.seq})"

    def encode(self) -> bytes:
        return struct.pack("!BHIqB3sI",
            self.type, self.channel_id, self.seq,
            self.timestamp, self.flags, b"\x00\x00\x00",
            len(self.payload)) + self.payload

    @staticmethod
    def decode(data: bytes) -> "Frame":
        if len(data) < FRAME_HEADER_SIZE:
            raise ValueError(f"frame too short: {len(data)}B")
        typ, ch_id, seq, ts, flags, _, pay_len = struct.unpack(
            "!BHIqB3sI", data[:FRAME_HEADER_SIZE])
        return Frame(typ, ch_id, seq, ts, flags,
                     data[FRAME_HEADER_SIZE:FRAME_HEADER_SIZE + pay_len])

    @staticmethod
    def make_sig(typ: int, body: dict) -> "Frame":
        return Frame(typ, 0, 0, now_us(), 0, json.dumps(body).encode())


def now_us() -> int:
    return int(time.time() * 1_000_000)


def parse_sig(frame: Frame) -> dict:
    return json.loads(frame.payload)


# ── Frame Reader ────────────────────────────────────────────────
class FrameReader:
    def __init__(self, reader: asyncio.StreamReader):
        self._r = reader

    async def read_frame(self) -> Frame:
        hdr = await self._r.readexactly(FRAME_HEADER_SIZE)
        pay_len = struct.unpack("!I", hdr[19:23])[0]
        payload = b""
        if pay_len > 0:
            if pay_len > 64 * 1024 * 1024:
                raise ValueError(f"payload too large: {pay_len}")
            payload = await self._r.readexactly(pay_len)
        typ, ch_id, seq, ts, flags, _, _ = struct.unpack("!BHIqB3sI", hdr)
        return Frame(typ, ch_id, seq, ts, flags, payload)


# ── QUIC Protocol ───────────────────────────────────────────────
class PubSubProtocol(QuicConnectionProtocol):
    """Custom protocol that captures push streams via quic_event_received."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handshake_event = asyncio.Event()
        self.push_handler = None

    def quic_event_received(self, event):
        # Intercept push stream data (server-initiated = odd stream IDs)
        if isinstance(event, StreamDataReceived):
            sid = event.stream_id
            if sid % 2 == 1 and self.push_handler:
                self.push_handler.feed(sid, event.data)
                return  # don't call super() - we handle push streams
        super().quic_event_received(event)
        if isinstance(event, HandshakeCompleted):
            self.handshake_event.set()

    def set_push_callback(self, cb):
        self.push_handler = _PushStreamHandler(cb)


class _PushStreamHandler:
    """Parses frames from push stream data chunks."""

    def __init__(self, callback):
        self.callback = callback
        self.tracks: dict[int, str] = {}
        self.buffers: dict[int, bytes] = {}

    def feed(self, stream_id: int, data: bytes):
        buf = self.buffers.get(stream_id, b"") + data
        self.buffers[stream_id] = buf
        self._process(stream_id)

    def _process(self, stream_id: int):
        buf = self.buffers.get(stream_id, b"")
        track = self.tracks.get(stream_id)

        while len(buf) >= FRAME_HEADER_SIZE:
            pay_len = struct.unpack("!I", buf[19:23])[0]
            total = FRAME_HEADER_SIZE + pay_len
            if len(buf) < total:
                break
            frame = Frame.decode(buf[:total])
            buf = buf[total:]

            if frame.type == SIG_ACK and track is None:
                meta = parse_sig(frame)
                track = meta.get("track", "unknown")
                self.tracks[stream_id] = track
                print(f"  [push] stream #{stream_id} track={track}")
                continue

            if frame.type in (AUDIO, VIDEO, MESSAGE) and self.callback:
                self.callback(track or "unknown", frame)

        self.buffers[stream_id] = buf


# ── Publisher ───────────────────────────────────────────────────
async def run_publisher(server: str, channel: str, insecure: bool, bw: float = 10.0):
    host, port = parse_addr(server)
    config = QuicConfiguration(is_client=True, alpn_protocols=["quic-pubsub/1"])
    config.verify_mode = False if insecure else True

    async with connect(host, port, configuration=config,
                       create_protocol=PubSubProtocol) as protocol:
        await protocol.handshake_event.wait()
        print(f"[pub]  connected to {server}")

        reader, writer = await protocol.create_stream()
        fr = FrameReader(reader)

        sig = Frame.make_sig(SIG_PUB, {"channel": channel, "tracks": ["audio", "video", "message"]})
        writer.write(sig.encode())
        await writer.drain()

        ack = await fr.read_frame()
        if ack.type == SIG_ERR:
            print(f"[pub]  server error: {parse_sig(ack)}")
            return
        ch_id = ack.channel_id
        print(f"[pub]  channel '{channel}' ready (id={ch_id})")

        audio_payload = b"\x00" * 80
        video_payload = b"\x00" * 15000
        keyframe = b"\x00" * 80000
        msg_payload = json.dumps({"type": "chat", "sender": "py-pub",
                                   "text": "hello from python"}).encode()

        start = time.time()
        audio_seq = video_seq = msg_seq = 0

        while time.time() - start < 60:
            now = now_us()

            audio_seq += 1
            writer.write(Frame(AUDIO, ch_id, audio_seq, now, 0, audio_payload).encode())

            video_seq += 1
            is_key = video_seq % 30 == 0
            payload = keyframe if is_key else video_payload
            writer.write(Frame(VIDEO, ch_id, video_seq, now,
                               1 if is_key else 0, payload).encode())

            if video_seq % 30 == 0:
                msg_seq += 1
                writer.write(Frame(MESSAGE, ch_id, msg_seq, now, 0, msg_payload).encode())

            if video_seq % 10 == 0:
                await writer.drain()

            await asyncio.sleep(0.033)

        await writer.drain()
        elapsed = time.time() - start
        total_mb = (audio_seq * 80 + video_seq * 15000 * 0.97 +
                    (video_seq // 30) * 80000 + msg_seq * len(msg_payload)) / 1024 / 1024
        print(f"[pub]  done: {audio_seq} audio, {video_seq} video, "
              f"{msg_seq} msg in {elapsed:.1f}s ({total_mb/elapsed:.1f} MB/s)")


# ── Subscriber ──────────────────────────────────────────────────
async def run_subscriber(server: str, channel: str, insecure: bool, show_stats: bool):
    host, port = parse_addr(server)
    config = QuicConfiguration(is_client=True, alpn_protocols=["quic-pubsub/1"])
    config.verify_mode = False if insecure else True

    stats = {"audio": 0, "video": 0, "msg": 0, "audio_b": 0, "video_b": 0}
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
                print(f"  [sub]  KEYFRAME seq={frame.seq} latency={latency:.1f}ms")
        elif frame.type == MESSAGE:
            stats["msg"] += 1
            print(f"  [sub]  message: {parse_sig(frame).get('text', frame.payload)}")

    async with connect(host, port, configuration=config,
                       create_protocol=PubSubProtocol) as protocol:
        await protocol.handshake_event.wait()
        protocol.set_push_callback(on_frame)

        # Open signaling stream
        reader, writer = await protocol.create_stream()
        fr = FrameReader(reader)

        sig = Frame.make_sig(SIG_SUB, {"channel": channel, "tracks": ["all"]})
        writer.write(sig.encode())
        await writer.drain()

        ack = await fr.read_frame()
        if ack.type == SIG_ERR:
            print(f"[sub]  server error: {parse_sig(ack)}")
            return
        print(f"[sub]  subscribed to '{channel}', waiting for push streams...")

        last_stats = time.time()
        while True:
            await asyncio.sleep(0.5)
            now_t = time.time()
            if show_stats and now_t - last_stats >= 5:
                elapsed = now_t - start
                vb = stats["video_b"]
                mbps = (vb / elapsed / 1024 / 1024 * 8) if elapsed > 0 else 0
                print(f"\n  ─── Stats ({elapsed:.0f}s) ───")
                print(f"  Audio : {stats['audio']} frames ({stats['audio_b']/1024:.0f} KB)")
                print(f"  Video : {stats['video']} frames ({vb/1024/1024:.1f} MB, {mbps:.1f} Mbps)")
                print(f"  Msg   : {stats['msg']} messages")
                print(f"  ─────────────────")
                last_stats = now_t


# ── Utils ───────────────────────────────────────────────────────
def parse_addr(addr: str):
    parts = addr.rsplit(":", 1)
    return (parts[0], int(parts[1])) if len(parts) == 2 else (addr, 4430)


# ── CLI ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="QUIC AV Pub/Sub Python Client")
    sub = parser.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("publish", help="Publish media to a channel")
    p.add_argument("--server", default="arm2.pvpv.bid:4430")
    p.add_argument("--ch", default="room:101")
    p.add_argument("--insecure", action="store_true", default=True)
    p.add_argument("--bw", type=float, default=10.0, help="Target Mbps (unused)")

    s = sub.add_parser("subscribe", help="Subscribe to a channel")
    s.add_argument("--server", default="arm2.pvpv.bid:4430")
    s.add_argument("--ch", default="room:101")
    s.add_argument("--insecure", action="store_true", default=True)
    s.add_argument("--stats", action="store_true", help="Show stats every 5s")

    args = parser.parse_args()
    if args.mode == "publish":
        bw = getattr(args, 'bw', 10.0)
        asyncio.run(run_publisher(args.server, args.ch, args.insecure, bw))
    else:
        asyncio.run(run_subscriber(args.server, args.ch, args.insecure, args.stats))


if __name__ == "__main__":
    main()
