# 开发者使用指南

## 公共测试服务器

无需自己部署服务端，直接用以下地址测试：

| 地址 | 端口 |
|------|------|
| `arm2.pvpv.bid` | `4430` |

```bash
# 订阅者测试（加入频道）
./bin/subscriber -server arm2.pvpv.bid:4430 -ch room:101

# 发布者测试（发送音视频到频道）
./bin/publisher -server arm2.pvpv.bid:4430 -ch room:101 -bw 15
```

> 测试服务器为 arm64 架构，位于公网。使用自签证书，客户端需设置 `Insecure: true`。

---

## 本地快速开始

```bash
# 生成证书（首次）
bash certs/gen.sh

# 安装依赖
go mod tidy

# 编译
go build -o bin/server    ./cmd/server
go build -o bin/publisher ./cmd/publisher
go build -o bin/subscriber ./cmd/subscriber

# 1. 启动服务端
./bin/server -addr :4430 -v

# 2. 启动订阅者（另一个终端）
./bin/subscriber -server 127.0.0.1:4430 -ch room:101

# 3. 启动发布者（另一个终端）
./bin/publisher -server 127.0.0.1:4430 -ch room:101 -bw 15
```

---

## 在你的应用中使用

### 1. 引入依赖

```go
import (
    "github.com/littletao08/quic-av-pubsub/pkg/proto"
    "github.com/littletao08/quic-av-pubsub/pkg/transport"
)
```

### 2. 作为发布者

```go
// 创建发布者（本地测试用 127.0.0.1，远程测试用 arm2.pvpv.bid）
pub := transport.NewPublisher(transport.ClientConfig{
    ServerAddr:    "127.0.0.1:4430",    // 或 "arm2.pvpv.bid:4430"
    Insecure:      true,           // 开发阶段跳过证书验证
    TargetMbps:    10.0,           // Brutal 拥塞控制目标带宽
    HeartbeatSec:  5,              // 心跳间隔（秒）
}, logger)

// 连接服务端
if err := pub.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer pub.Close()

// 注册频道（服务端自动创建）
chID, err := pub.Publish(ctx, "room:101", []proto.TrackType{
    proto.TrackAudio,
    proto.TrackVideo,
    proto.TrackMessage,
})
if err != nil {
    log.Fatal(err)
}
// chID 是服务端分配的 uint16 ID

// 发送音频帧（Opus 数据）
pub.SendAudio("room:101", opusData)

// 发送视频帧（H.264/AV1 数据）
pub.SendVideo("room:101", h264Data, isKeyFrame) // isKeyFrame=true 表示 I 帧

// 发送普通消息（JSON 或二进制）
pub.SendMessage("room:101", jsonData)
```

### 3. 作为订阅者

```go
// 定义收帧回调
onFrame := func(track proto.TrackType, f *proto.Frame) {
    switch track {
    case proto.TrackAudio:
        // f.Payload 是 Opus 编码数据
        audioDecoder.Push(f.Payload)

    case proto.TrackVideo:
        // f.IsKeyFrame() 判断是否为 I 帧
        videoDecoder.Push(f.Payload)

    case proto.TrackMessage:
        var msg map[string]any
        json.Unmarshal(f.Payload, &msg)
    }
}

// 创建订阅者（本地或远程测试）
sub := transport.NewSubscriber(transport.ClientConfig{
    ServerAddr: "127.0.0.1:4430",    // 或 "arm2.pvpv.bid:4430"
    Insecure:   true,
}, onFrame, logger)

// 连接
if err := sub.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer sub.Close()

// 订阅频道
if err := sub.Subscribe(ctx, "room:101", []proto.TrackType{proto.TrackAll}); err != nil {
    log.Fatal(err)
}

// 阻塞运行
select {}
```

### 4. 只订阅特定 track

```go
// 只订阅音频
sub.Subscribe(ctx, "room:101", []proto.TrackType{proto.TrackAudio})

// 只订阅音频+消息
sub.Subscribe(ctx, "room:101", []proto.TrackType{proto.TrackAudio, proto.TrackMessage})
```

---

## 帧结构

### 帧头（23 字节）

```
[0]    Type       (1 byte)  帧类型
[1-2]  ChannelID  (2 bytes) 服务端分配的频道 ID
[3-6]  Seq        (4 bytes) 自动递增序列号
[7-14] Timestamp  (8 bytes) Unix 微秒时间戳
[15]   Flags      (1 byte)  bit0=KeyFrame(视频 I 帧)
[16-18] Reserved  (3 bytes) 填充 0
[19-22] PayloadLen (4 bytes) 载荷长度（大端序）
[23+]  Payload    (variable)
```

### 帧类型

| 值 | 名称 | 用途 |
|----|------|------|
| 0x01 | AUDIO | 音频数据 (Opus) |
| 0x02 | VIDEO | 视频数据 (H.264/AV1) |
| 0x03 | MESSAGE | 消息 (JSON/二进制) |
| 0x10 | SIG_PUB | 发布请求 |
| 0x11 | SIG_SUB | 订阅请求 |
| 0x13 | SIG_ACK | 服务端应答 |
| 0x14 | SIG_ERR | 服务端错误 |
| 0xFF | HEARTBEAT | 心跳 |

---

## CLI 参数说明

### 服务端 (`./bin/server`)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `:4430` | 监听 UDP 端口 |
| `-cert` | `./certs/server.crt` | TLS 证书路径 |
| `-key` | `./certs/server.key` | TLS 私钥路径 |
| `-v` | `false` | 详细日志 (debug) |

### 发布者 (`./bin/publisher`)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-server` | `127.0.0.1:4430` | 服务端地址 |
| `-ch` | `room:101` | 频道名称 |
| `-bw` | `10.0` | Brutal 目标带宽 (Mbps) |

### 订阅者 (`./bin/subscriber`)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-server` | `127.0.0.1:4430` | 服务端地址 |
| `-ch` | `room:101` | 频道名称 |

---

## 关键设计要点

### 自动创建频道

发布者第一次调用 `Publish()` 时，服务端自动创建频道。后续发布者发往同一频道时复用已有频道：

```go
// 第一次调用：服务端创建频道
chID, _ := pub.Publish(ctx, "room:101", tracks)

// 后续调用：立即返回已有 channelID（无网络交互）
chID, _ = pub.Publish(ctx, "room:101", tracks)
```

### 心跳机制

心跳在发布者的所有数据流上发送，服务端收到心跳后回复。订阅者不使用心跳（由 QUIC 的 KeepAlive 替代）：

```go
// 心跳间隔在 ClientConfig 中设置
cfg := transport.ClientConfig{
    HeartbeatSec: 5,  // 每 5 秒发送一次心跳
}
```

### Brutal 拥塞控制

令牌桶工作在应用层，发送前调用 `Wait()` 阻塞直到获得许可：

```go
// 设置目标带宽 10 Mbps（建议设为实际码率的 1.2~1.5 倍）
pub := transport.NewPublisher(transport.ClientConfig{
    TargetMbps: 10.0,
}, logger)

// 关闭限速（不限速）
pub := transport.NewPublisher(transport.ClientConfig{
    TargetMbps: 0,
}, logger)
```

### Jitter Buffer（接收端重排）

每个 push 流独立拥有 jitter buffer（插入排序），订阅者收到帧时自动重排。buffer 满或等待过半时自动跳号（视频直播可接受的丢帧）：

- 默认 buffer 大小：256 帧
- 乱序帧按 Seq 插入排序
- Buffer 半满时强制跳号输出

---

## 通信流程

### 发布流程

```
Publisher                   Server
  |--- QUIC Dial :4430 --->|
  |--- SIG_PUB ----------->|  自动创建频道
  |<-- SIG_ACK (ch_id) ----|
  |--- AUDIO/VIDEO/MSG --->|  投递 Broker
  |<-- HEARTBEAT ----------|  心跳
```

### 订阅流程

```
Subscriber                   Server
  |--- QUIC Dial :4430 ---->|
  |--- SIG_SUB ------------>|  查找频道
  |<-- SIG_ACK (ch_id) -----|  注册订阅者
  |<-- OpenUniStream (audio)|  服务端推流
  |<-- OpenUniStream (video)|
  |<-- AUDIO/VIDEO frames --|  持续推送
```

---

## 生产环境建议

### TLS 证书

```go
// 开发阶段跳过证书验证
cfg := transport.ClientConfig{Insecure: true}

// 生产环境使用 Let's Encrypt 等正式证书
cfg := transport.ClientConfig{Insecure: false} // 同时配置 TLS root CAs
```

### Brutal 带宽设置

| 实际流码率 | `-bw` 推荐值 | 说明 |
|-----------|-------------|------|
| 2 Mbps 视频 + 128 kbps 音频 | 4.0 | 1.5x 抢占带宽 |
| 8 Mbps 视频 | 12.0 | 跨国弱网 |
| 20 Mbps 超高清 | 30.0 | 内网/高带宽 |

### 内核调优

```bash
# /etc/sysctl.d/99-quic-pubsub.conf
net.core.rmem_max = 67108864       # 64 MB
net.core.wmem_max = 67108864
net.core.netdev_max_backlog = 5000
```

---

## API 参考

### `transport.ClientConfig`

```go
type ClientConfig struct {
    ServerAddr    string   // 服务端地址 "host:port"
    Insecure      bool     // 跳过 TLS 验证（开发用）
    TargetMbps    float64  // Brutal 目标带宽，0=不限速
    HeartbeatSec  int      // 心跳间隔秒，0=不发心跳
}
```

### `transport.Publisher`

| 方法 | 说明 |
|------|------|
| `NewPublisher(cfg, logger)` | 创建发布者 |
| `Connect(ctx)` | 建立 QUIC 连接 |
| `Publish(ctx, channel, tracks)` | 注册频道，返回 channelID |
| `SendAudio(channel, payload)` | 发送音频帧 |
| `SendVideo(channel, payload, isKeyFrame)` | 发送视频帧 |
| `SendMessage(channel, payload)` | 发送消息帧 |
| `Close()` | 关闭连接 |

### `transport.Subscriber`

| 方法 | 说明 |
|------|------|
| `NewSubscriber(cfg, callback, logger)` | 创建订阅者 |
| `Connect(ctx)` | 建立 QUIC 连接 |
| `Subscribe(ctx, channel, tracks)` | 订阅频道 |
| `Close()` | 关闭连接 |

### `proto.Frame`

```go
type Frame struct {
    Type      FrameType  // 帧类型
    ChannelID uint16     // 频道 ID
    Seq       uint32     // 序列号
    Timestamp int64      // Unix 微秒
    Flags     uint8      // 标志位
    Payload   []byte     // 载荷数据
}

func (f *Frame) IsKeyFrame() bool  // 判断视频 I 帧
```

### `proto.TrackType`

```go
TrackAudio   // "audio"  音频
TrackVideo   // "video"  视频
TrackMessage // "message" 消息
TrackAll     // "all"    所有类型
```
