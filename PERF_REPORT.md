# 性能测试报告

> 测试日期：2026-05-20  
> 测试平台：Linux arm64, Go 1.24.4  
> 核心依赖：quic-go v0.44  
> CPU：2 核 (arm64)

---

## 1. 帧编解码 (`pkg/proto`)

### 1.1 写帧吞吐

| 载荷 | 耗时/op | 吞吐量 | 分配 |
|------|---------|--------|------|
| 音频 80B | 75 ns | 53 GB/s | 24 B, 1 alloc |
| SD 15KB | 594 ns | 28 GB/s | 24 B, 1 alloc |
| HD 100KB | 7.2 µs | 15 GB/s | 24 B, 1 alloc |
| FHD 200KB | 6.8 µs | 36 GB/s | 25 B, 1 alloc |
| 4K 500KB | 20.7 µs | 26 GB/s | 33 B, 1 alloc |

### 1.2 读帧吞吐

| 载荷 | 耗时/op | 分配 |
|------|---------|------|
| 音频 80B | 332 ns | 200 B, 4 allocs |
| SD 15KB | 6 µs | 16504 B, 4 allocs |
| HD 100KB | 69 µs | 106616 B, 4 allocs |
| FHD 200KB | 53 µs | 204920 B, 4 allocs |
| 4K 500KB | 140 µs | 516216 B, 4 allocs |

### 1.3 并行与混合

| 场景 | 耗时/op | 分配 |
|------|---------|------|
| 并行写帧 (15KB) | 222 ns | 24 B, 1 alloc |
| 混合音视频 (6帧轮换) | 225 ns | 24 B, 1 alloc |
| 编解码往返 (写+读 15KB) | 6.7 µs | 16528 B, 5 allocs |
| 连续Seq写帧 (15KB) | 4.9 µs | 16408 B, 2 allocs |
| FrameType.String() 切换 | 579 ns | 456 B, 3 allocs |

### 1.4 持续吞吐量（2秒窗口）

| 载荷 | 帧数 | 吞吐量 |
|------|------|--------|
| 80B | 10,051,564 | 3,949 Mbps |
| 15KB | 3,865,848 | 221,545 Mbps |
| 200KB | 368,664 | 288,051 Mbps |
| 500KB | 106,912 | 208,821 Mbps |

> 注：200KB 帧达最高吞吐 288 Gbps，受限于 memcpy 带宽而非编解码逻辑。

---

## 2. Broker 发布系统 (`pkg/broker`)

### 2.1 扇出基准

| 订阅者数 | 耗时/op | 帧/秒 | 零分配 |
|---------|---------|-------|--------|
| 1 | 247 ns | 4.0M | ✓ |
| 5 | 319 ns | 3.1M | ✓ |
| 10 | 895 ns | 1.1M | ✓ |
| 50 | 1.3 µs | 785K | ✓ |
| 100 | 2.9 µs | 345K | ✓ |
| 500 | 18.9 µs | 53K | ✓ |

> 扇出延迟 O(n) 线性增长，500 订阅者时仍仅 19 µs。

### 2.2 并发场景

| 场景 | 耗时/op | 分配 |
|------|---------|------|
| 并发发布 (10 goroutine) | 144 ns | 0 alloc |
| 100频道并行访问 | 3.2 µs | 16437 B, 3 allocs |
| 多track发布 (音/视/消息) | 162 ns | 0 alloc |
| Buffer满背压 | 164 ns | 0 alloc |

### 2.3 频道管理

| 操作 | 耗时/op | 分配 |
|------|---------|------|
| 注册频道 | 3.4 µs | 641 B, 10 allocs |
| 清理连接 (100 subs) | 524 ns | 32 B, 3 allocs |
| 注销+重注册频道 | 2.6 µs | 571 B, 12 allocs |

### 2.4 可扩展性测试

| 场景 | 耗时 | 帧率 | 总扇出 |
|------|------|------|--------|
| 10,000帧 → 1订阅者 | 2 ms | 4.9M fps | 4.9M fps |
| 10,000帧 → 10订阅者 | 6 ms | 1.7M fps | 16.7M fps |
| 10,000帧 → 100订阅者 | 25 ms | 398K fps | 39.8M fps |
| 10,000帧 → 1000订阅者 | 2.2 s | 4.5K fps | 4.5M fps |
| 并发压测: 10Pub+20Sub, 5万帧 | 662 ms | — | — |

---

## 3. Jitter Buffer (`pkg/transport`)

| 场景 | 耗时/op | 分配 |
|------|---------|------|
| Push (插入排序) | 39 ns | 16 B, 0 alloc |
| Push + Pop (有序) | 14 µs | 16440 B, 3 allocs |
| Push + Pop (乱序) | 482 ns | 14 B, 0 alloc |
| Buffer满强制丢弃 | 11 µs | 16448 B, 2 allocs |
| 高延迟 (512 buffer) | 15.7 µs | 16452 B, 2 allocs |

### 辅助函数

| 操作 | 耗时/op | 分配 |
|------|---------|------|
| expandTracks (TrackAll) | 208 ns | 0 alloc |
| expandTracks (各种组合) | 178 ns | 0 alloc |
| mustMarshalJSON | 1.4 µs | 192 B, 6 allocs |

---

## 4. Brutal 拥塞控制 (`pkg/brutal`)

| 操作 | 耗时 | 分配 |
|------|------|------|
| Wait 15KB (100Mbps bot) | 1.13 µs | 0 alloc |
| Wait 1MB (10Mbps bot) | 700 ms | — |

> Pacer 在 <1µs 内完成令牌桶检查，0 分配。大包等待时基于 deficit 计算睡眠时间。

---

## 5. 系统极限估算

基于压力测试结果，预估单实例生产极限：

| 指标 | 估算值 | 瓶颈 |
|------|--------|------|
| **帧吞吐** | >10M 帧/秒 | memcpy (大帧) |
| **QUIC 连接数** | ~1,000 | MaxIncomingStreams |
| **单频道扇出** | 500 subs @ <20µs | 遍历订阅者 |
| **频道数** | >100K | map 内存 |
| **端到端延迟** | <1ms (内网) | QUIC 传输层 |
| **写入带宽** | >200 Gbps (单线程) | CPU 频率 |

### 已知瓶颈

1. **大帧读分配**: `ReadFrame` 每次都分配 payload (4K帧 516KB)，可用 pool 优化
2. **扇出线性遍历**: 500+订阅者时遍历 map 耗时明显，可改广播/fanout 优化
3. **Jitter Buffer 无锁**: 非线程安全，按设计每流单 goroutine 使用
4. **Brutal 应用层限速**: 睡眠精度受 OS 调度影响，生产应换 hysteria2 内核实现

---

## 6. 测试命令

```bash
# 全部测试
go test ./pkg/... -v -count=1 -timeout 300s

# 全部基准
go test ./pkg/proto/... -bench=. -benchmem -count=3 -timeout 180s
go test ./pkg/broker/... -bench=. -benchmem -count=3 -timeout 300s
go test ./pkg/transport/... -bench=. -benchmem -count=3 -timeout 120s
go test ./pkg/brutal/... -bench=. -benchmem -count=3 -timeout 60s

# 吞吐测试
go test ./pkg/proto/... -run TestMaxWriteThroughput -v -timeout 30s
go test ./pkg/proto/... -run TestLargeVideoFrameWriteThroughput -v -timeout 30s
go test ./pkg/broker/... -run TestBrokerScalability -v -timeout 30s
go test ./pkg/broker/... -run TestConcurrentPubSubStress -v -timeout 30s
```
