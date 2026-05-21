# Route 模块 - 架构设计文档

## 1. 概述

Route 模块是一个轻量级、可嵌入的多跳路由协议栈，专为资源受限的嵌入式系统设计。它通过异构物理链路连接的节点网络，提供可靠的、支持分片的消息传递能力。

```
┌─────────────────────────────────────────────────────┐
│                    应用层                             │
│         route_send_sync / route_send_async           │
│         route_broadcast / route_reply                │
├─────────────────────────────────────────────────────┤
│                   事务层                             │
│         请求/响应匹配，超时管理                        │
├─────────────────────────────────────────────────────┤
│                  可靠性层                             │
│         基于 ACK 的重传机制（可插拔）                  │
├─────────────────────────────────────────────────────┤
│                   分片层                             │
│         大载荷拆分/重组                               │
├─────────────────────────────────────────────────────┤
│                  路由层                              │
│         路由转发、去重                               │
├─────────────────────────────────────────────────────┤
│               端口抽象（HAL）                         │
│         UART / SPI / MQ / 任意物理链路               │
└─────────────────────────────────────────────────────┘
```

## 2. 设计原则

| 原则 | 实现方式 |
|------|---------|
| 零动态内存分配 | 静态实例池 + 固定大小内存池 |
| 不依赖堆 | 所有缓冲区编译期预分配 |
| OS 可选 | OS 抽象层为可选项；裸机环境无需 OS |
| 可插拔可靠性 | 策略模式实现 ACK/重传逻辑 |
| 传输无关 | 端口抽象将协议栈与物理层解耦 |
| 多实例支持 | 最多 `ROUTE_MAX_INSTANCES` 个独立路由栈 |

## 3. 模块结构

```
route/
├── route.h                    # 公共 API（面向应用层）
├── route_instance.c           # 实例生命周期、层间连接、公共 API 实现
├── common/
│   ├── route_types.h          # 所有类型定义、配置宏、实例结构体
│   ├── route_pool.{h,c}       # 固定大小块内存池
│   ├── route_queue.{h,c}      # 环形缓冲接收队列
│   └── route_crc.{h,c}        # CRC-16/CCITT
├── router/
│   └── route_router.{h,c}     # 帧构建/解析、路由转发、去重
├── frag/
│   └── route_frag.{h,c}       # 分片与重组
├── reliability/
│   └── route_reliability.{h,c}# 端到端 ACK 及重传
├── transaction/
│   └── route_transaction.{h,c}# 请求-响应关联、同步/异步 API
└── example/
    ├── example_mq_transport.c # PC 演示：3 节点链形拓扑 + 消息队列传输
    └── Makefile
```

## 4. 各层详细说明

### 4.1 路由层（`route_router.c`）

**职责：**
- 帧序列化/反序列化（8 字节头部 + 载荷 + 2 字节 CRC-16）
- 基于静态路由表的下一跳查找
- 多跳转发（TTL 逐跳递减）
- 广播泛洪（向除入口外的所有端口转发）
- 重复帧检测（环形 seen 表，仅追踪 REQUEST 帧）

**帧格式（线上格式）：**

```
偏移  大小  字段
  0    1   src          源节点 ID
  1    1   dst          目的节点 ID（0xFF = 广播）
  2    1   type         0=REQUEST, 1=RESPONSE, 2=ACK
  3    1   trans_id     事务 ID（trans_table 中的索引）
  4    1   seq          序列号（实例级计数器）
  5    1   ttl          生存时间（每跳递减）
  6    1   frag_idx     分片索引（从 0 开始）
  7    1   frag_total   分片总数
  8..N var  payload      0 到 ROUTE_FRAG_SIZE 字节
 N+1   2   crc16        CRC-16/CCITT，覆盖字节 [0..N]
```

**总帧长度：** `ROUTE_HEADER_SIZE(10) + payload_len`

**去重机制：** 环形缓冲区存储 `(src, seq, trans_id)` 三元组。仅追踪 REQUEST 帧（含广播）。防止路由环路和重复投递。

### 4.2 分片层（`route_frag.c`）

**职责：**
- 将超过 `ROUTE_FRAG_SIZE`（32 字节）的外发载荷拆分为多帧
- 将收到的分片重组为完整消息
- 超时清理过期的重组上下文

**设计要点：**
- 最多 `ROUTE_MAX_REASM_SLOTS`（4）个并发重组上下文
- 每个 slot 通过内存池分配的块追踪各分片
- 重组超时：`ROUTE_REASM_TIMEOUT_MS`（5000ms）
- 输出缓冲区：`inst->reasm_buf[ROUTE_MAX_PAYLOAD]`（避免栈上大数组）

**分片寻址：** 同一消息的所有分片共享相同的 `(src, seq)` 对。`frag_idx` 标识位置，`frag_total` 标识完整性。

**安全边界：**
- `frag_total` 上限为 `(ROUTE_MAX_PAYLOAD / ROUTE_FRAG_SIZE) + 1 = 9`
- 单片载荷上限为 `ROUTE_FRAG_SIZE`
- 重组总长度校验不超过 `ROUTE_MAX_PAYLOAD`

### 4.3 可靠性层（`route_reliability.c`）

**职责：**
- 端到端 ACK 追踪
- 超时自动重传
- 通过 `reliability_strategy_t` 可插拔（策略模式）

**内置策略：`e2e_strategy`**

```
on_send()     → 存储待确认条目（dest, seq, trans_id, 数据副本）
on_recv_ack() → 清除匹配的待确认条目
on_tick()     → 检查超时，最多重传 ROUTE_ACK_RETRY_MAX 次
```

**参数：**
- `ROUTE_ACK_TIMEOUT_MS`：每次重传间隔 1000ms
- `ROUTE_ACK_RETRY_MAX`：最大重试 3 次
- 待确认槽位数：`ROUTE_MAX_CONCURRENT_TRANSACTIONS`（8）

**限制：** 仅单分片载荷被追踪重传（多分片消息依赖事务层超时进行恢复）。

### 4.4 事务层（`route_transaction.c`）

**职责：**
- 通过 `trans_id`（`trans_table` 中的槽位索引）进行请求/响应关联
- 同步 API（阻塞在信号量上，支持超时）
- 异步 API（响应到达或超时时触发回调）
- 每事务独立的超时管理

**状态机：**

```
IDLE ──[发送]──> WAITING ──[收到响应]──> IDLE
                    │
                    └──[超时]──> callback(ERR_TIMEOUT) ──> IDLE
```

**超时时长：** `ROUTE_ACK_TIMEOUT_MS × (ROUTE_ACK_RETRY_MAX + 2) = 5000ms`

**并发保护：** 当 OS 层存在时，由 `trans_mutex` 保护。槽位分配和响应投递均在互斥锁内完成。

### 4.5 公共工具

| 模块 | 用途 |
|------|------|
| `route_pool` | 固定大小块分配器（LIFO 空闲链表）。供分片层存储分片数据使用。 |
| `route_queue` | 环形缓冲区，用于接收原始帧。ISR/DMA 安全写入，轮询读取。 |
| `route_crc` | CRC-16/CCITT（多项式 0x1021，初始值 0xFFFF）。帧完整性校验。 |

### 4.6 端口抽象

```c
typedef struct {
    uint8_t port_id;
    int (*send)(uint8_t port_id, const uint8_t *buf, uint16_t len);
    void *ctx;
} route_port_t;
```

每个实例最多支持 `ROUTE_MAX_PORTS`（4）个物理端口。路由表将 `(dest_id)` 映射到 `(next_hop, port_id)` 用于转发决策。

**接收路径：** 外部代码（ISR、DMA、轮询线程）通过 `route_queue_push()` 将原始帧推入 `inst->recv_queue`。主循环调用 `route_poll()` 进行处理。

### 4.7 OS 抽象层

```c
typedef struct {
    void *(*mutex_create)(void);
    void  (*mutex_lock)(void *mutex);
    void  (*mutex_unlock)(void *mutex);
    void  (*mutex_destroy)(void *mutex);
    void *(*sem_create)(void);
    int   (*sem_wait)(void *sem, uint32_t timeout_ms);
    void  (*sem_post)(void *sem);
    void  (*sem_destroy)(void *sem);
} route_os_t;
```

**可选项。** 当 `config->os == NULL` 时：
- 同步发送（`route_send_sync`）返回 `ROUTE_ERR_PARAM`
- 异步发送和裸机轮询仍可正常工作
- 无互斥保护（假定单线程环境）

## 5. 数据流

### 5.1 发送路径（A 经 B 发送到 C）

```
应用层: route_send_async(inst_a, dest=3, data, len, callback)
    │
    ▼
事务层: 分配 slot[idx]，state=WAITING
    │
    ▼
分片层: 拆分为 N 个分片（每片 ≤ 32 字节）
    │ 对每个分片:
    ▼
路由层: build_frame(hdr + payload + CRC)，查路由表 → 端口发送
    │
    ▼
端口: send_a_to_b() → queue_ab
    │
    ════════════ 节点 B ════════════
    │
    ▼
queue_ab → route_queue_push(inst_b)
route_poll(inst_b) → route_router_handle_frame()
    │ hdr.dst != 本机 ID，TTL--，转发
    ▼
路由层: lookup(dest=3) → port 1，发送
    │
    ▼
端口: send_b_to_c() → queue_bc
    │
    ════════════ 节点 C ════════════
    │
    ▼
queue_bc → route_queue_push(inst_c)
route_poll(inst_c) → handle_frame() → dst == 本机 ID → 投递
    │
    ▼
路由层投递 → 分片重组（收集所有分片）
    │ 全部收齐:
    ▼
分片完成 → 发送 ACK + 调用 on_recv_cb(inst_c, src=1, data)
    │
    ▼
应用层: on_recv_c() → route_reply(inst_c, dest=1, reply_data)
```

### 5.2 接收路径（C 的响应返回 A）

```
route_reply() → frag_lower_send（type=RESPONSE）
    │
    ▼ （经 B 反向传递）
    │
节点 A: frag_complete_cb → type==RESPONSE → transaction_on_response()
    │
    ▼
事务层: 匹配 trans_id，调用 callback(ROUTE_OK, data)
    │ （同步模式则 sem_post）
    ▼
应用层: on_response_a() 被调用
```

### 5.3 定时驱动处理

```
route_tick(inst, now_ms)
    ├── route_frag_tick()        → 超时清理过期的重组 slot
    ├── reliability.on_tick()    → 重传未收到 ACK 的帧
    └── route_transaction_tick() → 超时过期的事务
```

## 6. 内存布局

### 6.1 每实例静态分配

| 组件 | 大小（字节） | 计算公式 |
|------|:---:|---------|
| pool_storage | 672 | `POOL_BLOCK_COUNT(16) × BLOCK_SIZE(42)` |
| reasm_buf | 256 | `ROUTE_MAX_PAYLOAD` |
| recv_queue.data | 672 | `RECV_QUEUE_SIZE(16) × BLOCK_SIZE(42)` |
| reasm_slots | ~160 | `4 × sizeof(route_reasm_ctx_t)` |
| pending_acks | ~384 | `8 × sizeof(route_pending_ack_t)` |
| trans_table | ~256 | `8 × sizeof(transaction_t)` |
| route_table | 96 | `32 × 3` |
| seen_table | 64 | `16 × 4` |
| ports | 48 | `4 × sizeof(route_port_t)` |
| **每实例合计** | **~2.6 KB** | |

### 6.2 全局静态分配

| 组件 | 大小 |
|------|------|
| `instances[4]` | `4 × ~2.6 KB ≈ 10.4 KB` |
| `instance_used[4]` | 4 字节 |

## 7. 配置参数

所有参数均为编译期 `#define`，有默认值（可通过 `route_config.h` 或编译器参数覆盖）：

| 宏 | 默认值 | 说明 |
|----|--------|------|
| `ROUTE_MAX_INSTANCES` | 4 | 最大并发路由实例数 |
| `ROUTE_MAX_NODES` | 32 | 路由表最大条目数 |
| `ROUTE_MAX_PORTS` | 4 | 每实例最大物理端口数 |
| `ROUTE_MAX_CONCURRENT_TRANSACTIONS` | 8 | 最大并发事务数 |
| `ROUTE_MAX_PAYLOAD` | 256 | 重组后消息最大长度 |
| `ROUTE_FRAG_SIZE` | 32 | 每分片最大载荷 |
| `ROUTE_MAX_REASM_SLOTS` | 4 | 最大并发重组上下文数 |
| `ROUTE_POOL_BLOCK_COUNT` | 16 | 内存池块数量 |
| `ROUTE_ACK_TIMEOUT_MS` | 1000 | 重传间隔（毫秒） |
| `ROUTE_ACK_RETRY_MAX` | 3 | 最大重传次数 |
| `ROUTE_DEFAULT_TTL` | 8 | 外发帧初始 TTL |
| `ROUTE_SEEN_TABLE_SIZE` | 16 | 去重表条目数 |
| `ROUTE_REASM_TIMEOUT_MS` | 5000 | 重组超时（毫秒） |
| `ROUTE_RECV_QUEUE_SIZE` | 16 | 接收队列深度 |

## 8. 线程模型

```
┌──────────────────────────────────────────────┐
│               应用线程                        │
│  route_send_sync() ──阻塞在信号量上──┐       │
│  route_send_async() ──立即返回        │       │
│                                       │       │
├───────────────────────────────────────┤       │
│            轮询线程 / 主循环           │       │
│  while(1) {                           │       │
│    收取物理层数据 → recv_queue;        │       │
│    route_poll(inst);  // 处理接收队列  │       │
│    route_tick(inst, now_ms()); // 定时 │       │
│    sleep(interval);                   │       │
│  }                                    │       │
├───────────────────────────────────────┤       │
│            ISR / DMA 上下文            │       │
│  route_queue_push(&inst->recv_queue)  │       │
│  （单生产者时无锁安全）                │       │
└──────────────────────────────────────────────┘
```

**同步点：**
- `trans_mutex`：保护 `trans_table` 的分配和状态转换
- `sync_sem`：每事务信号量，用于同步调用阻塞
- `recv_queue`：ISR 写入，轮询循环读取（环形缓冲区，单生产者/单消费者时天然原子）

## 9. API 概览

### 生命周期
```c
route_instance_t *route_create(uint8_t node_id, const route_config_t *config);
void              route_destroy(route_instance_t *inst);
```

### 配置
```c
int  route_port_register(inst, const route_port_t *port);
int  route_table_set(inst, const route_entry_t *entries, uint8_t count);
void route_reliability_set(inst, const reliability_strategy_t *strategy);
void route_on_recv(inst, callback);
```

### 发送
```c
int route_send_sync(inst, dest, data, len, resp_buf, resp_len, timeout_ms);
int route_send_async(inst, dest, data, len, callback, user_data);
int route_broadcast(inst, data, len, callback, user_data);
int route_reply(inst, dest, trans_id, data, len);
```

### 驱动（必须周期性调用）
```c
void route_poll(inst);              // 处理接收到的帧
void route_tick(inst, now_ms);      // 驱动定时器（分片、可靠性、事务）
```

## 10. 错误码

| 错误码 | 值 | 含义 |
|--------|:--:|------|
| `ROUTE_OK` | 0 | 成功 |
| `ROUTE_ERR_TIMEOUT` | -1 | 操作超时 |
| `ROUTE_ERR_NO_ROUTE` | -2 | 无对应目的地的路由表条目 |
| `ROUTE_ERR_NO_MEM` | -3 | 内存池耗尽 |
| `ROUTE_ERR_FULL` | -4 | 队列/表/槽位已满 |
| `ROUTE_ERR_REASM` | -5 | 重组错误（溢出） |
| `ROUTE_ERR_PARAM` | -6 | 参数无效 |
| `ROUTE_ERR_NO_PORT` | -7 | 路由条目对应的端口不存在 |

## 11. 限制与约束

1. **仅支持静态路由** — 无动态路由发现协议
2. **广播不支持分片** — 限制为 `ROUTE_FRAG_SIZE`（32 字节）
3. **可靠性仅覆盖单分片** — 多分片消息依赖事务层超时恢复
4. **去重表较小** — 16 条目环形覆盖；高吞吐场景可能出现漏检
5. **序列号为 8 位** — 每 256 条消息回绕；结合小去重表，高负载下可能发生去重碰撞
6. **无加密或认证** — CRC-16 仅防传输错误，不防恶意篡改
7. **轮询函数单线程假设** — `route_poll()` 和 `route_tick()` 不可从多个线程并发调用

## 12. 典型集成示例（裸机）

```c
// 初始化
route_instance_t *inst = route_create(MY_NODE_ID, NULL);  // 无 OS
route_port_t uart_port = {.port_id = 0, .send = uart_send};
route_port_register(inst, &uart_port);
route_table_set(inst, my_routes, route_count);
route_on_recv(inst, app_on_request);

// UART 中断服务程序
void UART_IRQHandler(void) {
    uint8_t frame[ROUTE_BLOCK_SIZE];
    uint16_t len = uart_read(frame, sizeof(frame));
    route_queue_push(&inst->recv_queue, frame, len, 0);
}

// 主循环
while (1) {
    route_poll(inst);
    route_tick(inst, HAL_GetTick());
    // ... 其他任务 ...
}
```
