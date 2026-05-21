# API 线程安全与设计契约

## 总览

| 模块 | 线程安全 | 条件 |
|------|:--------:|------|
| Router | 部分 | `input` 可从任意线程/ISR 调用（需配置 `queue_lock`）；`send`/`poll` 必须在同一线程 |
| Frag | 可选 | 配置 `lock/unlock` 后，`send`/`input`/`tick` 可从不同线程调用 |
| Transaction | 是 | 内部 mutex 保护，`send_async`/`send_sync`/`on_response`/`tick` 可并发 |
| Stack | 继承各层 | `input` 线程安全（继承 Router queue_lock）；其余同 Router/Frag 约束 |

---

## Router 层

### 线程安全契约

```
┌─────────────────────────────────────────────────────────┐
│  ISR / 接收线程          │  主循环线程                    │
│                          │                               │
│  route_router_input() ──►│◄── route_router_poll()        │
│       (queue_lock 保护)  │       (queue_lock 保护)        │
│                          │                               │
│         禁止             │◄── route_router_send()        │
│                          │       (共享 tx_frame_buf)      │
└─────────────────────────────────────────────────────────┘
```

**规则：**

1. `route_router_input()` 可从任意上下文调用（ISR、DMA 完成回调、接收线程）。需配置 `queue_lock/queue_unlock`（如关中断或 spinlock）。
2. `route_router_send()` 和 `route_router_poll()` **必须在同一线程**调用——它们共享 `tx_frame_buf`，无内部锁保护。
3. `route_router_tick()` 仅写 `current_ms`（单字原子写），可从任意线程调用。
4. `route_router_port_register()` 和 `route_router_table_set()` 仅在初始化阶段调用，不可与 `send`/`poll` 并发。

### queue_lock 选择指南

| 场景 | 推荐 lock 实现 |
|------|---------------|
| 裸机，单 ISR 写入 | 关中断 `__disable_irq()` / `__enable_irq()` |
| 裸机，多 ISR 源 | 关中断 |
| RTOS，接收任务写入 | mutex 或 critical section |
| 单线程（poll 模式） | NULL（不配置，零开销） |

### tx_frame_buf / rx_frame_buf 生命周期

- `tx_frame_buf`：在 `send()` 和 `poll()`（转发）中使用，调用返回后内容无效
- `rx_frame_buf`：在 `poll()` 中使用，`deliver_cb` 回调期间 payload 指针指向其内部
- **`deliver_cb` 中不得调用 `route_router_poll()`**（会覆盖 `rx_frame_buf`）

---

## Frag 层

### 线程安全契约

**默认无锁（单线程使用）。** 配置 `lock/unlock` 后支持多线程。

```
┌──────────────────────────────────────────────────┐
│  发送线程              │  接收/tick 线程           │
│                        │                          │
│  route_frag_send() ───►│◄── route_frag_input()   │
│     (frag_lock 保护    │       (frag_lock 保护    │
│      pending_acks)     │        pool/reasm)       │
│                        │                          │
│                        │◄── route_frag_tick()     │
│                        │       (frag_lock 保护)   │
└──────────────────────────────────────────────────┘
```

**规则：**

1. 不配置 lock 时，`send`/`input`/`tick` 必须在同一线程调用。
2. 配置 lock 后，三者可从不同线程调用。
3. `complete_cb` 在锁外调用——回调中可安全调用 `route_frag_send()`（不会死锁）。
4. `lower_send` 在锁外调用——不会在持锁时调用下层。
5. `route_frag_deinit()` 不可与其他 API 并发。

### lock 选择指南

| 场景 | 推荐 |
|------|------|
| 单线程主循环 | NULL（不配置） |
| 发送线程 ≠ 接收线程 | mutex |
| ISR 中调用 input | 关中断（注意：lock 区间内不可有阻塞操作） |

---

## Transaction 层

### 线程安全契约

**内部 mutex 保护，完全线程安全**（需配置 `route_os_t`）。

```
┌────────────────────────────────────────────────────────────┐
│  用户线程 A     │  用户线程 B     │  tick 线程 / 主循环    │
│                 │                 │                         │
│  send_sync() ──►│◄── send_async()│◄── tick()              │
│  send_async()   │    reply()     │    on_response()        │
│                 │                 │                         │
│  (全部 mutex    │  (全部 mutex   │  (全部 mutex            │
│   保护)         │   保护)        │   保护)                 │
└────────────────────────────────────────────────────────────┘
```

**规则：**

1. 所有公共 API 均通过内部 mutex 序列化。
2. `callback` 和 `sem_post` 在锁外执行——回调中可安全调用 `send_async`/`reply`。
3. `send_sync()` 要求 `route_os_t` 非 NULL（需要信号量）。
4. `send_async()` 在无 OS 层时也可使用（仅需 mutex 或单线程）。
5. `route_transaction_deinit()` 会唤醒所有阻塞的 `send_sync` 调用者（result = TIMEOUT）。

### 不配置 OS 层时

- `send_sync()` 返回 `ROUTE_ERR_PARAM`
- `send_async()` 可用（无 mutex 时退化为单线程模式）
- `tick()` 中超时的同步 transaction 无法唤醒（不应存在）

---

## route_pool

### 调用者责任

1. **传入合法指针**：`route_pool_free(block)` 中 `block` 必须是之前 `route_pool_alloc()` 返回的地址。传入其他地址（含 NULL 以外的非法值）导致未定义行为。
2. **不可重复释放**：对同一 `block` 调用两次 `free` 会导致 bitmap 状态错误（双重分配）。
3. **无线程安全**：pool 本身无锁。由 Frag 层的 `frag_lock` 保护。

---

## route_recv_queue

### 调用者责任

1. **单消费者**：`route_queue_pop()` 仅由一个线程调用（`route_router_poll`）。
2. **多生产者需加锁**：`route_queue_push()` 若从多个 ISR/线程调用，必须配置 `queue_lock`。
3. **容量上限**：队列满时 push 返回 -1，帧丢弃。调用者检查返回值。

---

## Stats (route_stats_t)

### 设计契约

- 统计计数器使用普通 `uint32_t++`，**非原子操作**。
- 多线程并发递增可能丢失少量计数——这是有意的设计取舍（避免原子操作开销）。
- **统计不影响功能正确性**——仅用于监控和调试。
- 如需精确统计，用户可在外部用原子类型包装 `route_stats_t`，或用平台特定的原子递增替代。

---

## 典型部署模式

### 模式 1：单线程裸机（最简）

```c
// 所有 lock = NULL，零开销
// 主循环：tick → poll → 处理回调 → send
while (1) {
    route_stack_tick(&stack, get_ms());
    route_stack_poll(&stack);
}
// ISR 中：
void uart_isr(void) {
    route_stack_input(&stack, rx_buf, rx_len, port_id);  // 无需 queue_lock（单写者）
}
```

### 模式 2：RTOS 双线程

```c
// 线程 1（通信线程）：tick + poll + input
// 线程 2（应用线程）：send_sync / send_async

// 配置：
// - queue_lock: critical section（ISR → input）
// - frag lock: mutex（跨线程 send vs input/tick）
// - transaction: 内部 mutex 已保护
```

### 模式 3：ISR 驱动 + 主循环处理

```c
// ISR：route_router_input()（queue_lock = 关中断）
// 主循环：poll + tick + send（单线程，frag/transaction 无需额外锁）
```
