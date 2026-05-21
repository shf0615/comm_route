# 模块依赖关系

## 依赖图

```
                    ┌─────────────┐
                    │ route_stack │ (可选串联层)
                    └──┬───┬───┬─┘
                       │   │   │
          ┌────────────┘   │   └────────────┐
          ▼                ▼                 ▼
┌──────────────┐   ┌────────────┐   ┌───────────────────┐
│  Router      │   │   Frag     │   │   Transaction     │
│              │   │            │   │                   │
│ route_types  │   │ route_types│   │ route_types       │
│ route_queue  │   │ route_pool │   │                   │
│ route_crc    │   │            │   │                   │
└──────────────┘   └────────────┘   └───────────────────┘
          │                │                 │
          ▼                ▼                 ▼
    ┌──────────────────────────────────────────┐
    │              common/                      │
    │  route_types.h  route_queue  route_pool   │
    │  route_crc                                │
    └──────────────────────────────────────────┘
```

## 各模块详情

| 模块 | 文件 | 代码行 | 依赖 | 独立可编译 |
|------|------|--------|------|:---:|
| **common/types** | `route_types.h` | 92 | 仅 `<stdint.h>` `<stddef.h>` | ✓ |
| **common/crc** | `route_crc.c/h` | 28 | 仅 `<stdint.h>` `<stddef.h>` | ✓ |
| **common/pool** | `route_pool.c/h` | 44 | `route_types.h` | ✓ |
| **common/queue** | `route_queue.c/h` | 63 | `route_types.h` | ✓ |
| **Router** | `router/route_router.c/h` | 384 | `route_types.h` `route_queue.h` `route_crc.h` | ✓ |
| **Frag** | `frag/route_frag.c/h` | 472 | `route_types.h` `route_pool.h` | ✓ |
| **Transaction** | `transaction/route_transaction.c/h` | 329 | `route_types.h` | ✓ |
| **Stack** | `route_stack.c/h` | 201 | Router + Frag + Transaction（全部） | — |

## 编译组合

| 使用场景 | 需要编译的文件 |
|----------|---------------|
| **纯转发节点** | `common/*` + `router/*` |
| **大消息 fire-and-forget** | `common/*` + `router/*` + `frag/*` |
| **完整请求-响应** | `common/*` + `router/*` + `frag/*` + `transaction/*` + `route_stack.*` |
| **小消息请求-响应（无分片）** | `common/*` + `router/*` + `transaction/*` + `route_stack.*` |

## 层间连接方式

所有层间通过**函数指针 + `void *ctx`** 连接，无编译期耦合：

| 连接 | 回调签名 | 方向 |
|------|----------|------|
| Router → 上层 | `void (*deliver_cb)(void *ctx, hdr, payload, len)` | 上行 |
| Frag → Router | `int (*lower_send)(void *ctx, hdr, payload, len)` | 下行 |
| Frag → 上层 | `void (*complete_cb)(void *ctx, hdr, data, len)` | 上行 |
| Transaction → Frag | `int (*lower_send)(void *ctx, dest, trans_id, seq, type, data, len)` | 下行 |

## 可替换点

| 组件 | 注入方式 | 默认实现 |
|------|----------|----------|
| 帧编解码 | `route_codec_t *codec` 注入 Router | 8 字节 header + CRC16-CCITT |
| 可靠传输 | Frag config 中 `pending_acks` 设 NULL 禁用 | 逐片 ACK + 超时重传 |
| OS 抽象 | `route_os_t *os` 注入 Transaction | 无默认，裸机下不用 sync API |
