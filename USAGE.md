# Route 模块 - 全场景使用文档

## 目录

1. [快速开始](#1-快速开始)
2. [裸机环境集成](#2-裸机环境集成)
3. [RTOS 环境集成](#3-rtos-环境集成)
4. [异步发送与回调](#4-异步发送与回调)
5. [同步发送（阻塞等待响应）](#5-同步发送阻塞等待响应)
6. [接收请求并回复](#6-接收请求并回复)
7. [广播消息](#7-广播消息)
8. [多跳中继](#8-多跳中继)
9. [大数据分片传输](#9-大数据分片传输)
10. [多端口节点](#10-多端口节点)
11. [可靠性策略配置](#11-可靠性策略配置)
12. [自定义可靠性策略](#12-自定义可靠性策略)
13. [多实例共存](#13-多实例共存)
14. [动态路由表更新](#14-动态路由表更新)
15. [ISR 中接收数据](#15-isr-中接收数据)
16. [错误处理](#16-错误处理)
17. [资源规划与调优](#17-资源规划与调优)
18. [调试与排查](#18-调试与排查)

---

## 1. 快速开始

### 1.1 最小可运行示例（两节点直连）

```c
#include "route.h"
#include "common/route_queue.h"
#include "reliability/route_reliability.h"

// === 物理层：两节点共享缓冲区 ===
static uint8_t shared_buf[ROUTE_BLOCK_SIZE];
static uint16_t shared_len = 0;
static volatile int has_data = 0;

static int port_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    memcpy(shared_buf, buf, len);
    shared_len = len;
    has_data = 1;
    return 0;
}

// === 节点 A（发送方） ===
route_instance_t *node_a;
// === 节点 B（接收方） ===
route_instance_t *node_b;

void setup(void) {
    // 创建实例（裸机，无 OS）
    node_a = route_create(1, NULL);
    node_b = route_create(2, NULL);

    // 注册端口
    route_port_t port = {.port_id = 0, .send = port_send};
    route_port_register(node_a, &port);
    route_port_register(node_b, &port);

    // 设置路由表
    route_entry_t route_a[] = {{.dest_id = 2, .next_hop = 2, .port_id = 0}};
    route_entry_t route_b[] = {{.dest_id = 1, .next_hop = 1, .port_id = 0}};
    route_table_set(node_a, route_a, 1);
    route_table_set(node_b, route_b, 1);

    // 设置接收回调
    route_on_recv(node_b, my_on_recv);
}
```

### 1.2 编译

```bash
gcc -I<route_dir> -I<route_dir>/common \
    <route_dir>/route_instance.c \
    <route_dir>/common/route_pool.c \
    <route_dir>/common/route_queue.c \
    <route_dir>/common/route_crc.c \
    <route_dir>/router/route_router.c \
    <route_dir>/frag/route_frag.c \
    <route_dir>/reliability/route_reliability.c \
    <route_dir>/transaction/route_transaction.c \
    your_app.c -o your_app
```

---

## 2. 裸机环境集成

### 2.1 初始化

```c
#include "route.h"
#include "common/route_queue.h"

static route_instance_t *inst;

void route_init(void) {
    // 不传入 OS 抽象，表示裸机模式
    inst = route_create(MY_NODE_ID, NULL);

    // 注册 UART 端口
    route_port_t uart_port = {
        .port_id = 0,
        .send = uart_frame_send,  // 你的 UART 发送函数
        .ctx = NULL,
    };
    route_port_register(inst, &uart_port);

    // 配置路由表
    route_entry_t routes[] = {
        {.dest_id = 2, .next_hop = 2, .port_id = 0},
        {.dest_id = 3, .next_hop = 2, .port_id = 0},
    };
    route_table_set(inst, routes, 2);

    // 设置可靠性（可选）
    route_reliability_set(inst, route_reliability_e2e_strategy());

    // 注册接收回调
    route_on_recv(inst, app_on_request);
}
```

### 2.2 主循环

```c
void main_loop(void) {
    while (1) {
        // 1. 处理接收队列中的帧
        route_poll(inst);

        // 2. 驱动定时器（分片超时、重传、事务超时）
        route_tick(inst, HAL_GetTick());

        // 3. 其他应用逻辑
        app_process();
    }
}
```

### 2.3 UART 接收中断

```c
// 假设已有帧定界逻辑（如长度前缀或特殊标记）
void UART_RX_Complete_Callback(uint8_t *frame, uint16_t len) {
    // 在中断中直接推入接收队列（无锁安全，单生产者）
    route_queue_push(&inst->recv_queue, frame, len, 0);
}
```

### 2.4 裸机限制

| 功能 | 裸机支持 |
|------|---------|
| `route_send_async` | ✅ 支持 |
| `route_send_sync` | ❌ 需要 OS（信号量阻塞） |
| `route_broadcast` | ✅ 支持 |
| `route_reply` | ✅ 支持 |
| 多线程并发 | ❌ 单线程假设 |

---

## 3. RTOS 环境集成

### 3.1 OS 抽象层实现（以 FreeRTOS 为例）

```c
#include "FreeRTOS.h"
#include "semphr.h"

static void *freertos_mutex_create(void) {
    return xSemaphoreCreateMutex();
}
static void freertos_mutex_lock(void *mutex) {
    xSemaphoreTake((SemaphoreHandle_t)mutex, portMAX_DELAY);
}
static void freertos_mutex_unlock(void *mutex) {
    xSemaphoreGive((SemaphoreHandle_t)mutex);
}
static void freertos_mutex_destroy(void *mutex) {
    vSemaphoreDelete((SemaphoreHandle_t)mutex);
}
static void *freertos_sem_create(void) {
    return xSemaphoreCreateBinary();
}
static int freertos_sem_wait(void *sem, uint32_t timeout_ms) {
    return xSemaphoreTake((SemaphoreHandle_t)sem,
                          pdMS_TO_TICKS(timeout_ms)) == pdTRUE ? 0 : -1;
}
static void freertos_sem_post(void *sem) {
    xSemaphoreGive((SemaphoreHandle_t)sem);
}
static void freertos_sem_destroy(void *sem) {
    vSemaphoreDelete((SemaphoreHandle_t)sem);
}

static const route_os_t freertos_os = {
    .mutex_create  = freertos_mutex_create,
    .mutex_lock    = freertos_mutex_lock,
    .mutex_unlock  = freertos_mutex_unlock,
    .mutex_destroy = freertos_mutex_destroy,
    .sem_create    = freertos_sem_create,
    .sem_wait      = freertos_sem_wait,
    .sem_post      = freertos_sem_post,
    .sem_destroy   = freertos_sem_destroy,
};
```

### 3.2 初始化（带 OS）

```c
void route_init(void) {
    route_config_t cfg = {.os = &freertos_os};
    inst = route_create(MY_NODE_ID, &cfg);
    // ... 端口、路由表、可靠性配置同裸机 ...
}
```

### 3.3 轮询任务

```c
void route_task(void *param) {
    (void)param;
    while (1) {
        route_poll(inst);
        route_tick(inst, xTaskGetTickCount());
        vTaskDelay(pdMS_TO_TICKS(5));  // 5ms 轮询间隔
    }
}
```

### 3.4 应用任务中同步发送

```c
void app_task(void *param) {
    (void)param;

    uint8_t request[] = {0x01, 0x02, 0x03};
    uint8_t response[64];
    uint16_t resp_len = sizeof(response);

    // 阻塞等待响应，超时 2 秒
    int rc = route_send_sync(inst, DEST_NODE_ID,
                             request, sizeof(request),
                             response, &resp_len, 2000);
    if (rc == ROUTE_OK) {
        // 处理响应 response[0..resp_len-1]
    }
}
```

### 3.5 RTOS 线程安全保证

```
┌─────────────────────────────────────┐
│  应用任务（可多个）                    │
│  route_send_sync / route_send_async │──── trans_mutex 保护
├─────────────────────────────────────┤
│  路由轮询任务（单一）                  │
│  route_poll + route_tick            │──── 不可多线程并发调用
├─────────────────────────────────────┤
│  ISR                                │
│  route_queue_push                   │──── 环形缓冲区，单生产者安全
└─────────────────────────────────────┘
```

---

## 4. 异步发送与回调

### 4.1 基本用法

```c
static void on_response(int result, const uint8_t *data, uint16_t len, void *user_data) {
    int *request_id = (int *)user_data;

    if (result == ROUTE_OK) {
        printf("请求 %d 成功，响应长度: %d\n", *request_id, len);
        // 处理 data[0..len-1]
    } else {
        printf("请求 %d 失败，错误码: %d\n", *request_id, result);
    }
}

void send_request(void) {
    static int req_id = 42;
    uint8_t payload[] = {0xAA, 0xBB, 0xCC};

    int rc = route_send_async(inst, DEST_NODE, payload, sizeof(payload),
                              on_response, &req_id);
    if (rc != ROUTE_OK) {
        printf("发送失败: %d\n", rc);
    }
}
```

### 4.2 注意事项

- 回调在 `route_poll()` 或 `route_tick()` 的上下文中被调用
- 回调中不要执行耗时操作（会阻塞轮询循环）
- `user_data` 指向的内存必须在回调触发前保持有效
- 回调可能在超时后被调用（`result == ROUTE_ERR_TIMEOUT`）

### 4.3 超时时长

异步发送的超时 = `ROUTE_ACK_TIMEOUT_MS × (ROUTE_ACK_RETRY_MAX + 2)`

默认值 = 1000 × (3 + 2) = **5000ms**

---

## 5. 同步发送（阻塞等待响应）

### 5.1 基本用法

```c
void sync_request_example(void) {
    uint8_t request[] = {CMD_READ_SENSOR, 0x01};
    uint8_t response[128];
    uint16_t resp_len = sizeof(response);  // 传入缓冲区大小

    int rc = route_send_sync(inst, SENSOR_NODE_ID,
                             request, sizeof(request),
                             response, &resp_len, 3000);  // 3秒超时

    switch (rc) {
    case ROUTE_OK:
        printf("传感器数据: ");
        for (uint16_t i = 0; i < resp_len; i++) printf("%02X ", response[i]);
        printf("\n");
        break;
    case ROUTE_ERR_TIMEOUT:
        printf("传感器节点无响应\n");
        break;
    case ROUTE_ERR_NO_ROUTE:
        printf("无到传感器的路由\n");
        break;
    default:
        printf("错误: %d\n", rc);
        break;
    }
}
```

### 5.2 前置条件

- **必须提供 OS 抽象层**（`route_config_t.os != NULL`）
- 调用线程会阻塞直到收到响应或超时
- 必须有另一个线程/任务运行 `route_poll()` + `route_tick()`

### 5.3 响应缓冲区

```c
// resp_len 的双重含义：
uint16_t resp_len = 64;  // 传入：缓冲区最大容量
route_send_sync(..., response, &resp_len, timeout);
// 返回后 resp_len = 实际响应长度（可能小于 64）
// 如果响应超过 64 字节，会被截断
```

---

## 6. 接收请求并回复

### 6.1 注册接收回调

```c
static void on_request(route_instance_t *inst, uint8_t src,
                       uint8_t trans_id, const uint8_t *data, uint16_t len) {
    printf("收到来自节点 %d 的请求，长度 %d\n", src, len);

    // 解析请求
    uint8_t cmd = data[0];

    // 构造响应
    uint8_t reply[32];
    uint16_t reply_len = 0;

    switch (cmd) {
    case CMD_READ_SENSOR:
        reply[0] = STATUS_OK;
        reply[1] = read_temperature();
        reply[2] = read_humidity();
        reply_len = 3;
        break;
    case CMD_GET_VERSION:
        reply[0] = VERSION_MAJOR;
        reply[1] = VERSION_MINOR;
        reply_len = 2;
        break;
    default:
        reply[0] = STATUS_UNKNOWN_CMD;
        reply_len = 1;
        break;
    }

    // 发送回复（必须使用 route_reply，不是 route_send_async）
    route_reply(inst, src, trans_id, reply, reply_len);
}

void init(void) {
    // ...
    route_on_recv(inst, on_request);
}
```

### 6.2 关键要点

- **必须使用 `route_reply()`** 而非 `route_send_async()` 来回复请求
- `trans_id` 必须原样传回——它用于让发送方匹配请求和响应
- 回复的帧类型为 `ROUTE_TYPE_RESPONSE`，而非 `ROUTE_TYPE_REQUEST`
- 可以选择不回复（忽略请求），发送方将超时

### 6.3 回复大数据

```c
// route_reply 内部会自动分片（支持最多 ROUTE_MAX_PAYLOAD = 256 字节）
uint8_t large_reply[200];
fill_large_data(large_reply, 200);
route_reply(inst, src, trans_id, large_reply, 200);  // 自动拆分为 7 个分片
```

---

## 7. 广播消息

### 7.1 发送广播

```c
void broadcast_example(void) {
    uint8_t announce[] = {MSG_HEARTBEAT, MY_NODE_ID, get_status()};

    int rc = route_broadcast(inst, announce, sizeof(announce), NULL, NULL);
    if (rc != ROUTE_OK) {
        printf("广播失败: %d\n", rc);
    }
}
```

### 7.2 接收广播

广播消息通过与单播相同的 `on_recv_cb` 回调接收：

```c
static void on_request(route_instance_t *inst, uint8_t src,
                       uint8_t trans_id, const uint8_t *data, uint16_t len) {
    if (trans_id == 0 && data[0] == MSG_HEARTBEAT) {
        // 这是广播消息（trans_id=0 是广播的特征）
        printf("节点 %d 心跳，状态: %d\n", src, data[2]);
        // 广播消息通常不需要回复
        return;
    }

    // 普通单播请求，需要回复
    // ...
    route_reply(inst, src, trans_id, reply, reply_len);
}
```

### 7.3 广播限制

| 限制项 | 说明 |
|--------|------|
| 最大载荷 | `ROUTE_FRAG_SIZE`（32 字节），不支持分片 |
| 可靠性 | 无 ACK，不保证所有节点都能收到 |
| 回调参数 | `route_broadcast()` 的 `cb` 参数当前未实现（传 NULL） |
| 去重 | 依赖 seen 表，防止环路重复投递 |
| TTL | 默认 8 跳 |

---

## 8. 多跳中继

### 8.1 网络拓扑示例

```
节点 A(ID=1) ←UART→ 节点 B(ID=2) ←SPI→ 节点 C(ID=3) ←UART→ 节点 D(ID=4)
```

### 8.2 中继节点配置（节点 B）

```c
void setup_relay_node_b(void) {
    inst_b = route_create(2, &cfg);

    // 端口 0：连接 A（UART）
    route_port_t port_to_a = {.port_id = 0, .send = uart_send_to_a};
    route_port_register(inst_b, &port_to_a);

    // 端口 1：连接 C（SPI）
    route_port_t port_to_c = {.port_id = 1, .send = spi_send_to_c};
    route_port_register(inst_b, &port_to_c);

    // 路由表：知道如何到达所有节点
    route_entry_t routes[] = {
        {.dest_id = 1, .next_hop = 1, .port_id = 0},  // A 直连
        {.dest_id = 3, .next_hop = 3, .port_id = 1},  // C 直连
        {.dest_id = 4, .next_hop = 3, .port_id = 1},  // D 经 C
    };
    route_table_set(inst_b, routes, 3);

    // 中继节点也可以接收发给自己的消息
    route_on_recv(inst_b, on_request_b);

    // 可靠性（可选，用于发给自己的消息）
    route_reliability_set(inst_b, route_reliability_e2e_strategy());
}
```

### 8.3 中继行为（自动）

中继节点**无需额外代码**。路由层自动完成：

1. 收到帧 → 解析目的地
2. 目的地不是本机 → 查路由表 → TTL 递减 → 转发到对应端口
3. 目的地是本机 → 投递到上层

### 8.4 端节点路由表（节点 A）

```c
// A 只需知道"所有非直连节点经 B 转发"
route_entry_t routes_a[] = {
    {.dest_id = 2, .next_hop = 2, .port_id = 0},  // B 直连
    {.dest_id = 3, .next_hop = 2, .port_id = 0},  // C 经 B
    {.dest_id = 4, .next_hop = 2, .port_id = 0},  // D 经 B
};
route_table_set(inst_a, routes_a, 3);
```

### 8.5 TTL 与环路保护

- 每跳 TTL 递减 1，到 0 则丢弃
- 默认 TTL = 8，支持最多 8 跳网络
- seen 表防止同一帧被重复转发

---

## 9. 大数据分片传输

### 9.1 自动分片（对应用透明）

```c
// 发送 200 字节数据，自动拆分为 7 个分片（每片 32 字节，最后一片 8 字节）
uint8_t large_data[200];
fill_data(large_data, 200);

int rc = route_send_async(inst, dest, large_data, 200, on_response, NULL);
// 底层自动：
// frag 0: [0..31]   (32 bytes)
// frag 1: [32..63]  (32 bytes)
// frag 2: [64..95]  (32 bytes)
// frag 3: [96..127] (32 bytes)
// frag 4: [128..159](32 bytes)
// frag 5: [160..191](32 bytes)
// frag 6: [192..199](8 bytes)
```

### 9.2 接收端重组（自动）

接收端无需额外代码，分片层自动：
1. 按 `(src, seq)` 识别属于同一消息的分片
2. 按 `frag_idx` 顺序存储各分片
3. 当 `received_count == frag_total` 时重组并投递完整消息

### 9.3 分片参数

| 参数 | 值 | 说明 |
|------|:--:|------|
| 单片最大载荷 | 32 字节 | `ROUTE_FRAG_SIZE` |
| 最大总载荷 | 256 字节 | `ROUTE_MAX_PAYLOAD` |
| 最大分片数 | 9 | `(256/32) + 1` |
| 并发重组 | 4 路 | `ROUTE_MAX_REASM_SLOTS` |
| 重组超时 | 5 秒 | `ROUTE_REASM_TIMEOUT_MS` |

### 9.4 分片失败场景

```c
// 超过 256 字节会在分片层计算出 frag_total > 9，
// 但 route_frag_send 的 frag_total 是 uint8_t，
// 计算 (256+32-1)/32 = 9 没问题，(257+32-1)/32 = 9 也没问题
// 实际限制来自接收端 reasm_buf 大小。
// 建议：应用层确保单次发送不超过 ROUTE_MAX_PAYLOAD 字节。
```

---

## 10. 多端口节点

### 10.1 场景：网关节点连接多种总线

```c
// 网关同时连接 UART（到传感器网络）和 CAN（到控制网络）
void setup_gateway(void) {
    inst = route_create(GATEWAY_ID, &cfg);

    // 端口 0：UART（连接传感器子网）
    route_port_t uart_port = {
        .port_id = 0,
        .send = uart_send,
        .ctx = &uart_handle,
    };
    route_port_register(inst, &uart_port);

    // 端口 1：CAN（连接控制子网）
    route_port_t can_port = {
        .port_id = 1,
        .send = can_send,
        .ctx = &can_handle,
    };
    route_port_register(inst, &can_port);

    // 端口 2：SPI（连接存储节点）
    route_port_t spi_port = {
        .port_id = 2,
        .send = spi_send,
        .ctx = &spi_handle,
    };
    route_port_register(inst, &spi_port);

    // 路由表：不同目的走不同端口
    route_entry_t routes[] = {
        {.dest_id = 10, .next_hop = 10, .port_id = 0},  // 传感器 10 经 UART
        {.dest_id = 11, .next_hop = 11, .port_id = 0},  // 传感器 11 经 UART
        {.dest_id = 20, .next_hop = 20, .port_id = 1},  // 控制器 20 经 CAN
        {.dest_id = 21, .next_hop = 21, .port_id = 1},  // 控制器 21 经 CAN
        {.dest_id = 30, .next_hop = 30, .port_id = 2},  // 存储节点经 SPI
    };
    route_table_set(inst, routes, 5);
}
```

### 10.2 多端口接收

```c
// 从不同端口接收数据时，需标明来源端口（用于广播转发时排除入口）
void uart_rx_handler(uint8_t *frame, uint16_t len) {
    route_queue_push(&inst->recv_queue, frame, len, 0);  // from port 0
}

void can_rx_handler(uint8_t *frame, uint16_t len) {
    route_queue_push(&inst->recv_queue, frame, len, 1);  // from port 1
}

void spi_rx_handler(uint8_t *frame, uint16_t len) {
    route_queue_push(&inst->recv_queue, frame, len, 2);  // from port 2
}
```

---

## 11. 可靠性策略配置

### 11.1 启用端到端 ACK

```c
#include "reliability/route_reliability.h"

// 使用内置策略
route_reliability_set(inst, route_reliability_e2e_strategy());
```

### 11.2 不使用可靠性（最简配置）

```c
// 不调用 route_reliability_set()，或传 NULL
route_reliability_set(inst, NULL);
// 此时：
// - 不发送 ACK
// - 不重传
// - 丢包会导致事务超时
```

### 11.3 可靠性工作流程

```
发送方                          接收方
  │                               │
  ├── 发送 REQUEST ──────────────>│
  │   [存入 pending_acks]         │
  │                               ├── 收到，重组完成
  │                               ├── 发送 ACK ──────────>│（发送方收到）
  │<────────── ACK ───────────────┤                       │
  │   [清除 pending_acks]         │                       │
  │                               │                       │
  │── 如果 ACK 超时：重传 ────────>│                       │
  │   [retry_count++]             │                       │
  │                               │                       │
  │── 重传 3 次仍无 ACK：放弃 ────│                       │
```

### 11.4 参数调优

```c
// 在编译时覆盖默认值（在包含头文件前或通过编译参数）
#define ROUTE_ACK_TIMEOUT_MS   500   // 500ms 重传间隔（低延迟网络）
#define ROUTE_ACK_RETRY_MAX    5     // 最多重试 5 次（不可靠网络）
```

---

## 12. 自定义可靠性策略

### 12.1 实现自定义策略

```c
// 示例：仅记录日志，不实际重传的"调试策略"
static int debug_on_send(route_instance_t *inst, uint8_t dest, uint8_t seq,
                         uint8_t trans_id, const uint8_t *data, uint16_t len) {
    (void)inst; (void)data; (void)len;
    printf("[RELIABILITY] 跟踪: dest=%d seq=%d trans_id=%d\n", dest, seq, trans_id);
    return ROUTE_OK;
}

static void debug_on_recv_ack(route_instance_t *inst, uint8_t src, uint8_t seq) {
    (void)inst;
    printf("[RELIABILITY] 收到 ACK: src=%d seq=%d\n", src, seq);
}

static void debug_on_tick(route_instance_t *inst, uint32_t now_ms) {
    (void)inst; (void)now_ms;
    // 不做重传
}

static const reliability_strategy_t debug_strategy = {
    .on_send = debug_on_send,
    .on_recv_ack = debug_on_recv_ack,
    .on_tick = debug_on_tick,
};

// 使用
route_reliability_set(inst, &debug_strategy);
```

### 12.2 策略接口说明

```c
typedef struct {
    // 发送数据时调用，用于记录待确认信息
    // 返回 ROUTE_OK 表示成功，其他值表示拒绝发送
    int (*on_send)(route_instance_t *inst, uint8_t dest, uint8_t seq,
                   uint8_t trans_id, const uint8_t *data, uint16_t len);

    // 收到 ACK 帧时调用
    void (*on_recv_ack)(route_instance_t *inst, uint8_t src, uint8_t seq);

    // 周期性调用（由 route_tick 驱动），用于检查超时和重传
    void (*on_tick)(route_instance_t *inst, uint32_t now_ms);
} reliability_strategy_t;
```

---

## 13. 多实例共存

### 13.1 场景：同一 MCU 运行多个独立网络

```c
// 实例 1：传感器网络（node_id=5）
route_instance_t *sensor_net = route_create(5, &cfg);
route_port_register(sensor_net, &uart1_port);
route_table_set(sensor_net, sensor_routes, sensor_route_count);

// 实例 2：控制网络（node_id=1）
route_instance_t *control_net = route_create(1, &cfg);
route_port_register(control_net, &can_port);
route_table_set(control_net, control_routes, control_route_count);

// 两个网络完全独立，互不干扰
// 各自的轮询：
void tick_all(uint32_t now) {
    route_poll(sensor_net);
    route_tick(sensor_net, now);

    route_poll(control_net);
    route_tick(control_net, now);
}
```

### 13.2 实例数量限制

- 最多 `ROUTE_MAX_INSTANCES`（默认 4）个实例
- 每个实例约占 2.6 KB RAM
- 超过限制时 `route_create()` 返回 `NULL`

### 13.3 实例销毁

```c
route_destroy(sensor_net);  // 释放 slot，清理资源
sensor_net = NULL;

// 之后可以创建新实例复用该 slot
route_instance_t *new_net = route_create(7, &cfg);
```

---

## 14. 动态路由表更新

### 14.1 运行时更新路由表

```c
// 路由表可以随时更新（全量替换）
void update_routes_on_topology_change(void) {
    route_entry_t new_routes[] = {
        {.dest_id = 2, .next_hop = 2, .port_id = 0},
        {.dest_id = 3, .next_hop = 4, .port_id = 1},  // 路径变更：经节点4
        {.dest_id = 4, .next_hop = 4, .port_id = 1},
        {.dest_id = 5, .next_hop = 4, .port_id = 1},  // 新增节点
    };
    route_table_set(inst, new_routes, 4);
}
```

### 14.2 注意事项

- `route_table_set` 是全量替换（不是增量）
- 更新期间不加锁，如有并发转发可能短暂使用旧表（可接受）
- 进行中的事务不受影响（已发出的帧不会被撤回）

---

## 15. ISR 中接收数据

### 15.1 标准模式：ISR 推入队列

```c
// UART 接收完成中断（假设已有帧定界）
void USART1_IRQHandler(void) {
    if (frame_complete) {
        route_queue_push(&inst->recv_queue, rx_buffer, rx_len, PORT_UART1);
        rx_len = 0;
    }
}
```

### 15.2 DMA 模式

```c
void DMA1_Stream5_IRQHandler(void) {
    uint16_t len = DMA_BUFFER_SIZE - DMA1_Stream5->NDTR;
    route_queue_push(&inst->recv_queue, dma_buffer, len, PORT_UART1);
    restart_dma_receive();
}
```

### 15.3 帧定界方案

路由模块**不处理帧定界**。应用层需要在推入队列前保证每次 push 的是一个完整帧。常用定界方案：

| 方案 | 适用场景 |
|------|---------|
| 长度前缀 | `[LEN_H][LEN_L][DATA...]` |
| 特殊标记 | `0x7E ... 0x7E`（HDLC 风格） |
| 固定长度 | 所有帧等长 |
| 空闲检测 | UART 空闲中断（STM32 IDLE） |

### 15.4 队列溢出处理

```c
// route_queue_push 在队列满时返回 -1
int rc = route_queue_push(&inst->recv_queue, frame, len, port);
if (rc != 0) {
    // 队列满，丢弃帧（统计计数）
    stats.queue_overflow_count++;
}
```

---

## 16. 错误处理

### 16.1 发送错误处理

```c
int rc = route_send_async(inst, dest, data, len, callback, NULL);
switch (rc) {
case ROUTE_OK:
    // 发送已入队，等待回调
    break;
case ROUTE_ERR_FULL:
    // 事务槽位已满（8 个并发请求都在等待中）
    // 处理：等待或丢弃
    break;
case ROUTE_ERR_NO_ROUTE:
    // 路由表中无此目的地
    // 处理：检查路由配置
    break;
case ROUTE_ERR_NO_PORT:
    // 路由表指向的端口不存在
    // 处理：检查端口注册
    break;
case ROUTE_ERR_PARAM:
    // 参数错误（如同步发送无 OS）
    break;
default:
    break;
}
```

### 16.2 回调中的错误

```c
static void on_response(int result, const uint8_t *data, uint16_t len, void *ctx) {
    switch (result) {
    case ROUTE_OK:
        process_response(data, len);
        break;
    case ROUTE_ERR_TIMEOUT:
        // 对端未响应（重传已耗尽）
        handle_node_offline(ctx);
        break;
    }
}
```

### 16.3 常见错误及排查

| 现象 | 可能原因 | 排查方法 |
|------|---------|---------|
| 发送返回 `ERR_NO_ROUTE` | 路由表未配置目的地 | 检查 `route_table_set` |
| 发送返回 `ERR_FULL` | 8 个事务槽位全满 | 是否有大量未响应请求 |
| 回调收到 `ERR_TIMEOUT` | 对端未收到/未回复 | 检查物理链路、路由表 |
| 对端收不到消息 | 队列溢出 / TTL 耗尽 | 检查队列计数、跳数 |
| 收到重复消息 | seen 表回绕 | 增大 `ROUTE_SEEN_TABLE_SIZE` |

---

## 17. 资源规划与调优

### 17.1 RAM 估算公式

```
每实例 RAM ≈ POOL_BLOCK_COUNT × BLOCK_SIZE          (内存池)
           + RECV_QUEUE_SIZE × BLOCK_SIZE            (接收队列)
           + MAX_PAYLOAD                             (重组缓冲区)
           + MAX_REASM_SLOTS × (~40 + 9×ptr)        (重组上下文)
           + MAX_CONCURRENT_TRANSACTIONS × (~48)     (事务表+pending_acks)
           + MAX_NODES × 3                           (路由表)
           + SEEN_TABLE_SIZE × 4                     (去重表)
           + MAX_PORTS × sizeof(port)                (端口)

默认配置: ~2.6 KB / 实例
```

### 17.2 场景化调优建议

#### 低资源场景（< 4KB RAM 可用）

```c
#define ROUTE_MAX_INSTANCES               1
#define ROUTE_MAX_NODES                   8
#define ROUTE_MAX_PORTS                   2
#define ROUTE_MAX_CONCURRENT_TRANSACTIONS 4
#define ROUTE_MAX_PAYLOAD                 128
#define ROUTE_FRAG_SIZE                   32
#define ROUTE_MAX_REASM_SLOTS             2
#define ROUTE_POOL_BLOCK_COUNT            8
#define ROUTE_SEEN_TABLE_SIZE             8
#define ROUTE_RECV_QUEUE_SIZE             8
// 约 1.3 KB / 实例
```

#### 高吞吐场景

```c
#define ROUTE_MAX_CONCURRENT_TRANSACTIONS 16
#define ROUTE_POOL_BLOCK_COUNT            32
#define ROUTE_RECV_QUEUE_SIZE             32
#define ROUTE_SEEN_TABLE_SIZE             32
#define ROUTE_MAX_REASM_SLOTS             8
// 约 4.8 KB / 实例
```

#### 大包传输场景

```c
#define ROUTE_MAX_PAYLOAD                 1024
#define ROUTE_FRAG_SIZE                   64
#define ROUTE_POOL_BLOCK_COUNT            32
#define ROUTE_REASM_TIMEOUT_MS            10000
// 约 5.2 KB / 实例
```

### 17.3 轮询频率选择

| 场景 | 建议 `route_tick` 间隔 | 说明 |
|------|:---:|------|
| 低延迟控制 | 1-5 ms | 快速重传、快速超时检测 |
| 普通传感器网络 | 10-50 ms | 平衡功耗与响应速度 |
| 低功耗应用 | 100-500 ms | 省电优先，可接受较高延迟 |

---

## 18. 调试与排查

### 18.1 添加日志钩子

```c
// 包装 port send 函数，添加日志
static int uart_send_with_log(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    printf("[TX port%d] len=%d: ", port_id, len);
    for (uint16_t i = 0; i < len; i++) printf("%02X ", buf[i]);
    printf("\n");
    return uart_raw_send(port_id, buf, len);
}

// 包装 queue push，添加日志
void rx_with_log(uint8_t *frame, uint16_t len, uint8_t port) {
    printf("[RX port%d] len=%d: ", port, len);
    for (uint16_t i = 0; i < len; i++) printf("%02X ", frame[i]);
    printf("\n");
    route_queue_push(&inst->recv_queue, frame, len, port);
}
```

### 18.2 帧解析辅助

```c
void dump_frame(const uint8_t *frame, uint16_t len) {
    if (len < 10) { printf("帧太短\n"); return; }
    printf("  src=%d dst=%d type=%d trans_id=%d seq=%d ttl=%d frag=%d/%d payload_len=%d\n",
           frame[0], frame[1], frame[2], frame[3],
           frame[4], frame[5], frame[6], frame[7], len - 10);
}
```

### 18.3 状态监控

```c
void print_route_status(route_instance_t *inst) {
    printf("=== 路由状态 ===\n");
    printf("节点 ID: %d\n", inst->node_id);
    printf("序列号计数: %d\n", inst->seq_counter);
    printf("端口数: %d\n", inst->port_count);
    printf("路由条目数: %d\n", inst->route_count);
    printf("接收队列: %d/%d\n", inst->recv_queue.count, ROUTE_RECV_QUEUE_SIZE);
    printf("内存池可用: %d/%d\n", inst->pool.free_count, ROUTE_POOL_BLOCK_COUNT);

    printf("活跃重组: ");
    for (uint8_t i = 0; i < ROUTE_MAX_REASM_SLOTS; i++) {
        if (inst->reasm_slots[i].active) {
            printf("[src=%d seq=%d %d/%d] ",
                   inst->reasm_slots[i].src_id,
                   inst->reasm_slots[i].seq,
                   inst->reasm_slots[i].received_count,
                   inst->reasm_slots[i].frag_total);
        }
    }
    printf("\n");

    printf("活跃事务: ");
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        if (inst->trans_table[i].state != TRANS_STATE_IDLE) {
            printf("[%d: dest=%d state=%d] ", i,
                   inst->trans_table[i].dest_id,
                   inst->trans_table[i].state);
        }
    }
    printf("\n");

    if (inst->reliability) {
        printf("待确认: ");
        for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
            if (inst->pending_acks[i].active) {
                printf("[dest=%d seq=%d retry=%d] ",
                       inst->pending_acks[i].dest_id,
                       inst->pending_acks[i].seq,
                       inst->pending_acks[i].retry_count);
            }
        }
        printf("\n");
    }
}
```

### 18.4 常见问题排查清单

**问题：消息发送后对端没有收到**

```
1. [ ] 发送方 route_send_async 返回值是否为 ROUTE_OK？
2. [ ] port.send 函数是否正确发送了数据？（添加日志确认）
3. [ ] 物理层是否连通？（示波器/逻辑分析仪确认）
4. [ ] 接收端 route_queue_push 是否被调用？（队列 count 是否增长）
5. [ ] 接收端 route_poll 是否被周期性调用？
6. [ ] CRC 是否正确？（帧在传输中是否被破坏）
7. [ ] 路由表是否正确配置？（dest_id、port_id 是否匹配）
8. [ ] TTL 是否足够？（多跳网络检查跳数）
9. [ ] seen 表是否误判为重复？（高流量场景）
```

**问题：同步请求超时**

```
1. [ ] 对端是否调用了 route_reply()？
2. [ ] reply 的 trans_id 是否正确传回？
3. [ ] 响应帧的路由是否配置（反向路径）？
4. [ ] timeout_ms 是否设置足够大？（考虑多跳延迟）
5. [ ] route_poll/route_tick 是否有独立线程运行？（同步调用会阻塞）
```

---

## 附录 A：完整初始化模板

```c
#include "route.h"
#include "common/route_queue.h"
#include "reliability/route_reliability.h"

static route_instance_t *my_inst;

// 你的 OS 适配（或 NULL 表示裸机）
extern const route_os_t my_os;

// 你的发送函数
extern int my_uart_send(uint8_t port_id, const uint8_t *buf, uint16_t len);

// 接收回调
static void on_request(route_instance_t *inst, uint8_t src,
                       uint8_t trans_id, const uint8_t *data, uint16_t len) {
    // 处理请求并回复
    uint8_t reply[] = {0x00};  // ACK
    route_reply(inst, src, trans_id, reply, 1);
}

void route_app_init(void) {
    // 1. 创建实例
    route_config_t cfg = {.os = &my_os};  // 或 NULL
    my_inst = route_create(MY_NODE_ID, &cfg);
    if (my_inst == NULL) {
        // 实例池已满
        error_handler();
        return;
    }

    // 2. 注册端口
    route_port_t port = {
        .port_id = 0,
        .send = my_uart_send,
        .ctx = NULL,
    };
    route_port_register(my_inst, &port);

    // 3. 配置路由表
    route_entry_t routes[] = {
        {.dest_id = PEER_NODE_ID, .next_hop = PEER_NODE_ID, .port_id = 0},
    };
    route_table_set(my_inst, routes, 1);

    // 4. 启用可靠性（可选）
    route_reliability_set(my_inst, route_reliability_e2e_strategy());

    // 5. 注册接收回调
    route_on_recv(my_inst, on_request);
}

// UART 接收（ISR 或 DMA 回调）
void on_uart_frame_received(uint8_t *frame, uint16_t len) {
    route_queue_push(&my_inst->recv_queue, frame, len, 0);
}

// 主循环或 RTOS 任务
void route_app_run(uint32_t now_ms) {
    route_poll(my_inst);
    route_tick(my_inst, now_ms);
}
```

## 附录 B：API 快速参考

| API | 用途 | OS 需求 | 返回值 |
|-----|------|:-------:|--------|
| `route_create` | 创建实例 | 无 | 实例指针或 NULL |
| `route_destroy` | 销毁实例 | 无 | void |
| `route_port_register` | 注册物理端口 | 无 | 0=成功 |
| `route_table_set` | 设置路由表 | 无 | 0=成功 |
| `route_reliability_set` | 设置可靠性策略 | 无 | void |
| `route_on_recv` | 注册接收回调 | 无 | void |
| `route_send_sync` | 同步发送（阻塞） | **需要** | 0=成功 |
| `route_send_async` | 异步发送 | 无 | 0=成功 |
| `route_broadcast` | 广播 | 无 | 0=成功 |
| `route_reply` | 回复请求 | 无 | 0=成功 |
| `route_poll` | 处理接收队列 | 无 | void |
| `route_tick` | 驱动定时器 | 无 | void |
