# 使用示例

## 1. 仅使用 Router（纯转发/广播节点）

```c
#include "router/route_router.h"

// ---- 存储分配 ----
static route_port_t ports[2];
static route_entry_t route_table[8];
static route_seen_entry_t seen_table[16];
static uint8_t rq_data[16 * 42];       // recv_queue_size * (HEADER_SIZE + frag_size)
static uint16_t rq_lengths[16];
static uint8_t rq_from_port[16];

// ---- port send 实现 ----
static int uart_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    // 硬件发送...
    (void)port_id;
    hal_uart_write(buf, len);
    return ROUTE_OK;
}

// ---- 收到帧回调 ----
static void on_frame_deliver(void *ctx, const route_header_t *hdr,
                             const uint8_t *payload, uint16_t payload_len) {
    (void)ctx;
    printf("Received from node %d: %.*s\n", hdr->src, payload_len, payload);
}

// ---- 初始化 ----
static route_router_ctx_t router;
static route_stats_t stats;

void app_init(void) {
    route_router_config_t cfg = {
        .node_id = 0x01,
        .default_ttl = 8,
        .frag_size = 32,
        .codec = NULL,              // 使用内置默认 codec
        .max_ports = 2,
        .max_nodes = 8,
        .seen_table_size = 16,
        .seen_expire_ms = 10000,
        .recv_queue_size = 16,
        .ports = ports,
        .route_table = route_table,
        .seen_table = seen_table,
        .recv_queue_data = rq_data,
        .recv_queue_lengths = rq_lengths,
        .recv_queue_from_port = rq_from_port,
        .stats = &stats,
    };
    route_router_init(&router, &cfg);

    // 注册端口
    route_port_t uart_port = { .port_id = 0, .send = uart_send };
    route_router_port_register(&router, &uart_port);

    // 设置路由表
    route_entry_t entries[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
    };
    route_router_table_set(&router, entries, 2);

    // 设置接收回调
    route_router_set_deliver_cb(&router, on_frame_deliver, NULL);
}

// ---- 主循环 ----
void app_loop(uint32_t now_ms) {
    route_router_tick(&router, now_ms);
    route_router_poll(&router);
}

// ---- UART 中断接收 ----
void uart_rx_isr(const uint8_t *data, uint16_t len) {
    route_router_input(&router, data, len, 0);
}

// ---- 发送广播 ----
void send_broadcast(const uint8_t *data, uint16_t len) {
    route_header_t hdr = {
        .src = 0x01,
        .dst = ROUTE_BROADCAST_ADDR,
        .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0,
        .seq = 0,
        .ttl = 8,
        .frag_idx = 0,
        .frag_total = 1,
    };
    route_router_send(&router, &hdr, data, len);
}
```

---

## 2. Router + Frag（大消息 fire-and-forget）

```c
#include "router/route_router.h"
#include "frag/route_frag.h"

// ---- 额外存储 ----
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       ((MAX_PAYLOAD / FRAG_SIZE) + 1)  // 9
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      (10 + FRAG_SIZE)  // HEADER_SIZE + FRAG_SIZE = 42

static route_reasm_ctx_t reasm_slots[REASM_SLOTS];
static uint8_t reasm_buf[MAX_PAYLOAD];
static uint8_t *reasm_frag_ptrs[REASM_SLOTS * MAX_FRAGS];
static uint8_t reasm_frag_lens[REASM_SLOTS * MAX_FRAGS];
static uint8_t *pool_free_list[POOL_BLOCKS];
static uint8_t pool_storage[POOL_BLOCKS * BLOCK_SIZE];

// Router 存储（同示例 1）
static route_port_t ports[2];
static route_entry_t route_table[8];
static route_seen_entry_t seen_table[16];
static uint8_t rq_data[16 * BLOCK_SIZE];
static uint16_t rq_lengths[16];
static uint8_t rq_from_port[16];
static route_stats_t stats;

static route_router_ctx_t router;
static route_frag_ctx_t frag;

// ---- 收到完整消息回调 ----
static void on_message(void *ctx, const route_header_t *hdr,
                       const uint8_t *data, uint16_t len) {
    (void)ctx;
    printf("Complete message from %d, len=%d\n", hdr->src, len);
}

// ---- Router → Frag 适配 ----
static void router_to_frag(void *ctx, const route_header_t *hdr,
                           const uint8_t *payload, uint16_t payload_len) {
    (void)ctx;
    route_frag_input(&frag, hdr, payload, payload_len);
}

// ---- Frag → Router 适配 ----
static int frag_to_router(void *ctx, const route_header_t *hdr,
                          const uint8_t *payload, uint16_t len) {
    (void)ctx;
    return route_router_send(&router, hdr, payload, len);
}

void app_init(void) {
    // 初始化 Router（同示例 1，省略 port/table 配置）
    route_router_config_t rcfg = {
        .node_id = 0x01, .default_ttl = 8, .frag_size = FRAG_SIZE,
        .max_ports = 2, .max_nodes = 8,
        .seen_table_size = 16, .seen_expire_ms = 10000, .recv_queue_size = 16,
        .ports = ports, .route_table = route_table, .seen_table = seen_table,
        .recv_queue_data = rq_data, .recv_queue_lengths = rq_lengths,
        .recv_queue_from_port = rq_from_port, .stats = &stats,
    };
    route_router_init(&router, &rcfg);

    // 初始化 Frag（无 reliability）
    route_frag_config_t fcfg = {
        .node_id = 0x01, .frag_size = FRAG_SIZE, .max_payload = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS, .max_reasm_slots = REASM_SLOTS,
        .reasm_timeout_ms = 5000, .default_ttl = 8,
        .max_pending_acks = 0,          // 禁用 reliability
        .pending_acks = NULL,
        .pending_ack_data = NULL,
        .reasm_slots = reasm_slots, .reasm_buf = reasm_buf,
        .reasm_frag_ptrs = reasm_frag_ptrs, .reasm_frag_lens = reasm_frag_lens,
        .pool_free_list = pool_free_list, .pool_storage = pool_storage,
        .pool_block_count = POOL_BLOCKS, .stats = &stats,
    };
    route_frag_init(&frag, &fcfg);

    // 手动连接
    route_router_set_deliver_cb(&router, router_to_frag, NULL);
    route_frag_set_lower_send(&frag, frag_to_router, NULL);
    route_frag_set_complete_cb(&frag, on_message, NULL);
}

void app_loop(uint32_t now_ms) {
    route_router_tick(&router, now_ms);
    route_frag_tick(&frag, now_ms);
    route_router_poll(&router);
}

// 发送大消息（自动分片）
void send_large_message(uint8_t dest, const uint8_t *data, uint16_t len) {
    static uint8_t seq = 0;
    route_frag_send(&frag, dest, 0, seq++, ROUTE_TYPE_REQUEST, data, len);
}
```

---

## 3. 完整栈（通过 route_stack 串联）

```c
#include "route_stack.h"

// ---- 所有存储 ----
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       9
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      42
#define MAX_TRANS       8
#define MAX_PENDING     16  // 独立于 MAX_TRANS

static route_port_t ports[2];
static route_entry_t route_table[8];
static route_seen_entry_t seen_table[16];
static uint8_t rq_data[16 * BLOCK_SIZE];
static uint16_t rq_lengths[16];
static uint8_t rq_from_port[16];

static route_reasm_ctx_t reasm_slots[REASM_SLOTS];
static uint8_t reasm_buf[MAX_PAYLOAD];
static uint8_t *reasm_frag_ptrs[REASM_SLOTS * MAX_FRAGS];
static uint8_t reasm_frag_lens[REASM_SLOTS * MAX_FRAGS];
static uint8_t *pool_free_list[POOL_BLOCKS];
static uint8_t pool_storage[POOL_BLOCKS * BLOCK_SIZE];
static route_pending_ack_t pending_acks[MAX_PENDING];
static uint8_t pending_ack_data[MAX_PENDING * FRAG_SIZE];

static transaction_t trans_table[MAX_TRANS];
static route_stats_t stats;

static route_router_ctx_t router;
static route_frag_ctx_t frag;
static route_transaction_ctx_t trans;
static route_stack_t stack;

// ---- OS 抽象（FreeRTOS 示例） ----
static const route_os_t my_os = {
    .mutex_create = (void*(*)(void))xSemaphoreCreateMutex,
    .mutex_lock   = (void(*)(void*))xSemaphoreTake_wrapper,
    .mutex_unlock = (void(*)(void*))xSemaphoreGive,
    .mutex_destroy = vSemaphoreDelete,
    .sem_create   = (void*(*)(void))xSemaphoreCreateBinary,
    .sem_wait     = (int(*)(void*,uint32_t))xSemaphoreTake_timeout,
    .sem_post     = (void(*)(void*))xSemaphoreGive,
    .sem_destroy  = vSemaphoreDelete,
};

// ---- port send ----
static int uart0_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    (void)port_id;
    return hal_uart0_write(buf, len) ? ROUTE_OK : ROUTE_ERR_TIMEOUT;
}

// ---- 应用接收回调 ----
static void on_request(void *ctx, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len) {
    (void)ctx;
    // 处理请求并回复
    uint8_t resp[] = "OK";
    route_stack_reply(&stack, src, trans_id, resp, 2);
}

// ---- 初始化 ----
void app_init(void) {
    // Router
    route_router_config_t rcfg = {
        .node_id = 0x01, .default_ttl = 8, .frag_size = FRAG_SIZE,
        .max_ports = 2, .max_nodes = 8,
        .seen_table_size = 16, .seen_expire_ms = 10000, .recv_queue_size = 16,
        .ports = ports, .route_table = route_table, .seen_table = seen_table,
        .recv_queue_data = rq_data, .recv_queue_lengths = rq_lengths,
        .recv_queue_from_port = rq_from_port, .stats = &stats,
    };
    route_router_init(&router, &rcfg);

    route_port_t p0 = { .port_id = 0, .send = uart0_send };
    route_router_port_register(&router, &p0);
    route_entry_t entries[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&router, entries, 1);

    // Frag（含 reliability）
    route_frag_config_t fcfg = {
        .node_id = 0x01, .frag_size = FRAG_SIZE, .max_payload = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS, .max_reasm_slots = REASM_SLOTS,
        .reasm_timeout_ms = 5000, .default_ttl = 8,
        .max_pending_acks = MAX_PENDING, .ack_timeout_ms = 1000, .ack_retry_max = 3,
        .pending_acks = pending_acks, .pending_ack_data = pending_ack_data,
        .reasm_slots = reasm_slots, .reasm_buf = reasm_buf,
        .reasm_frag_ptrs = reasm_frag_ptrs, .reasm_frag_lens = reasm_frag_lens,
        .pool_free_list = pool_free_list, .pool_storage = pool_storage,
        .pool_block_count = POOL_BLOCKS, .stats = &stats,
    };
    route_frag_init(&frag, &fcfg);

    // Transaction
    route_transaction_config_t tcfg = {
        .max_concurrent_trans = MAX_TRANS,
        .default_timeout_ms = 5000,
        .os = &my_os,
        .trans_table = trans_table,
        .lower_send = NULL,     // 由 stack_wire 设置
        .lower_send_ctx = NULL,
        .stats = &stats,
    };
    route_transaction_init(&trans, &tcfg);

    // 一行串联
    route_stack_wire(&stack, &router, &frag, &trans);
    route_stack_set_recv_cb(&stack, on_request, NULL);
}

// ---- 主循环 ----
void app_loop(uint32_t now_ms) {
    route_stack_tick(&stack, now_ms);
    route_stack_poll(&stack);
}

// ---- UART 中断接收 ----
void uart0_rx_isr(const uint8_t *data, uint16_t len) {
    route_stack_input(&stack, data, len, 0);
}

// ---- 发送请求（同步） ----
int send_request(uint8_t dest, const uint8_t *data, uint16_t len) {
    uint8_t resp[64];
    uint16_t resp_len = sizeof(resp);
    return route_stack_send_sync(&stack, dest, data, len, resp, &resp_len, 3000);
}

// ---- 发送请求（异步） ----
static void on_response(int result, const uint8_t *data, uint16_t len, void *user_data) {
    (void)user_data;
    if (result == ROUTE_OK) {
        printf("Got response: %.*s\n", len, data);
    } else {
        printf("Request failed: %d\n", result);
    }
}

void send_request_async(uint8_t dest, const uint8_t *data, uint16_t len) {
    route_stack_send_async(&stack, dest, data, len, on_response, NULL);
}
```

---

## 4. 自定义 Codec（替换帧格式）

```c
#include "router/route_router.h"

// 自定义帧格式：4 字节紧凑 header + payload + 1 字节 checksum
static uint16_t my_encode(const route_header_t *hdr, const uint8_t *payload,
                          uint16_t payload_len, uint8_t *frame) {
    frame[0] = (hdr->src << 4) | (hdr->dst & 0x0F);   // src:4 + dst:4
    frame[1] = (hdr->type << 6) | (hdr->ttl & 0x3F);   // type:2 + ttl:6
    frame[2] = hdr->seq;
    frame[3] = (hdr->frag_idx << 4) | (hdr->frag_total & 0x0F);
    if (payload_len > 0) memcpy(&frame[4], payload, payload_len);
    // 简单 XOR checksum
    uint8_t cksum = 0;
    for (uint16_t i = 0; i < 4 + payload_len; i++) cksum ^= frame[i];
    frame[4 + payload_len] = cksum;
    return 5 + payload_len;
}

static int my_decode(const uint8_t *frame, uint16_t frame_len,
                     route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len) {
    if (frame_len < 5) return ROUTE_ERR_PARAM;
    // 校验 checksum
    uint8_t cksum = 0;
    for (uint16_t i = 0; i < frame_len - 1; i++) cksum ^= frame[i];
    if (cksum != frame[frame_len - 1]) return ROUTE_ERR_PARAM;
    hdr->src = frame[0] >> 4;
    hdr->dst = frame[0] & 0x0F;
    hdr->type = frame[1] >> 6;
    hdr->ttl = frame[1] & 0x3F;
    hdr->seq = frame[2];
    hdr->trans_id = 0;
    hdr->frag_idx = frame[3] >> 4;
    hdr->frag_total = frame[3] & 0x0F;
    *payload_len = frame_len - 5;
    *payload = (*payload_len > 0) ? &frame[4] : NULL;
    return ROUTE_OK;
}

static const route_codec_t my_codec = {
    .encode = my_encode,
    .decode = my_decode,
    .overhead = 5,  // 4 header + 1 checksum
};

// 使用时注入
void app_init(void) {
    route_router_config_t cfg = {
        .node_id = 0x01,
        .default_ttl = 8,
        .frag_size = 32,
        .codec = &my_codec,     // ← 替换默认 codec
        // ... 其余配置
    };
    route_router_init(&router, &cfg);
}
```

---

## 5. 多节点拓扑仿真

### 5.1 星型拓扑（1 中心 + 3 叶子）

```
     [Node B: 0x02]
          |  port0
          |
[Node A: 0x01] --port1-- [Center: 0x10] --port2-- [Node C: 0x03]
                              |
                           port0
                              |
                        [Node D: 0x04]
```

```c
#include "route_stack.h"
#include <stdio.h>
#include <string.h>

// ---- 节点定义 ----
#define NODE_COUNT  4
#define CENTER_IDX  0
#define NODE_B_IDX  1
#define NODE_C_IDX  2
#define NODE_D_IDX  3

typedef struct {
    route_router_ctx_t router;
    route_frag_ctx_t frag;
    route_transaction_ctx_t trans;
    route_stack_t stack;
    route_stats_t stats;
    uint8_t node_id;
} sim_node_t;

static sim_node_t nodes[NODE_COUNT];

// ---- 连接表：port send 时查找对端 ----
typedef struct {
    sim_node_t *remote_node;
    uint8_t remote_port_id;
} sim_link_t;

// links[node_idx][port_id] = 对端信息
static sim_link_t links[NODE_COUNT][4];

static int sim_port_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    // 通过 __thread 或全局上下文找到当前节点（简化：用全局变量）
    // 实际实现中用 route_port_t.ctx 携带节点索引
    return ROUTE_OK;  // 见下方完整实现
}

// 通用 port send：通过 port.ctx 找到 node_idx
static int generic_port_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    // port.ctx 存储 node_idx（编码为 uintptr_t）
    // 这里展示思路，实际需要通过遍历找到是哪个节点的 port
    for (int n = 0; n < NODE_COUNT; n++) {
        for (uint8_t p = 0; p < nodes[n].router.port_count; p++) {
            if (nodes[n].router.ports[p].port_id == port_id &&
                nodes[n].router.ports[p].send == generic_port_send) {
                // 找到发送方节点和 port，投递到对端
                sim_link_t *link = &links[n][port_id];
                if (link->remote_node) {
                    route_stack_input(&link->remote_node->stack, buf, len, link->remote_port_id);
                }
                return ROUTE_OK;
            }
        }
    }
    return ROUTE_ERR_NO_PORT;
}

// 更简洁的方案：每个节点一组 port send 函数（通过闭包/索引）
// 以下用宏生成：

#define MAKE_PORT_SEND(node_idx, port_idx) \
    static int port_send_##node_idx##_##port_idx(uint8_t port_id, const uint8_t *buf, uint16_t len) { \
        (void)port_id; \
        sim_link_t *link = &links[node_idx][port_idx]; \
        if (link->remote_node) \
            return route_stack_input(&link->remote_node->stack, buf, len, link->remote_port_id); \
        return ROUTE_ERR_NO_PORT; \
    }

MAKE_PORT_SEND(0, 0)  // Center port0
MAKE_PORT_SEND(0, 1)  // Center port1
MAKE_PORT_SEND(0, 2)  // Center port2
MAKE_PORT_SEND(1, 0)  // Node B port0
MAKE_PORT_SEND(2, 0)  // Node C port0
MAKE_PORT_SEND(3, 0)  // Node D port0

// ---- 连接函数 ----
void sim_connect(int node_a, uint8_t port_a, int node_b, uint8_t port_b) {
    links[node_a][port_a].remote_node = &nodes[node_b];
    links[node_a][port_a].remote_port_id = port_b;
    links[node_b][port_b].remote_node = &nodes[node_a];
    links[node_b][port_b].remote_port_id = port_a;
}

// ---- 构建星型 ----
void build_star_topology(void) {
    // Center (0x10) 有 3 个 port
    // Node B (0x02) 有 1 个 port
    // Node C (0x03) 有 1 个 port
    // Node D (0x04) 有 1 个 port

    // 连接
    sim_connect(CENTER_IDX, 0, NODE_B_IDX, 0);  // Center:port0 ↔ B:port0
    sim_connect(CENTER_IDX, 1, NODE_C_IDX, 0);  // Center:port1 ↔ C:port0
    sim_connect(CENTER_IDX, 2, NODE_D_IDX, 0);  // Center:port2 ↔ D:port0

    // Center 路由表：知道所有叶子通过哪个 port 可达
    route_entry_t center_routes[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 2 },
    };
    route_router_table_set(&nodes[CENTER_IDX].router, center_routes, 3);

    // 各叶子路由表：所有目标通过 port0（即 center 方向）
    route_entry_t leaf_routes[] = {
        { .dest_id = 0x10, .port_id = 0 },
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[NODE_B_IDX].router, leaf_routes, 4);
    route_router_table_set(&nodes[NODE_C_IDX].router, leaf_routes, 4);
    route_router_table_set(&nodes[NODE_D_IDX].router, leaf_routes, 4);
}

// ---- 驱动所有节点 ----
void sim_tick_all(uint32_t now_ms) {
    for (int i = 0; i < NODE_COUNT; i++) {
        route_stack_tick(&nodes[i].stack, now_ms);
        route_stack_poll(&nodes[i].stack);
    }
}

// ---- 测试：叶子 B → 叶子 C（经 Center 转发）----
void test_star_cross_leaf(void) {
    build_star_topology();

    // B 向 C 发异步请求
    route_stack_send_async(&nodes[NODE_B_IDX].stack, 0x03,
        (uint8_t*)"ping", 4, my_callback, NULL);

    // 驱动
    for (uint32_t t = 0; t < 100; t += 10) {
        sim_tick_all(t);
    }
    // 预期：C 的 on_recv_cb 收到 "ping"
}
```

### 5.2 链式拓扑（4 节点链）

```
[A: 0x01] --p0:p1-- [B: 0x02] --p0:p1-- [C: 0x03] --p0:p1-- [D: 0x04]
```

```c
#define CHAIN_LEN 4
static sim_node_t chain[CHAIN_LEN];
// node_ids: 0x01, 0x02, 0x03, 0x04

void build_chain_topology(void) {
    // 连接
    for (int i = 0; i < CHAIN_LEN - 1; i++) {
        sim_connect(i, 0, i + 1, 1);
        // node[i].port0 ↔ node[i+1].port1
    }

    // 路由表：每个节点知道左边的走 port1，右边的走 port0
    // A: 所有目标走 port0（右边）
    route_entry_t a_routes[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&chain[0].router, a_routes, 3);

    // B: 左边(A)走 port1，右边(C,D)走 port0
    route_entry_t b_routes[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&chain[1].router, b_routes, 3);

    // C: 左边(A,B)走 port1，右边(D)走 port0
    route_entry_t c_routes[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&chain[2].router, c_routes, 3);

    // D: 所有目标走 port1（左边）
    route_entry_t d_routes[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 1 },
    };
    route_router_table_set(&chain[3].router, d_routes, 3);
}

// 测试：A → D 请求-响应（3 跳）
void test_chain_e2e(void) {
    build_chain_topology();

    // D 设置接收回调，自动回复
    route_stack_set_recv_cb(&chain[3].stack, auto_reply_cb, &chain[3].stack);

    // A 发同步请求（需要在线程中运行，或用 async）
    route_stack_send_async(&chain[0].stack, 0x04,
        (uint8_t*)"request", 7, response_cb, NULL);

    // 驱动：请求需要 3 跳到 D，响应需要 3 跳回 A
    for (uint32_t t = 0; t < 500; t += 10) {
        for (int i = 0; i < CHAIN_LEN; i++) {
            route_stack_tick(&chain[i].stack, t);
            route_stack_poll(&chain[i].stack);
        }
    }
    // 预期：response_cb 被调用，result=ROUTE_OK
}

// 测试：广播沿链传播
void test_chain_broadcast(void) {
    build_chain_topology();

    // A 广播
    route_stack_broadcast(&chain[0].stack, (uint8_t*)"bcast", 5);

    // 驱动多轮（广播需要逐跳转发）
    for (uint32_t t = 0; t < 200; t += 10) {
        for (int i = 0; i < CHAIN_LEN; i++) {
            route_stack_tick(&chain[i].stack, t);
            route_stack_poll(&chain[i].stack);
        }
    }
    // 预期：B, C, D 的 on_recv_cb 各收到一次 "bcast"
}

// 测试：TTL 不足
void test_chain_ttl_expire(void) {
    build_chain_topology();
    // A 发单播到 D，但 TTL=2（需要 3 跳）
    route_header_t hdr = {
        .src = 0x01, .dst = 0x04, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 0, .ttl = 2,
        .frag_idx = 0, .frag_total = 1,
    };
    route_router_send(&chain[0].router, &hdr, (uint8_t*)"hi", 2);

    // 驱动
    for (uint32_t t = 0; t < 100; t += 10) {
        for (int i = 0; i < CHAIN_LEN; i++) {
            route_stack_tick(&chain[i].stack, t);
            route_stack_poll(&chain[i].stack);
        }
    }
    // 预期：D 的 on_recv_cb 未被调用，C 的 stats.drop_ttl > 0
}
```

### 5.3 树形拓扑

```
            [Root: 0x10]
           /             \
       port0             port1
       /                     \
  [B: 0x02]             [C: 0x03]
  /       \                   \
port0   port1               port0
/           \                   \
[D: 0x04]  [E: 0x05]       [F: 0x06]
```

```c
#define TREE_NODES 6
// indices: ROOT=0, B=1, C=2, D=3, E=4, F=5
static sim_node_t tree[TREE_NODES];

void build_tree_topology(void) {
    // Root ↔ B (Root:port0 ↔ B:port2)
    sim_connect(0, 0, 1, 2);
    // Root ↔ C (Root:port1 ↔ C:port1)
    sim_connect(0, 1, 2, 1);
    // B ↔ D (B:port0 ↔ D:port0)
    sim_connect(1, 0, 3, 0);
    // B ↔ E (B:port1 ↔ E:port0)
    sim_connect(1, 1, 4, 0);
    // C ↔ F (C:port0 ↔ F:port0)
    sim_connect(2, 0, 5, 0);

    // Root 路由表
    route_entry_t root_routes[] = {
        { .dest_id = 0x02, .port_id = 0 },  // B 及其子树走 port0
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 1 },  // C 及其子树走 port1
        { .dest_id = 0x06, .port_id = 1 },
    };
    route_router_table_set(&tree[0].router, root_routes, 5);

    // B 路由表：D走port0, E走port1, 其余上行走port2
    route_entry_t b_routes[] = {
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 1 },
        { .dest_id = 0x10, .port_id = 2 },
        { .dest_id = 0x03, .port_id = 2 },
        { .dest_id = 0x06, .port_id = 2 },
    };
    route_router_table_set(&tree[1].router, b_routes, 5);

    // C 路由表：F走port0, 其余上行走port1
    route_entry_t c_routes[] = {
        { .dest_id = 0x06, .port_id = 0 },
        { .dest_id = 0x10, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 1 },
        { .dest_id = 0x05, .port_id = 1 },
    };
    route_router_table_set(&tree[2].router, c_routes, 5);

    // D, E, F：所有目标走 port0（上行）
    route_entry_t leaf_all_up[] = {
        { .dest_id = 0x10, .port_id = 0 },
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 0 },
        { .dest_id = 0x06, .port_id = 0 },
    };
    route_router_table_set(&tree[3].router, leaf_all_up, 6);
    route_router_table_set(&tree[4].router, leaf_all_up, 6);
    route_router_table_set(&tree[5].router, leaf_all_up, 6);
}

// 测试：跨子树通信 D → F（D→B→Root→C→F，4跳）
void test_tree_cross_subtree(void) {
    build_tree_topology();

    route_stack_set_recv_cb(&tree[5].stack, auto_reply_cb, &tree[5].stack);

    route_stack_send_async(&tree[3].stack, 0x06,
        (uint8_t*)"cross", 5, response_cb, NULL);

    for (uint32_t t = 0; t < 500; t += 10) {
        for (int i = 0; i < TREE_NODES; i++) {
            route_stack_tick(&tree[i].stack, t);
            route_stack_poll(&tree[i].stack);
        }
    }
    // 预期：F 收到 "cross"，D 收到 F 的回复
}

// 测试：同子树内通信 D → E（D→B→E，2跳）
void test_tree_same_subtree(void) {
    build_tree_topology();

    route_stack_send_async(&tree[3].stack, 0x05,
        (uint8_t*)"sibling", 7, response_cb, NULL);

    for (uint32_t t = 0; t < 200; t += 10) {
        for (int i = 0; i < TREE_NODES; i++) {
            route_stack_tick(&tree[i].stack, t);
            route_stack_poll(&tree[i].stack);
        }
    }
    // 预期：E 收到 "sibling"，不经过 Root
}

// 测试：Root 广播整棵树
void test_tree_broadcast_from_root(void) {
    build_tree_topology();

    route_stack_broadcast(&tree[0].stack, (uint8_t*)"hello tree", 10);

    for (uint32_t t = 0; t < 300; t += 10) {
        for (int i = 0; i < TREE_NODES; i++) {
            route_stack_tick(&tree[i].stack, t);
            route_stack_poll(&tree[i].stack);
        }
    }
    // 预期：B, C, D, E, F 均收到一次 "hello tree"
}

// 测试：叶子 D 广播上行
void test_tree_broadcast_from_leaf(void) {
    build_tree_topology();

    route_stack_broadcast(&tree[3].stack, (uint8_t*)"from D", 6);

    for (uint32_t t = 0; t < 400; t += 10) {
        for (int i = 0; i < TREE_NODES; i++) {
            route_stack_tick(&tree[i].stack, t);
            route_stack_poll(&tree[i].stack);
        }
    }
    // 预期：B, Root, C, E, F 均收到，seen_table 防止重复
}
```

### 5.4 仿真工具函数

```c
// 驱动所有节点
void sim_run(sim_node_t *nodes, int count, uint32_t duration_ms, uint32_t step_ms) {
    for (uint32_t t = 0; t < duration_ms; t += step_ms) {
        for (int i = 0; i < count; i++) {
            route_stack_tick(&nodes[i].stack, t);
            route_stack_poll(&nodes[i].stack);
        }
    }
}

// 自动回复回调（用于被请求方）
void auto_reply_cb(void *ctx, uint8_t src, uint8_t trans_id,
                   const uint8_t *data, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    uint8_t resp[] = "ACK";
    route_stack_reply(stack, src, trans_id, resp, 3);
}

// 断开链路模拟
void sim_disconnect(int node_idx, uint8_t port_idx) {
    links[node_idx][port_idx].remote_node = NULL;
}

// 恢复链路
void sim_reconnect(int node_a, uint8_t port_a, int node_b, uint8_t port_b) {
    sim_connect(node_a, port_a, node_b, port_b);
}
```
