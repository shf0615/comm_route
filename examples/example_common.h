/**
 * 示例公共头文件 — 仿真框架
 *
 * 提供：
 * - Mock port：节点间互联，port send 自动路由到对端 route_router_input
 * - Mock OS：单线程信号量/互斥锁模拟
 * - 仿真驱动：统一 tick/poll 所有节点
 */
#ifndef EXAMPLE_COMMON_H
#define EXAMPLE_COMMON_H

#include "../sources/route_stack.h"
#include <stdio.h>
#include <string.h>

// ============ 容量常量 ============

#define EX_MAX_NODES        8
#define EX_MAX_PORTS        4
#define EX_MAX_ROUTES       8
#define EX_SEEN_TABLE_SIZE  16
#define EX_RECV_QUEUE_SIZE  8
#define EX_FRAG_SIZE        64
#define EX_MAX_PAYLOAD      256
#define EX_MAX_FRAGS        (EX_MAX_PAYLOAD / EX_FRAG_SIZE)
#define EX_MAX_REASM_SLOTS  4
#define EX_POOL_BLOCKS      16
#define EX_MAX_PENDING_ACKS 8
#define EX_MAX_TRANS        4
#define EX_BLOCK_SIZE       (ROUTE_HEADER_SIZE + EX_FRAG_SIZE)

// ============ 连接表（仿真链路） ============

typedef struct {
    uint8_t valid;
    uint8_t local_node_idx;
    uint8_t local_port_id;
    uint8_t remote_node_idx;
    uint8_t remote_port_id;
} sim_link_t;

#define SIM_MAX_LINKS 32

// ============ 节点存储 ============

typedef struct {
    // Stack
    route_stack_t stack;
    route_router_ctx_t router;
    route_frag_ctx_t frag;
    route_transaction_ctx_t transaction;

    // Router 存储
    route_port_t ports[EX_MAX_PORTS];
    route_entry_t route_table[EX_MAX_ROUTES];
    route_seen_entry_t seen_table[EX_SEEN_TABLE_SIZE];
    uint8_t recv_queue_data[EX_RECV_QUEUE_SIZE * EX_BLOCK_SIZE];
    uint16_t recv_queue_lengths[EX_RECV_QUEUE_SIZE];
    uint8_t recv_queue_from_port[EX_RECV_QUEUE_SIZE];
    uint8_t tx_frame_buf[EX_BLOCK_SIZE];
    uint8_t rx_frame_buf[EX_BLOCK_SIZE];
    route_stats_t stats;

    // Frag 存储
    route_reasm_ctx_t reasm_slots[EX_MAX_REASM_SLOTS];
    uint8_t reasm_buf[EX_MAX_PAYLOAD];
    uint8_t *reasm_frag_ptrs[EX_MAX_REASM_SLOTS * EX_MAX_FRAGS];
    uint8_t reasm_frag_lens[EX_MAX_REASM_SLOTS * EX_MAX_FRAGS];
    uint8_t *pool_free_list[EX_POOL_BLOCKS];
    uint8_t pool_storage[EX_POOL_BLOCKS * EX_BLOCK_SIZE];
    route_pending_ack_t pending_acks[EX_MAX_PENDING_ACKS];
    uint8_t pending_ack_data[EX_MAX_PENDING_ACKS * EX_FRAG_SIZE];

    // Transaction 存储
    transaction_t trans_table[EX_MAX_TRANS];

    // 接收记录
    uint8_t last_recv_src;
    uint8_t last_recv_trans_id;
    uint8_t last_recv_data[EX_MAX_PAYLOAD];
    uint16_t last_recv_len;
    uint8_t recv_count;
} sim_node_t;

// ============ 全局仿真状态 ============

static sim_node_t g_nodes[EX_MAX_NODES];
static uint8_t g_node_count = 0;
static sim_link_t g_links[SIM_MAX_LINKS];
static uint8_t g_link_count = 0;

// ============ Mock Port Send ============

static int sim_port_send(uint8_t port_id, const uint8_t *buf, uint16_t len) {
    // 找到这个 port 对应的连接
    for (uint8_t i = 0; i < g_link_count; i++) {
        if (!g_links[i].valid) continue;
        if (g_links[i].local_port_id == port_id) {
            // 找到对端后，把数据投递到对端 router input
            uint8_t remote_idx = g_links[i].remote_node_idx;
            uint8_t remote_port = g_links[i].remote_port_id;
            route_router_input(&g_nodes[remote_idx].router, buf, len, remote_port);
            return ROUTE_OK;
        }
    }
    return ROUTE_ERR_NO_PORT;
}

// ============ Mock OS（单线程模拟） ============

typedef struct {
    uint8_t posted;
} mock_sem_t;

static mock_sem_t g_mock_sems[16];
static uint8_t g_mock_sem_idx = 0;

typedef struct {
    uint8_t locked;
} mock_mutex_t;

static mock_mutex_t g_mock_mutexes[16];
static uint8_t g_mock_mutex_idx = 0;

static void *mock_mutex_create(void) {
    mock_mutex_t *m = &g_mock_mutexes[g_mock_mutex_idx++];
    m->locked = 0;
    return m;
}
static void mock_mutex_lock(void *mutex) { ((mock_mutex_t *)mutex)->locked = 1; }
static void mock_mutex_unlock(void *mutex) { ((mock_mutex_t *)mutex)->locked = 0; }
static void mock_mutex_destroy(void *mutex) { (void)mutex; }

static void *mock_sem_create(void) {
    mock_sem_t *s = &g_mock_sems[g_mock_sem_idx++];
    s->posted = 0;
    return s;
}
static int mock_sem_wait(void *sem, uint32_t timeout_ms) {
    (void)timeout_ms;
    // 单线程模拟：检查是否已 post
    return ((mock_sem_t *)sem)->posted ? 0 : -1;
}
static void mock_sem_post(void *sem) { ((mock_sem_t *)sem)->posted = 1; }
static void mock_sem_destroy(void *sem) { (void)sem; }

static const route_os_t g_mock_os = {
    .mutex_create = mock_mutex_create,
    .mutex_lock = mock_mutex_lock,
    .mutex_unlock = mock_mutex_unlock,
    .mutex_destroy = mock_mutex_destroy,
    .sem_create = mock_sem_create,
    .sem_wait = mock_sem_wait,
    .sem_post = mock_sem_post,
    .sem_destroy = mock_sem_destroy,
};

// ============ 接收回调 ============

static void sim_on_recv(void *ctx, uint8_t src, uint8_t trans_id,
                        const uint8_t *data, uint16_t len) {
    sim_node_t *node = (sim_node_t *)ctx;
    node->last_recv_src = src;
    node->last_recv_trans_id = trans_id;
    if (len > 0 && len <= EX_MAX_PAYLOAD) {
        memcpy(node->last_recv_data, data, len);
    }
    node->last_recv_len = len;
    node->recv_count++;
}

// ============ 节点初始化 ============

static int sim_node_init(uint8_t node_id, uint8_t enable_frag, uint8_t enable_trans) {
    if (g_node_count >= EX_MAX_NODES) return -1;
    uint8_t idx = g_node_count++;
    sim_node_t *n = &g_nodes[idx];
    memset(n, 0, sizeof(*n));

    // Router init
    route_router_config_t rcfg = {
        .node_id = node_id,
        .default_ttl = 8,
        .frag_size = EX_FRAG_SIZE,
        .codec = NULL,
        .max_ports = EX_MAX_PORTS,
        .max_nodes = EX_MAX_ROUTES,
        .seen_table_size = EX_SEEN_TABLE_SIZE,
        .seen_expire_ms = 5000,
        .recv_queue_size = EX_RECV_QUEUE_SIZE,
        .ports = n->ports,
        .route_table = n->route_table,
        .seen_table = n->seen_table,
        .recv_queue_data = n->recv_queue_data,
        .recv_queue_lengths = n->recv_queue_lengths,
        .recv_queue_from_port = n->recv_queue_from_port,
        .tx_frame_buf = n->tx_frame_buf,
        .rx_frame_buf = n->rx_frame_buf,
        .stats = &n->stats,
    };
    route_router_init(&n->router, &rcfg);

    // Frag init
    if (enable_frag) {
        route_frag_config_t fcfg = {
            .node_id = node_id,
            .frag_size = EX_FRAG_SIZE,
            .max_payload = EX_MAX_PAYLOAD,
            .max_frags_per_msg = EX_MAX_FRAGS,
            .max_reasm_slots = EX_MAX_REASM_SLOTS,
            .reasm_timeout_ms = 3000,
            .default_ttl = 8,
            .max_pending_acks = EX_MAX_PENDING_ACKS,
            .ack_timeout_ms = 500,
            .ack_retry_max = 3,
            .pending_acks = n->pending_acks,
            .pending_ack_data = n->pending_ack_data,
            .reasm_slots = n->reasm_slots,
            .reasm_buf = n->reasm_buf,
            .reasm_frag_ptrs = n->reasm_frag_ptrs,
            .reasm_frag_lens = n->reasm_frag_lens,
            .pool_free_list = n->pool_free_list,
            .pool_storage = n->pool_storage,
            .pool_block_count = EX_POOL_BLOCKS,
            .stats = &n->stats,
        };
        route_frag_init(&n->frag, &fcfg);
    }

    // Transaction init
    if (enable_trans) {
        route_transaction_config_t tcfg = {
            .max_concurrent_trans = EX_MAX_TRANS,
            .default_timeout_ms = 2000,
            .os = &g_mock_os,
            .trans_table = n->trans_table,
            .lower_send = NULL,
            .lower_send_ctx = NULL,
            .stats = &n->stats,
        };
        route_transaction_init(&n->transaction, &tcfg);
    }

    // Wire stack
    route_stack_wire(&n->stack,
                     &n->router,
                     enable_frag ? &n->frag : NULL,
                     enable_trans ? &n->transaction : NULL);

    route_stack_set_recv_cb(&n->stack, sim_on_recv, n);

    return idx;
}

// ============ 连接两个节点 ============

static uint8_t g_next_port_id = 1;

static void sim_connect(uint8_t node_a_idx, uint8_t node_b_idx) {
    uint8_t port_a = g_next_port_id++;
    uint8_t port_b = g_next_port_id++;

    // 注册 port
    route_port_t pa = { .port_id = port_a, .send = sim_port_send, .ctx = NULL };
    route_port_t pb = { .port_id = port_b, .send = sim_port_send, .ctx = NULL };
    route_router_port_register(&g_nodes[node_a_idx].router, &pa);
    route_router_port_register(&g_nodes[node_b_idx].router, &pb);

    // 建立双向链路
    g_links[g_link_count++] = (sim_link_t){1, node_a_idx, port_a, node_b_idx, port_b};
    g_links[g_link_count++] = (sim_link_t){1, node_b_idx, port_b, node_a_idx, port_a};
}

// ============ 设置路由表 ============

static void sim_set_route(uint8_t node_idx, const route_entry_t *entries, uint8_t count) {
    route_router_table_set(&g_nodes[node_idx].router, entries, count);
}

// ============ 驱动所有节点 ============

static void sim_tick_all(uint32_t now_ms) {
    for (uint8_t i = 0; i < g_node_count; i++) {
        route_stack_tick(&g_nodes[i].stack, now_ms);
    }
}

static void sim_poll_all(void) {
    for (uint8_t i = 0; i < g_node_count; i++) {
        route_stack_poll(&g_nodes[i].stack);
    }
}

// ============ 打印统计 ============

static void sim_print_stats(uint8_t node_idx, const char *name) {
    route_stats_t *s = &g_nodes[node_idx].stats;
    printf("[%s] TX:%u RX:%u CRC_ERR:%u NO_ROUTE:%u TTL:%u DUP:%u Q_FULL:%u "
           "NO_MEM:%u RETX:%u REASM_TO:%u TRANS_TO:%u\n",
           name, s->tx_packets, s->rx_packets, s->crc_errors,
           s->drop_no_route, s->drop_ttl, s->drop_duplicate,
           s->drop_queue_full, s->drop_no_mem, s->retransmissions,
           s->reasm_timeouts, s->trans_timeouts);
}

// ============ 重置全局状态 ============

static void sim_reset(void) {
    memset(g_nodes, 0, sizeof(g_nodes));
    memset(g_links, 0, sizeof(g_links));
    g_node_count = 0;
    g_link_count = 0;
    g_next_port_id = 1;
    g_mock_sem_idx = 0;
    g_mock_mutex_idx = 0;
}

#endif // EXAMPLE_COMMON_H
