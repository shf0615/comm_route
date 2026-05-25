/**
 * 示例 5：多节点拓扑仿真
 *
 * 演示：星型、链式、树形拓扑的构建与测试。
 * 本文件用于 PC 端仿真测试，不依赖硬件。
 */
#include <stdio.h>
#include <string.h>
#include "route_stack.h"

/* ==== 通用仿真基础设施 ==== */

#define SIM_MAX_NODES   8
#define SIM_MAX_PORTS   4
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       9
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      (ROUTE_HEADER_SIZE + FRAG_SIZE)
#define QUEUE_SIZE      16
#define MAX_TRANS       4

/* 单节点所有存储 */
typedef struct {
    route_port_t ports[SIM_MAX_PORTS];
    route_entry_t route_table[8];
    route_seen_entry_t seen_table[16];
    uint8_t rq_data[QUEUE_SIZE * BLOCK_SIZE];
    uint16_t rq_lengths[QUEUE_SIZE];
    uint8_t rq_from_port[QUEUE_SIZE];
    uint8_t send_frame_buf[BLOCK_SIZE];
    uint8_t fwd_frame_buf[BLOCK_SIZE];
    uint8_t rx_frame_buf[BLOCK_SIZE];
    route_reasm_ctx_t reasm_slots[REASM_SLOTS];
    uint8_t reasm_buf[MAX_PAYLOAD];
    uint8_t *reasm_frag_ptrs[REASM_SLOTS * MAX_FRAGS];
    uint16_t reasm_frag_lens[REASM_SLOTS * MAX_FRAGS];
    uint32_t pool_bitmap[(POOL_BLOCKS + 31) / 32];
    uint8_t pool_storage[POOL_BLOCKS * BLOCK_SIZE];
    route_pending_ack_t pending_acks[4];
    uint8_t pending_ack_data[4 * FRAG_SIZE];
    transaction_t trans_table[MAX_TRANS];
} sim_node_storage_t;

typedef struct {
    route_router_ctx_t router;
    route_frag_ctx_t frag;
    route_transaction_ctx_t trans;
    route_stack_t stack;
    route_stats_t stats;
    sim_node_storage_t store;
    uint8_t node_id;
    int recv_count;
} sim_node_t;

/* 连接表 */
typedef struct {
    sim_node_t *remote_node;
    uint8_t remote_port_id;
} sim_link_t;

static sim_node_t nodes[SIM_MAX_NODES];
static sim_link_t links[SIM_MAX_NODES][SIM_MAX_PORTS];
static int node_count;

/* OS stub */
static void *stub_mutex_create(void) { return (void *)1; }
static void stub_nop(void *p) { (void)p; }
static int stub_sem_wait(void *s, uint32_t t) { (void)s; (void)t; return 0; }

static const route_os_t sim_os = {
    .mutex_create  = stub_mutex_create,
    .mutex_lock    = stub_nop,
    .mutex_unlock  = stub_nop,
    .mutex_destroy = stub_nop,
    .sem_create    = stub_mutex_create,
    .sem_wait      = stub_sem_wait,
    .sem_post      = stub_nop,
    .sem_destroy   = stub_nop,
};

/* ---- port send: 通过全局 links 转发到对端 ---- */

/* 用宏为每个 (node_idx, port_idx) 生成 send 函数 */
#define MAKE_SEND(ni, pi) \
    static int send_##ni##_##pi(uint8_t port_id, const uint8_t *buf, uint16_t len) { \
        (void)port_id; \
        sim_link_t *lk = &links[ni][pi]; \
        if (lk->remote_node) \
            return route_stack_input(&lk->remote_node->stack, buf, len, lk->remote_port_id); \
        return ROUTE_ERR_NO_PORT; \
    }

/* 为 8 个节点各 4 个 port 生成（实际只用到需要的） */
MAKE_SEND(0,0) MAKE_SEND(0,1) MAKE_SEND(0,2) MAKE_SEND(0,3)
MAKE_SEND(1,0) MAKE_SEND(1,1) MAKE_SEND(1,2) MAKE_SEND(1,3)
MAKE_SEND(2,0) MAKE_SEND(2,1) MAKE_SEND(2,2) MAKE_SEND(2,3)
MAKE_SEND(3,0) MAKE_SEND(3,1) MAKE_SEND(3,2) MAKE_SEND(3,3)
MAKE_SEND(4,0) MAKE_SEND(4,1) MAKE_SEND(4,2) MAKE_SEND(4,3)
MAKE_SEND(5,0) MAKE_SEND(5,1) MAKE_SEND(5,2) MAKE_SEND(5,3)

typedef int (*port_send_fn)(uint8_t, const uint8_t *, uint16_t);

static port_send_fn send_table[SIM_MAX_NODES][SIM_MAX_PORTS] = {
    { send_0_0, send_0_1, send_0_2, send_0_3 },
    { send_1_0, send_1_1, send_1_2, send_1_3 },
    { send_2_0, send_2_1, send_2_2, send_2_3 },
    { send_3_0, send_3_1, send_3_2, send_3_3 },
    { send_4_0, send_4_1, send_4_2, send_4_3 },
    { send_5_0, send_5_1, send_5_2, send_5_3 },
};

/* ---- 接收回调 ---- */
static void sim_on_recv(void *ctx, uint8_t src, uint8_t trans_id,
                        const uint8_t *data, uint16_t len)
{
    sim_node_t *n = (sim_node_t *)ctx;
    n->recv_count++;
    printf("[Node 0x%02X] recv from 0x%02X (trans=%u): %.*s\n",
           n->node_id, src, trans_id, len, data);
}

/* ---- 初始化单个节点 ---- */
static void sim_node_init(int idx, uint8_t node_id, uint8_t num_ports)
{
    sim_node_t *n = &nodes[idx];
    sim_node_storage_t *s = &n->store;
    memset(n, 0, sizeof(*n));
    n->node_id = node_id;

    /* Router */
    route_router_config_t rcfg = {
        .node_id         = node_id,
        .default_ttl     = 8,
        .frag_size       = FRAG_SIZE,
        .codec           = NULL,
        .max_ports       = num_ports,
        .max_nodes       = 8,
        .seen_table_size = 16,
        .seen_expire_ms  = 10000,
        .recv_queue_size = QUEUE_SIZE,
        .ports           = s->ports,
        .route_table     = s->route_table,
        .seen_table      = s->seen_table,
        .recv_queue_data      = s->rq_data,
        .recv_queue_lengths   = s->rq_lengths,
        .recv_queue_from_port = s->rq_from_port,
        .send_frame_buf  = s->send_frame_buf,
        .fwd_frame_buf   = s->fwd_frame_buf,
        .rx_frame_buf    = s->rx_frame_buf,
        .stats           = &n->stats,
    };
    route_router_init(&n->router, &rcfg);

    /* 注册 ports */
    for (uint8_t p = 0; p < num_ports; p++) {
        route_port_t port = { .port_id = p, .send = send_table[idx][p], .ctx = NULL };
        route_router_port_register(&n->router, &port);
    }

    /* Frag */
    route_frag_config_t fcfg = {
        .node_id           = node_id,
        .frag_size         = FRAG_SIZE,
        .max_payload       = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS,
        .default_ttl       = 8,
        .reasm = {
            .slots      = s->reasm_slots,
            .buf        = s->reasm_buf,
            .frag_ptrs  = s->reasm_frag_ptrs,
            .frag_lens  = s->reasm_frag_lens,
            .max_slots  = REASM_SLOTS,
            .timeout_ms = 5000,
        },
        .reliability = {
            .max_pending_acks = 4,
            .ack_timeout_ms   = 1000,
            .ack_retry_max    = 3,
            .pending_acks     = s->pending_acks,
            .pending_ack_data = s->pending_ack_data,
        },
        .pool_bitmap      = s->pool_bitmap,
        .pool_storage     = s->pool_storage,
        .pool_block_count = POOL_BLOCKS,
        .stats            = &n->stats,
    };
    route_frag_init(&n->frag, &fcfg);

    /* Transaction */
    route_transaction_config_t tcfg = {
        .max_concurrent_trans = MAX_TRANS,
        .default_timeout_ms  = 5000,
        .os                  = &sim_os,
        .trans_table         = s->trans_table,
        .lower_send          = NULL,
        .lower_send_ctx      = NULL,
        .stats               = &n->stats,
    };
    route_transaction_init(&n->trans, &tcfg);

    /* Wire */
    route_stack_wire(&n->stack, &n->router, &n->frag, &n->trans);
    route_stack_set_recv_cb(&n->stack, sim_on_recv, n);
}

/* ---- 连接两个节点的 port ---- */
static void sim_connect(int node_a, uint8_t port_a, int node_b, uint8_t port_b)
{
    links[node_a][port_a].remote_node = &nodes[node_b];
    links[node_a][port_a].remote_port_id = port_b;
    links[node_b][port_b].remote_node = &nodes[node_a];
    links[node_b][port_b].remote_port_id = port_a;
}

/* ---- 驱动所有节点 ---- */
static void sim_run(uint32_t duration_ms, uint32_t step_ms)
{
    for (uint32_t t = 0; t < duration_ms; t += step_ms) {
        for (int i = 0; i < node_count; i++) {
            route_stack_tick(&nodes[i].stack, t);
            route_stack_poll(&nodes[i].stack);
        }
    }
}

/* ==== 5.1 星型拓扑 ==== */
/*
 *      [B: 0x02]
 *          |  port0
 *  [Center: 0x10] (port0=B, port1=C, port2=D)
 *       /     \
 * [C: 0x03]  [D: 0x04]
 */
static void test_star(void)
{
    printf("\n=== Star Topology Test ===\n");
    memset(links, 0, sizeof(links));
    node_count = 4;

    sim_node_init(0, 0x10, 3);  /* Center */
    sim_node_init(1, 0x02, 1);  /* B */
    sim_node_init(2, 0x03, 1);  /* C */
    sim_node_init(3, 0x04, 1);  /* D */

    sim_connect(0, 0, 1, 0);  /* Center:p0 ↔ B:p0 */
    sim_connect(0, 1, 2, 0);  /* Center:p1 ↔ C:p0 */
    sim_connect(0, 2, 3, 0);  /* Center:p2 ↔ D:p0 */

    /* Center 路由表 */
    route_entry_t center_routes[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 2 },
    };
    route_router_table_set(&nodes[0].router, center_routes, 3);

    /* 叶子路由表：所有走 port0 */
    route_entry_t leaf_routes[] = {
        { .dest_id = 0x10, .port_id = 0 },
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[1].router, leaf_routes, 4);
    route_router_table_set(&nodes[2].router, leaf_routes, 4);
    route_router_table_set(&nodes[3].router, leaf_routes, 4);

    /* B → C 跨叶通信 */
    route_stack_send_async(&nodes[1].stack, 0x03,
        (uint8_t *)"ping", 4, NULL, NULL);

    sim_run(200, 10);
    printf("C recv_count=%d (expect 1)\n", nodes[2].recv_count);
}

/* ==== 5.2 链式拓扑 ==== */
/*
 * [A:0x01] --p0:p1-- [B:0x02] --p0:p1-- [C:0x03] --p0:p1-- [D:0x04]
 */
static void test_chain(void)
{
    printf("\n=== Chain Topology Test ===\n");
    memset(links, 0, sizeof(links));
    node_count = 4;

    sim_node_init(0, 0x01, 2);
    sim_node_init(1, 0x02, 2);
    sim_node_init(2, 0x03, 2);
    sim_node_init(3, 0x04, 2);

    for (int i = 0; i < 3; i++) {
        sim_connect(i, 0, i + 1, 1);
    }

    /* A: 右走 port0 */
    route_entry_t a_rt[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[0].router, a_rt, 3);

    /* B: 左走 port1, 右走 port0 */
    route_entry_t b_rt[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[1].router, b_rt, 3);

    /* C: 左走 port1, 右走 port0 */
    route_entry_t c_rt[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[2].router, c_rt, 3);

    /* D: 左走 port1 */
    route_entry_t d_rt[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 1 },
    };
    route_router_table_set(&nodes[3].router, d_rt, 3);

    /* A → D（3 跳） */
    route_stack_send_async(&nodes[0].stack, 0x04,
        (uint8_t *)"hello-D", 7, NULL, NULL);

    sim_run(500, 10);
    printf("D recv_count=%d (expect 1)\n", nodes[3].recv_count);
}

/* ==== 5.3 树形拓扑 ==== */
/*
 *          [Root: 0x10]
 *         /             \
 *     port0             port1
 *     /                     \
 * [B: 0x02]             [C: 0x03]
 * /       \                   \
 * port0   port1             port0
 * /         \                   \
 * [D:0x04] [E:0x05]         [F:0x06]
 */
static void test_tree(void)
{
    printf("\n=== Tree Topology Test ===\n");
    memset(links, 0, sizeof(links));
    node_count = 6;

    sim_node_init(0, 0x10, 2);  /* Root */
    sim_node_init(1, 0x02, 3);  /* B: port0=D, port1=E, port2=Root */
    sim_node_init(2, 0x03, 2);  /* C: port0=F, port1=Root */
    sim_node_init(3, 0x04, 1);  /* D */
    sim_node_init(4, 0x05, 1);  /* E */
    sim_node_init(5, 0x06, 1);  /* F */

    sim_connect(0, 0, 1, 2);  /* Root:p0 ↔ B:p2 */
    sim_connect(0, 1, 2, 1);  /* Root:p1 ↔ C:p1 */
    sim_connect(1, 0, 3, 0);  /* B:p0 ↔ D:p0 */
    sim_connect(1, 1, 4, 0);  /* B:p1 ↔ E:p0 */
    sim_connect(2, 0, 5, 0);  /* C:p0 ↔ F:p0 */

    /* Root 路由表 */
    route_entry_t root_rt[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 1 },
        { .dest_id = 0x06, .port_id = 1 },
    };
    route_router_table_set(&nodes[0].router, root_rt, 5);

    /* B 路由表 */
    route_entry_t b_rt[] = {
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 1 },
        { .dest_id = 0x10, .port_id = 2 },
        { .dest_id = 0x03, .port_id = 2 },
        { .dest_id = 0x06, .port_id = 2 },
    };
    route_router_table_set(&nodes[1].router, b_rt, 5);

    /* C 路由表 */
    route_entry_t c_rt[] = {
        { .dest_id = 0x06, .port_id = 0 },
        { .dest_id = 0x10, .port_id = 1 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 1 },
        { .dest_id = 0x05, .port_id = 1 },
    };
    route_router_table_set(&nodes[2].router, c_rt, 5);

    /* D, E, F: 全走 port0 上行 */
    route_entry_t leaf_rt[] = {
        { .dest_id = 0x10, .port_id = 0 },
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
        { .dest_id = 0x05, .port_id = 0 },
        { .dest_id = 0x06, .port_id = 0 },
    };
    route_router_table_set(&nodes[3].router, leaf_rt, 6);
    route_router_table_set(&nodes[4].router, leaf_rt, 6);
    route_router_table_set(&nodes[5].router, leaf_rt, 6);

    /* D → F 跨子树通信（D→B→Root→C→F, 4跳） */
    route_stack_send_async(&nodes[3].stack, 0x06,
        (uint8_t *)"cross", 5, NULL, NULL);

    sim_run(500, 10);
    printf("F recv_count=%d (expect 1)\n", nodes[5].recv_count);
}

/* ==== 5.4 环形拓扑 ==== */
/*
 * 4 节点环 + 对角线，测试 seen_table 防重复广播和环路单播。
 *
 *   [A:0x01] --p0:p1-- [B:0x02]
 *      |  \               |
 *    p1    p2(对角线)    p0
 *      |        \         |
 *   [D:0x04] --p0:p1-- [C:0x03]
 *
 * 环路: A→B→C→D→A
 * 对角线: A:p2 ↔ C:p2（形成三角环 A-B-C-A 和 A-C-D-A）
 */
static void test_ring(void)
{
    printf("\n=== Ring Topology Test (with loop) ===\n");
    memset(links, 0, sizeof(links));
    node_count = 4;

    sim_node_init(0, 0x01, 3);  /* A: p0=B, p1=D, p2=C(对角线) */
    sim_node_init(1, 0x02, 2);  /* B: p0=C, p1=A */
    sim_node_init(2, 0x03, 3);  /* C: p0=D, p1=B, p2=A(对角线) */
    sim_node_init(3, 0x04, 2);  /* D: p0=A, p1=C */

    /* 环路连接 */
    sim_connect(0, 0, 1, 1);  /* A:p0 ↔ B:p1 */
    sim_connect(1, 0, 2, 1);  /* B:p0 ↔ C:p1 */
    sim_connect(2, 0, 3, 1);  /* C:p0 ↔ D:p1 */
    sim_connect(3, 0, 0, 1);  /* D:p0 ↔ A:p1 */
    /* 对角线 */
    sim_connect(0, 2, 2, 2);  /* A:p2 ↔ C:p2 */

    /* 路由表：每个节点选择最短路径，但环路使广播可能循环 */
    /* A: B走p0, C走p2(对角线更近), D走p1 */
    route_entry_t a_rt[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 2 },
        { .dest_id = 0x04, .port_id = 1 },
    };
    route_router_table_set(&nodes[0].router, a_rt, 3);

    /* B: A走p1, C走p0, D走p0(经C) */
    route_entry_t b_rt[] = {
        { .dest_id = 0x01, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 0 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[1].router, b_rt, 3);

    /* C: A走p2(对角线), B走p1, D走p0 */
    route_entry_t c_rt[] = {
        { .dest_id = 0x01, .port_id = 2 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x04, .port_id = 0 },
    };
    route_router_table_set(&nodes[2].router, c_rt, 3);

    /* D: A走p0, B走p1(经C), C走p1 */
    route_entry_t d_rt[] = {
        { .dest_id = 0x01, .port_id = 0 },
        { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 1 },
    };
    route_router_table_set(&nodes[3].router, d_rt, 3);

    /* 测试 1：单播 A→D，验证正常到达 */
    printf("--- Unicast A->D ---\n");
    route_stack_send_async(&nodes[0].stack, 0x04,
        (uint8_t *)"uni-D", 5, NULL, NULL);
    sim_run(200, 10);
    printf("D recv_count=%d (expect 1)\n", nodes[3].recv_count);

    /* 重置计数 */
    for (int i = 0; i < node_count; i++) nodes[i].recv_count = 0;

    /* 测试 2：广播 - 环路中 seen_table 应防止重复接收 */
    printf("--- Broadcast from A (ring + diagonal) ---\n");
    route_stack_broadcast(&nodes[0].stack, (uint8_t *)"bcast", 5);
    sim_run(500, 10);

    printf("B recv_count=%d (expect 1)\n", nodes[1].recv_count);
    printf("C recv_count=%d (expect 1)\n", nodes[2].recv_count);
    printf("D recv_count=%d (expect 1)\n", nodes[3].recv_count);

    /* 验证：有环但每个节点只收到一次（seen_table 去重） */
    int pass = (nodes[1].recv_count == 1 &&
                nodes[2].recv_count == 1 &&
                nodes[3].recv_count == 1);
    printf("Loop suppression: %s\n", pass ? "PASS" : "FAIL");

    /* 测试 3：检查 drop_duplicate 统计（环路中必然产生重复帧被丢弃） */
    uint32_t total_dup = 0;
    for (int i = 0; i < node_count; i++) {
        total_dup += nodes[i].stats.drop_duplicate;
    }
    printf("Total duplicates dropped: %u (expect > 0 due to loop)\n", total_dup);
}

/* ==== main ==== */
int main(void)
{
    test_star();
    test_chain();
    test_tree();
    test_ring();
    printf("\nAll simulation tests done.\n");
    return 0;
}
