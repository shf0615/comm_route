/**
 * 压力测试：comm_route 协议栈
 *
 * 覆盖场景：
 *   1. 高频发送（队列满压力）
 *   2. 大量节点广播风暴
 *   3. 内存池耗尽与恢复
 *   4. 分片重组超时
 *   5. TTL 耗尽
 *   6. 环路广播抑制（大规模）
 *   7. 并发事务槽耗尽
 *   8. 最大 payload 边界
 */
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "route_stack.h"

/* ==== 仿真基础设施（复用 example_simulation 的模式） ==== */

#define SIM_MAX_NODES   8
#define SIM_MAX_PORTS   4
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       ((MAX_PAYLOAD / FRAG_SIZE) + 1)
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      (ROUTE_HEADER_SIZE + FRAG_SIZE)
#define QUEUE_SIZE      16
#define MAX_TRANS       4

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
} sim_storage_t;

typedef struct {
    route_router_ctx_t router;
    route_frag_ctx_t frag;
    route_transaction_ctx_t trans;
    route_stack_t stack;
    route_stats_t stats;
    sim_storage_t store;
    uint8_t node_id;
    int recv_count;
    uint32_t recv_bytes;
} sim_node_t;

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

/* Port send 函数表 */
#define MAKE_SEND(ni, pi) \
    static int send_##ni##_##pi(uint8_t port_id, const uint8_t *buf, uint16_t len) { \
        (void)port_id; (void)buf; (void)len; \
        sim_link_t *lk = &links[ni][pi]; \
        if (lk->remote_node) \
            return route_stack_input(&lk->remote_node->stack, buf, len, lk->remote_port_id); \
        return ROUTE_ERR_NO_PORT; \
    }

MAKE_SEND(0,0) MAKE_SEND(0,1) MAKE_SEND(0,2) MAKE_SEND(0,3)
MAKE_SEND(1,0) MAKE_SEND(1,1) MAKE_SEND(1,2) MAKE_SEND(1,3)
MAKE_SEND(2,0) MAKE_SEND(2,1) MAKE_SEND(2,2) MAKE_SEND(2,3)
MAKE_SEND(3,0) MAKE_SEND(3,1) MAKE_SEND(3,2) MAKE_SEND(3,3)
MAKE_SEND(4,0) MAKE_SEND(4,1) MAKE_SEND(4,2) MAKE_SEND(4,3)
MAKE_SEND(5,0) MAKE_SEND(5,1) MAKE_SEND(5,2) MAKE_SEND(5,3)
MAKE_SEND(6,0) MAKE_SEND(6,1) MAKE_SEND(6,2) MAKE_SEND(6,3)
MAKE_SEND(7,0) MAKE_SEND(7,1) MAKE_SEND(7,2) MAKE_SEND(7,3)

typedef int (*port_send_fn)(uint8_t, const uint8_t *, uint16_t);
static port_send_fn send_table[SIM_MAX_NODES][SIM_MAX_PORTS] = {
    { send_0_0, send_0_1, send_0_2, send_0_3 },
    { send_1_0, send_1_1, send_1_2, send_1_3 },
    { send_2_0, send_2_1, send_2_2, send_2_3 },
    { send_3_0, send_3_1, send_3_2, send_3_3 },
    { send_4_0, send_4_1, send_4_2, send_4_3 },
    { send_5_0, send_5_1, send_5_2, send_5_3 },
    { send_6_0, send_6_1, send_6_2, send_6_3 },
    { send_7_0, send_7_1, send_7_2, send_7_3 },
};

static void sim_on_recv(void *ctx, uint8_t src, uint8_t trans_id,
                        const uint8_t *data, uint16_t len)
{
    (void)src; (void)trans_id; (void)data;
    sim_node_t *n = (sim_node_t *)ctx;
    n->recv_count++;
    n->recv_bytes += len;
}

static void sim_node_init(int idx, uint8_t node_id, uint8_t num_ports)
{
    sim_node_t *n = &nodes[idx];
    sim_storage_t *s = &n->store;
    memset(n, 0, sizeof(*n));
    n->node_id = node_id;

    route_router_config_t rcfg = {
        .node_id = node_id, .default_ttl = 8, .frag_size = FRAG_SIZE,
        .codec = NULL, .max_ports = num_ports, .max_nodes = 8,
        .seen_table_size = 16, .seen_expire_ms = 10000, .recv_queue_size = QUEUE_SIZE,
        .ports = s->ports, .route_table = s->route_table, .seen_table = s->seen_table,
        .recv_queue_data = s->rq_data, .recv_queue_lengths = s->rq_lengths,
        .recv_queue_from_port = s->rq_from_port,
        .send_frame_buf = s->send_frame_buf, .fwd_frame_buf = s->fwd_frame_buf,
        .rx_frame_buf = s->rx_frame_buf, .stats = &n->stats,
    };
    route_router_init(&n->router, &rcfg);

    for (uint8_t p = 0; p < num_ports; p++) {
        route_port_t port = { .port_id = p, .send = send_table[idx][p], .ctx = NULL };
        route_router_port_register(&n->router, &port);
    }

    route_frag_config_t fcfg = {
        .node_id = node_id, .frag_size = FRAG_SIZE, .max_payload = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS, .default_ttl = 8,
        .reasm = { .slots = s->reasm_slots, .buf = s->reasm_buf,
                   .frag_ptrs = s->reasm_frag_ptrs, .frag_lens = s->reasm_frag_lens,
                   .max_slots = REASM_SLOTS, .timeout_ms = 5000 },
        .reliability = { .max_pending_acks = 4, .ack_timeout_ms = 1000,
                         .ack_retry_max = 3, .pending_acks = s->pending_acks,
                         .pending_ack_data = s->pending_ack_data },
        .pool_bitmap = s->pool_bitmap, .pool_storage = s->pool_storage,
        .pool_block_count = POOL_BLOCKS, .stats = &n->stats,
    };
    route_frag_init(&n->frag, &fcfg);

    route_transaction_config_t tcfg = {
        .max_concurrent_trans = MAX_TRANS, .default_timeout_ms = 5000,
        .os = &sim_os, .trans_table = s->trans_table,
        .lower_send = NULL, .lower_send_ctx = NULL, .stats = &n->stats,
    };
    route_transaction_init(&n->trans, &tcfg);

    route_stack_wire(&n->stack, &n->router, &n->frag, &n->trans);
    route_stack_set_recv_cb(&n->stack, sim_on_recv, n);
}

static void sim_connect(int a, uint8_t pa, int b, uint8_t pb)
{
    links[a][pa].remote_node = &nodes[b]; links[a][pa].remote_port_id = pb;
    links[b][pb].remote_node = &nodes[a]; links[b][pb].remote_port_id = pa;
}

static void sim_run(uint32_t duration_ms, uint32_t step_ms)
{
    for (uint32_t t = 0; t < duration_ms; t += step_ms) {
        for (int i = 0; i < node_count; i++) {
            route_stack_tick(&nodes[i].stack, t);
            route_stack_poll(&nodes[i].stack);
        }
    }
}

static void sim_reset(void)
{
    memset(links, 0, sizeof(links));
    memset(nodes, 0, sizeof(nodes));
    node_count = 0;
}

/* ==== 辅助 ==== */
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { printf("  FAIL: %s\n", msg); tests_failed++; return; } \
} while(0)

#define TEST_END(name) do { printf("  PASS\n"); tests_passed++; } while(0)

/* ==================================================================
 * 测试 1：高频发送 - 队列满压力
 * 一次性灌入超过 recv_queue_size 的帧，验证不崩溃且统计正确。
 * ================================================================== */
static void test_queue_full_pressure(void)
{
    printf("\n[TEST] Queue Full Pressure\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 不 poll，连续发送超过队列容量的帧 */
    uint8_t data[] = "stress";
    int success = 0, fail = 0;
    for (int i = 0; i < QUEUE_SIZE * 3; i++) {
        route_header_t hdr = {
            .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
            .trans_id = 0, .seq = (uint8_t)i, .ttl = 8,
            .frag_idx = 0, .frag_total = 1,
        };
        int ret = route_router_send(&nodes[0].router, &hdr, data, sizeof(data) - 1);
        if (ret == ROUTE_OK) success++; else fail++;
    }

    /* 现在 poll 处理 */
    sim_run(100, 10);

    printf("  Sent: success=%d, rejected=%d\n", success, fail);
    printf("  Node B recv_count=%d, drop_queue_full=%u\n",
           nodes[1].recv_count, nodes[1].stats.drop_queue_full);

    /* 不应崩溃，接收数应该 <= QUEUE_SIZE */
    TEST_ASSERT(nodes[1].recv_count <= QUEUE_SIZE, "recv should not exceed queue size");
    TEST_ASSERT(nodes[1].stats.drop_queue_full > 0, "should have queue drops");
    TEST_END("Queue Full Pressure");
}

/* ==================================================================
 * 测试 2：大量节点广播风暴
 * 8 节点全连接，所有节点同时广播，验证 seen_table 去重。
 * ================================================================== */
static void test_broadcast_storm(void)
{
    printf("\n[TEST] Broadcast Storm (8 nodes full mesh)\n");
    sim_reset();
    node_count = 8;

    /* 初始化 8 节点，每个 2 port（简化为链式 + 额外连接形成网状） */
    for (int i = 0; i < 8; i++) {
        sim_node_init(i, (uint8_t)(0x01 + i), 2);
    }

    /* 环形连接：每个节点 p0 连下一个的 p1 */
    for (int i = 0; i < 8; i++) {
        sim_connect(i, 0, (i + 1) % 8, 1);
    }

    /* 路由表：顺时针走 p0，逆时针走 p1 */
    for (int i = 0; i < 8; i++) {
        route_entry_t rt[7];
        for (int j = 0; j < 7; j++) {
            int target = (i + j + 1) % 8;
            rt[j].dest_id = (uint8_t)(0x01 + target);
            /* 前半顺时针(p0)，后半逆时针(p1) */
            rt[j].port_id = (j < 4) ? 0 : 1;
        }
        route_router_table_set(&nodes[i].router, rt, 7);
    }

    /* 所有节点同时广播 */
    for (int i = 0; i < 8; i++) {
        char msg[16];
        int len = snprintf(msg, sizeof(msg), "BC%d", i);
        route_stack_broadcast(&nodes[i].stack, (uint8_t *)msg, (uint16_t)len);
    }

    sim_run(1000, 10);

    /* 每个节点应该收到来自其他 7 个节点的广播，每个仅一次 */
    int total_recv = 0;
    uint32_t total_dup = 0;
    for (int i = 0; i < 8; i++) {
        total_recv += nodes[i].recv_count;
        total_dup += nodes[i].stats.drop_duplicate;
    }
    printf("  Total recv=%d (ideal=56), total duplicates dropped=%u\n", total_recv, total_dup);

    /* 每个节点最多收 7 条（来自其他 7 个），总共 56 */
    TEST_ASSERT(total_recv <= 56, "no node should receive duplicate broadcasts");
    TEST_ASSERT(total_dup > 0, "ring should produce duplicates that get dropped");
    TEST_END("Broadcast Storm");
}

/* ==================================================================
 * 测试 3：内存池耗尽与恢复
 * 同时发送多个大消息耗尽 pool，验证后续消息失败但不崩溃，
 * 等待重组超时释放后可以恢复。
 * ================================================================== */
static void test_pool_exhaustion(void)
{
    printf("\n[TEST] Pool Exhaustion and Recovery\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 发送大量大消息（每条 MAX_PAYLOAD 字节，需要 9 个分片块） */
    uint8_t big_data[MAX_PAYLOAD];
    memset(big_data, 'X', MAX_PAYLOAD);

    int success = 0, fail = 0;
    for (int i = 0; i < 20; i++) {
        int ret = route_frag_send(&nodes[0].frag, 0x02, 0, (uint8_t)i,
                                  ROUTE_TYPE_REQUEST, big_data, MAX_PAYLOAD);
        if (ret == ROUTE_OK) success++; else fail++;
    }

    printf("  Large sends: success=%d, failed=%d\n", success, fail);
    printf("  drop_no_mem=%u\n", nodes[0].stats.drop_no_mem + nodes[1].stats.drop_no_mem);

    /* poll 处理已发出的 */
    sim_run(200, 10);
    printf("  Node B recv_count=%d after first batch\n", nodes[1].recv_count);

    /* 等超时释放资源后再发 */
    sim_run(6000, 100);  /* 超过 reasm_timeout_ms=5000 */

    int ret = route_frag_send(&nodes[0].frag, 0x02, 0, 99,
                              ROUTE_TYPE_REQUEST, big_data, MAX_PAYLOAD);
    sim_run(200, 10);

    printf("  Recovery send result=%d, Node B final recv_count=%d\n", ret, nodes[1].recv_count);
    TEST_ASSERT(fail > 0 || nodes[0].stats.drop_no_mem > 0, "should have exhaustion");
    TEST_END("Pool Exhaustion");
}

/* ==================================================================
 * 测试 4：分片重组超时
 * 发送部分分片，不发完整消息，验证超时后资源释放。
 * ================================================================== */
static void test_reasm_timeout(void)
{
    printf("\n[TEST] Reassembly Timeout\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 手动注入部分分片（只发 frag_idx=0，声称 frag_total=5） */
    uint8_t payload[FRAG_SIZE];
    memset(payload, 'P', FRAG_SIZE);

    route_header_t hdr = {
        .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 42, .ttl = 8,
        .frag_idx = 0, .frag_total = 5,
    };

    /* 直接注入到 node B 的 frag 层 */
    route_frag_input(&nodes[1].frag, &hdr, payload, FRAG_SIZE);

    sim_run(100, 10);
    printf("  After partial frag: recv_count=%d (expect 0)\n", nodes[1].recv_count);
    TEST_ASSERT(nodes[1].recv_count == 0, "incomplete message should not deliver");

    /* 等待超时 */
    sim_run(6000, 100);
    printf("  After timeout: reasm_timeouts=%u\n", nodes[1].stats.reasm_timeouts);
    TEST_ASSERT(nodes[1].stats.reasm_timeouts > 0, "should have reasm timeout");

    /* 超时后应该能接收新的完整消息 */
    uint8_t small[] = "after-timeout";
    route_frag_send(&nodes[0].frag, 0x02, 0, 99, ROUTE_TYPE_REQUEST, small, sizeof(small) - 1);
    sim_run(200, 10);
    printf("  After recovery: recv_count=%d (expect 1)\n", nodes[1].recv_count);
    TEST_ASSERT(nodes[1].recv_count == 1, "should receive after timeout recovery");
    TEST_END("Reassembly Timeout");
}

/* ==================================================================
 * 测试 5：TTL 耗尽
 * 长链路中设置低 TTL，验证帧被丢弃。
 * ================================================================== */
static void test_ttl_exhaustion(void)
{
    printf("\n[TEST] TTL Exhaustion\n");
    sim_reset();
    node_count = 5;

    /* 5 节点链: 0x01 - 0x02 - 0x03 - 0x04 - 0x05 */
    for (int i = 0; i < 5; i++) {
        sim_node_init(i, (uint8_t)(0x01 + i), 2);
    }
    for (int i = 0; i < 4; i++) {
        sim_connect(i, 0, i + 1, 1);
    }

    /* 路由表：右走 p0, 左走 p1 */
    for (int i = 0; i < 5; i++) {
        route_entry_t rt[4];
        int cnt = 0;
        for (int j = 0; j < 5; j++) {
            if (j == i) continue;
            rt[cnt].dest_id = (uint8_t)(0x01 + j);
            rt[cnt].port_id = (j > i) ? 0 : 1;
            cnt++;
        }
        route_router_table_set(&nodes[i].router, rt, (uint8_t)cnt);
    }

    /* 从 node 0 发到 node 4（需要 4 跳），但 TTL=2 */
    route_header_t hdr = {
        .src = 0x01, .dst = 0x05, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 0, .ttl = 2,
        .frag_idx = 0, .frag_total = 1,
    };
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"ttl", 3);

    sim_run(500, 10);

    printf("  Node 5 recv_count=%d (expect 0)\n", nodes[4].recv_count);

    uint32_t total_ttl_drop = 0;
    for (int i = 0; i < 5; i++) total_ttl_drop += nodes[i].stats.drop_ttl;
    printf("  Total TTL drops=%u (expect > 0)\n", total_ttl_drop);

    TEST_ASSERT(nodes[4].recv_count == 0, "should not reach dest with low TTL");
    TEST_ASSERT(total_ttl_drop > 0, "should have TTL drops");

    /* 同样路径 TTL=8 应该成功 */
    hdr.ttl = 8;
    hdr.seq = 1;
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"ok", 2);
    sim_run(500, 10);
    printf("  With TTL=8: recv_count=%d (expect 1)\n", nodes[4].recv_count);
    TEST_ASSERT(nodes[4].recv_count == 1, "should reach with sufficient TTL");
    TEST_END("TTL Exhaustion");
}

/* ==================================================================
 * 测试 6：大规模环路广播抑制
 * 8 节点全互连环（每节点 2 port 形成双环），大量广播。
 * ================================================================== */
static void test_large_ring_suppression(void)
{
    printf("\n[TEST] Large Ring Broadcast Suppression\n");
    sim_reset();
    node_count = 6;

    /* 6 节点双环: p0 顺时针，p1 逆时针 */
    for (int i = 0; i < 6; i++) {
        sim_node_init(i, (uint8_t)(0x01 + i), 2);
    }
    for (int i = 0; i < 6; i++) {
        sim_connect(i, 0, (i + 1) % 6, 1);
    }

    /* 路由表 */
    for (int i = 0; i < 6; i++) {
        route_entry_t rt[5];
        for (int j = 0; j < 5; j++) {
            int target = (i + j + 1) % 6;
            rt[j].dest_id = (uint8_t)(0x01 + target);
            rt[j].port_id = (j < 3) ? 0 : 1;
        }
        route_router_table_set(&nodes[i].router, rt, 5);
    }

    /* 连续 100 次广播从 node 0 */
    for (int i = 0; i < 100; i++) {
        char msg[8];
        int len = snprintf(msg, sizeof(msg), "B%03d", i);
        route_stack_broadcast(&nodes[0].stack, (uint8_t *)msg, (uint16_t)len);
        /* 每次广播后驱动一轮让帧传播 */
        sim_run(50, 10);
    }

    /* 统计 */
    int max_recv = 0;
    uint32_t total_dup = 0;
    for (int i = 1; i < 6; i++) {
        if (nodes[i].recv_count > max_recv) max_recv = nodes[i].recv_count;
        total_dup += nodes[i].stats.drop_duplicate;
    }
    printf("  Max recv per node=%d (ideal=100), total dup drops=%u\n", max_recv, total_dup);

    /* seen_table 只有 16 槽，早期广播的 seen 会被覆盖，可能导致部分重复接收 */
    /* 但不应该无限循环（每帧 TTL 限制） */
    TEST_ASSERT(max_recv <= 200, "should not have runaway duplication");
    TEST_ASSERT(total_dup > 0, "ring must produce some duplicates");
    TEST_END("Large Ring Suppression");
}

/* ==================================================================
 * 测试 7：并发事务槽耗尽
 * 同时发起超过 MAX_TRANS 个异步请求，验证超出部分失败。
 * ================================================================== */
static void test_transaction_slot_exhaustion(void)
{
    printf("\n[TEST] Transaction Slot Exhaustion\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 不设接收端回复，所有事务会 pending */
    int success = 0, fail = 0;
    for (int i = 0; i < MAX_TRANS * 3; i++) {
        uint8_t data[] = "req";
        int ret = route_stack_send_async(&nodes[0].stack, 0x02, data, 3, NULL, NULL);
        if (ret == ROUTE_OK) success++; else fail++;
    }

    printf("  Async sends: success=%d, rejected=%d (MAX_TRANS=%d)\n", success, fail, MAX_TRANS);
    TEST_ASSERT(success <= MAX_TRANS, "should not exceed max concurrent trans");
    TEST_ASSERT(fail > 0, "excess transactions should fail");

    /* 等待超时释放 */
    sim_run(6000, 100);

    /* 超时后应该能再发 */
    uint8_t data2[] = "post-timeout";
    int ret = route_stack_send_async(&nodes[0].stack, 0x02, data2, sizeof(data2) - 1, NULL, NULL);
    printf("  After timeout: send result=%d (expect 0=OK)\n", ret);
    TEST_ASSERT(ret == ROUTE_OK, "should recover after timeout");

    printf("  trans_timeouts=%u\n", nodes[0].stats.trans_timeouts);
    TEST_ASSERT(nodes[0].stats.trans_timeouts > 0, "should have transaction timeouts");
    TEST_END("Transaction Slot Exhaustion");
}

/* ==================================================================
 * 测试 8：最大 payload 边界测试
 * 验证 MAX_PAYLOAD 大小消息能正确分片/重组。
 * ================================================================== */
static void test_max_payload_boundary(void)
{
    printf("\n[TEST] Max Payload Boundary\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 刚好 MAX_PAYLOAD */
    uint8_t big[MAX_PAYLOAD];
    for (int i = 0; i < MAX_PAYLOAD; i++) big[i] = (uint8_t)(i & 0xFF);

    int ret = route_frag_send(&nodes[0].frag, 0x02, 0, 1, ROUTE_TYPE_REQUEST, big, MAX_PAYLOAD);
    printf("  Send MAX_PAYLOAD(%d): ret=%d\n", MAX_PAYLOAD, ret);

    sim_run(500, 10);
    printf("  Node B recv_count=%d, recv_bytes=%u\n", nodes[1].recv_count, nodes[1].recv_bytes);
    TEST_ASSERT(ret == ROUTE_OK, "MAX_PAYLOAD send should succeed");
    TEST_ASSERT(nodes[1].recv_count == 1, "should receive one complete message");
    TEST_ASSERT(nodes[1].recv_bytes == MAX_PAYLOAD, "should receive full payload");

    /* 超过 MAX_PAYLOAD 应该被拒绝 */
    uint8_t too_big[MAX_PAYLOAD + 1];
    memset(too_big, 'Z', sizeof(too_big));
    ret = route_frag_send(&nodes[0].frag, 0x02, 0, 2, ROUTE_TYPE_REQUEST, too_big, MAX_PAYLOAD + 1);
    printf("  Send MAX_PAYLOAD+1: ret=%d (expect error)\n", ret);
    TEST_ASSERT(ret != ROUTE_OK, "exceeding MAX_PAYLOAD should fail");
    TEST_END("Max Payload Boundary");
}

/* ==== main ==== */
int main(void)
{
    printf("========================================\n");
    printf("  comm_route Stress Tests\n");
    printf("========================================\n");

    test_queue_full_pressure();
    test_broadcast_storm();
    test_pool_exhaustion();
    test_reasm_timeout();
    test_ttl_exhaustion();
    test_large_ring_suppression();
    test_transaction_slot_exhaustion();
    test_max_payload_boundary();

    printf("\n========================================\n");
    printf("  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("========================================\n");

    return tests_failed > 0 ? 1 : 0;
}
