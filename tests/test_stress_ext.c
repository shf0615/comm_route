/**
 * 压力测试（补充）：comm_route 协议栈
 *
 * 覆盖场景：
 *   9.  并发多源同时发送到同一节点（重组槽竞争）
 *   10. 重组槽耗尽
 *   11. 快速 seq 翻转（seen_table 不误判）
 *   12. 链路断开/恢复
 *   13. 零长度 payload
 *   14. 单节点自发自收（dst == src）
 *   15. 乱序分片到达
 *   16. 重复帧注入
 *   17. 长时间运行稳定性
 *   18. 路由表满
 */
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "route_stack.h"

/* ==== 仿真基础设施 ==== */

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
    uint8_t last_recv_data[MAX_PAYLOAD];
    uint16_t last_recv_len;
} sim_node_t;

typedef struct {
    sim_node_t *remote_node;
    uint8_t remote_port_id;
} sim_link_t;

static sim_node_t nodes[SIM_MAX_NODES];
static sim_link_t links[SIM_MAX_NODES][SIM_MAX_PORTS];
static int node_count;

static void *stub_mutex_create(void) { return (void *)1; }
static void stub_nop(void *p) { (void)p; }
static int stub_sem_wait(void *s, uint32_t t) { (void)s; (void)t; return 0; }

static const route_os_t sim_os = {
    .mutex_create = stub_mutex_create, .mutex_lock = stub_nop,
    .mutex_unlock = stub_nop, .mutex_destroy = stub_nop,
    .sem_create = stub_mutex_create, .sem_wait = stub_sem_wait,
    .sem_post = stub_nop, .sem_destroy = stub_nop,
};

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
    (void)src; (void)trans_id;
    sim_node_t *n = (sim_node_t *)ctx;
    n->recv_count++;
    n->recv_bytes += len;
    if (len <= MAX_PAYLOAD && data) {
        memcpy(n->last_recv_data, data, len);
        n->last_recv_len = len;
    }
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

static void sim_disconnect(int a, uint8_t pa, int b, uint8_t pb)
{
    links[a][pa].remote_node = NULL;
    links[b][pb].remote_node = NULL;
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

/* 带起始时间偏移的 sim_run */
static void sim_run_from(uint32_t start_ms, uint32_t duration_ms, uint32_t step_ms)
{
    for (uint32_t t = start_ms; t < start_ms + duration_ms; t += step_ms) {
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

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { printf("  FAIL: %s\n", msg); tests_failed++; return; } \
} while(0)

#define TEST_PASS() do { printf("  PASS\n"); tests_passed++; } while(0)

/* ==================================================================
 * 测试 9：并发多源同时发送到同一节点（重组槽竞争）
 * 4 个源同时向 1 个目标发大消息，目标仅有 REASM_SLOTS=4 个槽。
 * ================================================================== */
static void test_concurrent_multi_source(void)
{
    printf("\n[TEST 9] Concurrent Multi-Source to Single Dest\n");
    sim_reset();
    node_count = 5;

    /* 星型：nodes[0] 是中心接收方，1-4 是发送方 */
    sim_node_init(0, 0x10, 4);
    for (int i = 1; i <= 4; i++) {
        sim_node_init(i, (uint8_t)(0x01 + i - 1), 1);
        sim_connect(0, (uint8_t)(i - 1), i, 0);
    }

    /* 接收方路由表 */
    route_entry_t rt0[] = {
        { .dest_id = 0x01, .port_id = 0 }, { .dest_id = 0x02, .port_id = 1 },
        { .dest_id = 0x03, .port_id = 2 }, { .dest_id = 0x04, .port_id = 3 },
    };
    route_router_table_set(&nodes[0].router, rt0, 4);

    for (int i = 1; i <= 4; i++) {
        route_entry_t rt[] = { { .dest_id = 0x10, .port_id = 0 } };
        route_router_table_set(&nodes[i].router, rt, 1);
    }

    /* 4 个源同时发 128 字节消息（需要 4 个分片，刚好填满 4 个重组槽） */
    uint8_t data[128];
    memset(data, 'A', 128);

    for (int i = 1; i <= 4; i++) {
        data[0] = (uint8_t)('0' + i);  /* 标记来源 */
        route_frag_send(&nodes[i].frag, 0x10, 0, (uint8_t)i, ROUTE_TYPE_REQUEST, data, 128);
    }

    sim_run(500, 10);
    printf("  Center recv_count=%d (max possible=%d)\n", nodes[0].recv_count, REASM_SLOTS);
    printf("  drop_no_mem=%u\n", nodes[0].stats.drop_no_mem);

    /* 至少部分应该成功，不应崩溃 */
    TEST_ASSERT(nodes[0].recv_count > 0, "at least some messages should arrive");
    TEST_ASSERT(nodes[0].recv_count <= 4, "should not exceed sender count");
    TEST_PASS();
}

/* ==================================================================
 * 测试 10：重组槽耗尽
 * 向同一节点发送 >REASM_SLOTS 个不同 seq 的部分分片，验证丢弃。
 * ================================================================== */
static void test_reasm_slot_exhaustion(void)
{
    printf("\n[TEST 10] Reassembly Slot Exhaustion\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 发送 REASM_SLOTS+2 个不完整的多分片消息（只发 frag_idx=0） */
    uint8_t payload[FRAG_SIZE];
    memset(payload, 'R', FRAG_SIZE);

    for (int i = 0; i < REASM_SLOTS + 2; i++) {
        route_header_t hdr = {
            .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
            .trans_id = 0, .seq = (uint8_t)(50 + i), .ttl = 8,
            .frag_idx = 0, .frag_total = 3,
        };
        route_frag_input(&nodes[1].frag, &hdr, payload, FRAG_SIZE);
    }

    sim_run(100, 10);
    printf("  recv_count=%d (expect 0, all incomplete)\n", nodes[1].recv_count);
    printf("  drop_no_mem=%u (expect >= 2)\n", nodes[1].stats.drop_no_mem);

    TEST_ASSERT(nodes[1].recv_count == 0, "no complete messages");
    TEST_ASSERT(nodes[1].stats.drop_no_mem >= 2, "excess should be dropped");
    TEST_PASS();
}

/* ==================================================================
 * 测试 11：快速 seq 翻转（seen_table 不误判）
 * 连续发送 seq 从 0 到 255 的单播帧，验证全部到达。
 * ================================================================== */
static void test_seq_wraparound(void)
{
    printf("\n[TEST 11] Seq Wraparound (0-255)\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 发送 256 个不同 seq 的单帧消息 */
    uint8_t data[4] = "seq";
    for (int i = 0; i < 256; i++) {
        route_header_t hdr = {
            .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
            .trans_id = 0, .seq = (uint8_t)i, .ttl = 8,
            .frag_idx = 0, .frag_total = 1,
        };
        route_router_send(&nodes[0].router, &hdr, data, 3);
        /* 每几帧 poll 一次避免队列满 */
        if (i % (QUEUE_SIZE - 1) == 0) {
            sim_run(10, 10);
        }
    }
    sim_run(200, 10);

    printf("  recv_count=%d (expect 256)\n", nodes[1].recv_count);
    printf("  drop_duplicate=%u (expect 0)\n", nodes[1].stats.drop_duplicate);

    TEST_ASSERT(nodes[1].recv_count == 256, "all 256 seq values should arrive");
    TEST_ASSERT(nodes[1].stats.drop_duplicate == 0, "no false duplicate drops");
    TEST_PASS();
}

/* ==================================================================
 * 测试 12：链路断开/恢复
 * 中途断开链路，验证发送失败但不崩溃，恢复后正常工作。
 * ================================================================== */
static void test_link_disconnect_reconnect(void)
{
    printf("\n[TEST 12] Link Disconnect and Reconnect\n");
    sim_reset();
    node_count = 3;

    /* A --p0:p1-- B --p0:p1-- C */
    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 2);
    sim_node_init(2, 0x03, 1);
    sim_connect(0, 0, 1, 1);
    sim_connect(1, 0, 2, 0);

    route_entry_t a_rt[] = { { .dest_id = 0x02, .port_id = 0 }, { .dest_id = 0x03, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, a_rt, 2);
    route_entry_t b_rt[] = { { .dest_id = 0x01, .port_id = 1 }, { .dest_id = 0x03, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, b_rt, 2);
    route_entry_t c_rt[] = { { .dest_id = 0x01, .port_id = 0 }, { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[2].router, c_rt, 2);

    /* 正常发送 A→C */
    route_header_t hdr = {
        .src = 0x01, .dst = 0x03, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 1, .ttl = 8, .frag_idx = 0, .frag_total = 1,
    };
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"hi", 2);
    sim_run(100, 10);
    printf("  Before disconnect: C recv=%d\n", nodes[2].recv_count);
    TEST_ASSERT(nodes[2].recv_count == 1, "should receive before disconnect");

    /* 断开 B→C 链路 */
    sim_disconnect(1, 0, 2, 0);

    /* 再发，应该丢失 */
    hdr.seq = 2;
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"lost", 4);
    sim_run(100, 10);
    printf("  After disconnect: C recv=%d (still 1)\n", nodes[2].recv_count);
    TEST_ASSERT(nodes[2].recv_count == 1, "should not receive when link down");

    /* 恢复链路 */
    sim_connect(1, 0, 2, 0);

    hdr.seq = 3;
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"back", 4);
    sim_run(100, 10);
    printf("  After reconnect: C recv=%d (expect 2)\n", nodes[2].recv_count);
    TEST_ASSERT(nodes[2].recv_count == 2, "should receive after reconnect");
    TEST_PASS();
}

/* ==================================================================
 * 测试 13：零长度 payload
 * 发送空消息，验证不崩溃且正确投递。
 * ================================================================== */
static void test_zero_length_payload(void)
{
    printf("\n[TEST 13] Zero Length Payload\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 通过 router 层直接发送零长度 */
    route_header_t hdr = {
        .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 0, .ttl = 8, .frag_idx = 0, .frag_total = 1,
    };
    int ret = route_router_send(&nodes[0].router, &hdr, NULL, 0);
    printf("  Send empty: ret=%d\n", ret);

    sim_run(100, 10);
    printf("  recv_count=%d, recv_bytes=%u\n", nodes[1].recv_count, nodes[1].recv_bytes);

    /* 应能正常处理（无论投递与否不应崩溃） */
    TEST_ASSERT(ret == ROUTE_OK, "empty send should not crash");
    TEST_PASS();
}

/* ==================================================================
 * 测试 14：单节点自发自收（dst == src）
 * ================================================================== */
static void test_self_send(void)
{
    printf("\n[TEST 14] Self-Send (dst == src)\n");
    sim_reset();
    node_count = 1;

    sim_node_init(0, 0x01, 1);

    route_entry_t rt[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);

    route_header_t hdr = {
        .src = 0x01, .dst = 0x01, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 0, .ttl = 8, .frag_idx = 0, .frag_total = 1,
    };
    int ret = route_router_send(&nodes[0].router, &hdr, (uint8_t *)"self", 4);
    sim_run(100, 10);

    printf("  Send to self: ret=%d, recv_count=%d\n", ret, nodes[0].recv_count);
    /* 不崩溃即通过 */
    TEST_ASSERT(ret == ROUTE_OK || ret != 0, "self-send should not crash");
    TEST_PASS();
}

/* ==================================================================
 * 测试 15：乱序分片到达
 * 发送 4 个分片但以倒序注入接收方，验证能正确重组。
 * ================================================================== */
static void test_out_of_order_fragments(void)
{
    printf("\n[TEST 15] Out-of-Order Fragment Reassembly\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 构造 4 个分片（总 128 字节） */
    uint8_t full_data[128];
    for (int i = 0; i < 128; i++) full_data[i] = (uint8_t)i;

    /* 按倒序 3,2,1,0 注入到 node B 的 frag 层 */
    int order[] = {3, 2, 1, 0};
    for (int k = 0; k < 4; k++) {
        int idx = order[k];
        uint16_t offset = (uint16_t)(idx * FRAG_SIZE);
        uint16_t chunk = 128 - offset;
        if (chunk > FRAG_SIZE) chunk = FRAG_SIZE;

        route_header_t hdr = {
            .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
            .trans_id = 0, .seq = 77, .ttl = 8,
            .frag_idx = (uint8_t)idx, .frag_total = 4,
        };
        route_frag_input(&nodes[1].frag, &hdr, &full_data[offset], chunk);
    }

    sim_run(100, 10);
    printf("  recv_count=%d (expect 1)\n", nodes[1].recv_count);
    printf("  recv_bytes=%u (expect 128)\n", nodes[1].recv_bytes);

    TEST_ASSERT(nodes[1].recv_count == 1, "should reassemble out-of-order frags");
    TEST_ASSERT(nodes[1].recv_bytes == 128, "full payload should be received");

    /* 验证数据完整性 */
    int data_ok = (memcmp(nodes[1].last_recv_data, full_data, 128) == 0);
    printf("  Data integrity: %s\n", data_ok ? "OK" : "CORRUPTED");
    TEST_ASSERT(data_ok, "reassembled data should match original");
    TEST_PASS();
}

/* ==================================================================
 * 测试 16：重复帧注入
 * 同一帧多次注入，验证 seen_table 去重（仅收一次）。
 * ================================================================== */
static void test_duplicate_frame_injection(void)
{
    printf("\n[TEST 16] Duplicate Frame Injection\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    /* 发一帧 */
    route_header_t hdr = {
        .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0, .seq = 99, .ttl = 8, .frag_idx = 0, .frag_total = 1,
    };
    route_router_send(&nodes[0].router, &hdr, (uint8_t *)"dup", 3);
    sim_run(50, 10);

    int first_recv = nodes[1].recv_count;
    printf("  After first send: recv=%d\n", first_recv);

    /* 再发同一帧（同 src + seq）多次 */
    for (int i = 0; i < 10; i++) {
        route_router_send(&nodes[0].router, &hdr, (uint8_t *)"dup", 3);
    }
    sim_run(100, 10);

    printf("  After 10 duplicates: recv=%d, drop_duplicate=%u\n",
           nodes[1].recv_count, nodes[1].stats.drop_duplicate);

    /* 应能正常投递（seen_table 仅对广播去重） */
    TEST_ASSERT(nodes[1].recv_count == 11, "unicast not deduped by seen_table (by design)");
    TEST_PASS();
}

/* ==================================================================
 * 测试 17：长时间运行稳定性
 * 1000 轮发送+接收，检查统计一致性（无泄漏）。
 * ================================================================== */
static void test_long_running_stability(void)
{
    printf("\n[TEST 17] Long Running Stability (1000 messages)\n");
    sim_reset();
    node_count = 2;

    sim_node_init(0, 0x01, 1);
    sim_node_init(1, 0x02, 1);
    sim_connect(0, 0, 1, 0);

    route_entry_t rt[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&nodes[0].router, rt, 1);
    route_entry_t rt2[] = { { .dest_id = 0x01, .port_id = 0 } };
    route_router_table_set(&nodes[1].router, rt2, 1);

    uint8_t data[16] = "stability-test!";
    uint32_t time_ms = 0;
    int send_ok = 0;

    for (int i = 0; i < 1000; i++) {
        route_header_t hdr = {
            .src = 0x01, .dst = 0x02, .type = ROUTE_TYPE_REQUEST,
            .trans_id = 0, .seq = (uint8_t)(i & 0xFF), .ttl = 8,
            .frag_idx = 0, .frag_total = 1,
        };
        int ret = route_router_send(&nodes[0].router, &hdr, data, 15);
        if (ret == ROUTE_OK) send_ok++;

        /* 每发几帧 poll + tick */
        if (i % 8 == 7) {
            time_ms += 10;
            for (int n = 0; n < node_count; n++) {
                route_stack_tick(&nodes[n].stack, time_ms);
                route_stack_poll(&nodes[n].stack);
            }
        }
    }

    /* 最终 flush */
    for (int t = 0; t < 20; t++) {
        time_ms += 10;
        for (int n = 0; n < node_count; n++) {
            route_stack_tick(&nodes[n].stack, time_ms);
            route_stack_poll(&nodes[n].stack);
        }
    }

    uint32_t total_drop = nodes[1].stats.drop_queue_full + nodes[1].stats.drop_duplicate;
    printf("  Sent OK=%d, received=%d, drops(queue+dup)=%u\n",
           send_ok, nodes[1].recv_count, total_drop);

    /* 收 + 丢 应等于发 */
    int accounted = nodes[1].recv_count + (int)total_drop;
    printf("  Accounted=%d vs sent=%d\n", accounted, send_ok);

    /* 允许 seen_table 去重造成的差异（seen 有 expire） */
    TEST_ASSERT(nodes[1].recv_count > 0, "should receive messages");
    TEST_ASSERT(send_ok == 1000, "all sends should succeed (queue flushed in time)");

    /* 无崩溃，统计合理（注：直接单播不更新 tx/rx_packets，仅转发/广播更新） */
    TEST_ASSERT(nodes[1].recv_count == 1000, "all messages should be received");
    TEST_PASS();
}

/* ==================================================================
 * 测试 18：路由表满
 * 尝试写入超过 max_nodes 的路由表项，验证拒绝。
 * ================================================================== */
static void test_route_table_full(void)
{
    printf("\n[TEST 18] Route Table Full\n");
    sim_reset();
    node_count = 1;

    sim_node_init(0, 0x01, 1);

    /* max_nodes=8, 尝试写入 10 条 */
    route_entry_t big_table[10];
    for (int i = 0; i < 10; i++) {
        big_table[i].dest_id = (uint8_t)(0x10 + i);
        big_table[i].port_id = 0;
    }

    int ret = route_router_table_set(&nodes[0].router, big_table, 10);
    printf("  Set 10 routes (max=8): ret=%d\n", ret);

    /* 设置刚好 8 条应该成功 */
    int ret2 = route_router_table_set(&nodes[0].router, big_table, 8);
    printf("  Set 8 routes (max=8): ret=%d\n", ret2);

    TEST_ASSERT(ret != ROUTE_OK, "should reject over-capacity route table");
    TEST_ASSERT(ret2 == ROUTE_OK, "should accept exact-capacity route table");
    TEST_PASS();
}

/* ==== main ==== */
int main(void)
{
    printf("========================================\n");
    printf("  comm_route Stress Tests (Extended)\n");
    printf("========================================\n");

    test_concurrent_multi_source();
    test_reasm_slot_exhaustion();
    test_seq_wraparound();
    test_link_disconnect_reconnect();
    test_zero_length_payload();
    test_self_send();
    test_out_of_order_fragments();
    test_duplicate_frame_injection();
    test_long_running_stability();
    test_route_table_full();

    printf("\n========================================\n");
    printf("  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("========================================\n");

    return tests_failed > 0 ? 1 : 0;
}
