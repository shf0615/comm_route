/**
 * 示例 3：完整栈（通过 route_stack 串联 Router + Frag + Transaction）
 *
 * 演示：一行 wire 串联三层，同步/异步请求-响应。
 */
#include <stdio.h>
#include <string.h>
#include "route_stack.h"

/* ---- 配置参数 ---- */
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       9
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      (ROUTE_HEADER_SIZE + FRAG_SIZE)
#define QUEUE_SIZE      16
#define MAX_TRANS       8
#define MAX_PENDING     16

/* ---- Router 存储 ---- */
static route_port_t ports[2];
static route_entry_t route_table[8];
static route_seen_entry_t seen_table[16];
static uint8_t rq_data[QUEUE_SIZE * BLOCK_SIZE];
static uint16_t rq_lengths[QUEUE_SIZE];
static uint8_t rq_from_port[QUEUE_SIZE];
static uint8_t send_frame_buf[BLOCK_SIZE];
static uint8_t fwd_frame_buf[BLOCK_SIZE];
static uint8_t rx_frame_buf[BLOCK_SIZE];

/* ---- Frag 存储 ---- */
static route_reasm_ctx_t reasm_slots[REASM_SLOTS];
static uint8_t reasm_buf[MAX_PAYLOAD];
static uint8_t *reasm_frag_ptrs[REASM_SLOTS * MAX_FRAGS];
static uint16_t reasm_frag_lens[REASM_SLOTS * MAX_FRAGS];
static uint32_t pool_bitmap[(POOL_BLOCKS + 31) / 32];
static uint8_t pool_storage[POOL_BLOCKS * BLOCK_SIZE];
static route_pending_ack_t pending_acks[MAX_PENDING];
static uint8_t pending_ack_data[MAX_PENDING * FRAG_SIZE];

/* ---- Transaction 存储 ---- */
static transaction_t trans_table[MAX_TRANS];

/* ---- 公共 ---- */
static route_stats_t stats;
static route_router_ctx_t router;
static route_frag_ctx_t frag;
static route_transaction_ctx_t trans;
static route_stack_t stack;

/* ---- OS 抽象（裸机 stub） ---- */
static void *stub_mutex_create(void) { return (void *)1; }
static void stub_mutex_lock(void *m) { (void)m; }
static void stub_mutex_unlock(void *m) { (void)m; }
static void stub_mutex_destroy(void *m) { (void)m; }
static void *stub_sem_create(void) { return (void *)1; }
static int stub_sem_wait(void *s, uint32_t t) { (void)s; (void)t; return 0; }
static void stub_sem_post(void *s) { (void)s; }
static void stub_sem_destroy(void *s) { (void)s; }

static const route_os_t my_os = {
    .mutex_create  = stub_mutex_create,
    .mutex_lock    = stub_mutex_lock,
    .mutex_unlock  = stub_mutex_unlock,
    .mutex_destroy = stub_mutex_destroy,
    .sem_create    = stub_sem_create,
    .sem_wait      = stub_sem_wait,
    .sem_post      = stub_sem_post,
    .sem_destroy   = stub_sem_destroy,
};

/* ---- port send ---- */
static int uart0_send(uint8_t port_id, const uint8_t *buf, uint16_t len)
{
    (void)port_id;
    printf("[UART0 TX] %u bytes\n", len);
    return ROUTE_OK;
}

/* ---- 应用接收回调 ---- */
static void on_request(void *ctx, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len)
{
    (void)ctx;
    printf("Request from 0x%02X (trans=%u): %.*s\n", src, trans_id, len, data);
    /* 回复 */
    uint8_t resp[] = "OK";
    route_stack_reply(&stack, src, trans_id, resp, 2);
}

void app_init(void)
{
    /* Router */
    route_router_config_t rcfg = {
        .node_id         = 0x01,
        .default_ttl     = 8,
        .frag_size       = FRAG_SIZE,
        .codec           = NULL,
        .max_ports       = 2,
        .max_nodes       = 8,
        .seen_table_size = 16,
        .seen_expire_ms  = 10000,
        .recv_queue_size = QUEUE_SIZE,
        .ports           = ports,
        .route_table     = route_table,
        .seen_table      = seen_table,
        .recv_queue_data      = rq_data,
        .recv_queue_lengths   = rq_lengths,
        .recv_queue_from_port = rq_from_port,
        .send_frame_buf  = send_frame_buf,
        .fwd_frame_buf   = fwd_frame_buf,
        .rx_frame_buf    = rx_frame_buf,
        .stats           = &stats,
    };
    route_router_init(&router, &rcfg);

    route_port_t p0 = { .port_id = 0, .send = uart0_send, .ctx = NULL };
    route_router_port_register(&router, &p0);

    route_entry_t entries[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&router, entries, 1);

    /* Frag（含 reliability） */
    route_frag_config_t fcfg = {
        .node_id           = 0x01,
        .frag_size         = FRAG_SIZE,
        .max_payload       = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS,
        .default_ttl       = 8,
        .reasm = {
            .slots      = reasm_slots,
            .buf        = reasm_buf,
            .frag_ptrs  = reasm_frag_ptrs,
            .frag_lens  = reasm_frag_lens,
            .max_slots  = REASM_SLOTS,
            .timeout_ms = 5000,
        },
        .reliability = {
            .max_pending_acks = MAX_PENDING,
            .ack_timeout_ms   = 1000,
            .ack_retry_max    = 3,
            .pending_acks     = pending_acks,
            .pending_ack_data = pending_ack_data,
        },
        .pool_bitmap      = pool_bitmap,
        .pool_storage     = pool_storage,
        .pool_block_count = POOL_BLOCKS,
        .stats            = &stats,
    };
    route_frag_init(&frag, &fcfg);

    /* Transaction */
    route_transaction_config_t tcfg = {
        .max_concurrent_trans = MAX_TRANS,
        .default_timeout_ms  = 5000,
        .os                  = &my_os,
        .trans_table         = trans_table,
        .lower_send          = NULL,  /* 由 stack_wire 设置 */
        .lower_send_ctx      = NULL,
        .stats               = &stats,
    };
    route_transaction_init(&trans, &tcfg);

    /* 一行串联 */
    route_stack_wire(&stack, &router, &frag, &trans);
    route_stack_set_recv_cb(&stack, on_request, NULL);
}

void app_loop(uint32_t now_ms)
{
    route_stack_tick(&stack, now_ms);
    route_stack_poll(&stack);
}

/* UART 中断接收 */
void uart0_rx_isr(const uint8_t *data, uint16_t len)
{
    route_stack_input(&stack, data, len, 0);
}

/* 发送同步请求 */
int send_request_sync(uint8_t dest, const uint8_t *data, uint16_t len)
{
    uint8_t resp[64];
    uint16_t resp_len = sizeof(resp);
    int ret = route_stack_send_sync(&stack, dest, data, len, resp, &resp_len, 3000);
    if (ret == ROUTE_OK) {
        printf("Response: %.*s\n", resp_len, resp);
    }
    return ret;
}

/* 发送异步请求 */
static void on_response(int result, const uint8_t *data, uint16_t len, void *user_data)
{
    (void)user_data;
    if (result == ROUTE_OK) {
        printf("Async response: %.*s\n", len, data);
    } else {
        printf("Async request failed: %d\n", result);
    }
}

void send_request_async(uint8_t dest, const uint8_t *data, uint16_t len)
{
    route_stack_send_async(&stack, dest, data, len, on_response, NULL);
}
