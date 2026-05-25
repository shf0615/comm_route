/**
 * 示例 2：Router + Frag（大消息 fire-and-forget）
 *
 * 演示：Router 负责转发，Frag 负责分片/重组，无可靠性。
 */
#include <stdio.h>
#include <string.h>
#include "router/route_router.h"
#include "frag/route_frag.h"

/* ---- 配置参数 ---- */
#define FRAG_SIZE       32
#define MAX_PAYLOAD     256
#define MAX_FRAGS       ((MAX_PAYLOAD / FRAG_SIZE) + 1)  /* 9 */
#define REASM_SLOTS     4
#define POOL_BLOCKS     16
#define BLOCK_SIZE      (ROUTE_HEADER_SIZE + FRAG_SIZE)  /* 42 */
#define QUEUE_SIZE      16

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

static route_stats_t stats;
static route_router_ctx_t router;
static route_frag_ctx_t frag;

/* ---- port send ---- */
static int uart_send(uint8_t port_id, const uint8_t *buf, uint16_t len)
{
    (void)port_id;
    printf("[UART TX] %u bytes\n", len);
    return ROUTE_OK;
}

/* ---- 收到完整消息回调 ---- */
static void on_message(void *ctx, const route_header_t *hdr,
                       const uint8_t *data, uint16_t len)
{
    (void)ctx;
    printf("Complete message from 0x%02X, len=%u\n", hdr->src, len);
}

/* ---- Router → Frag 适配 ---- */
static void router_to_frag(void *ctx, const route_header_t *hdr,
                           const uint8_t *payload, uint16_t payload_len)
{
    (void)ctx;
    route_frag_input(&frag, hdr, payload, payload_len);
}

/* ---- Frag → Router 适配 ---- */
static int frag_to_router(void *ctx, const route_header_t *hdr,
                          const uint8_t *payload, uint16_t len)
{
    (void)ctx;
    return route_router_send(&router, hdr, payload, len);
}

void app_init(void)
{
    /* 初始化 Router */
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

    route_port_t p0 = { .port_id = 0, .send = uart_send, .ctx = NULL };
    route_router_port_register(&router, &p0);

    route_entry_t entries[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
    };
    route_router_table_set(&router, entries, 2);

    /* 初始化 Frag（无 reliability） */
    route_frag_config_t fcfg = {
        .node_id          = 0x01,
        .frag_size        = FRAG_SIZE,
        .max_payload      = MAX_PAYLOAD,
        .max_frags_per_msg = MAX_FRAGS,
        .default_ttl      = 8,
        .reasm = {
            .slots        = reasm_slots,
            .buf          = reasm_buf,
            .frag_ptrs    = reasm_frag_ptrs,
            .frag_lens    = reasm_frag_lens,
            .max_slots    = REASM_SLOTS,
            .timeout_ms   = 5000,
        },
        .reliability = {
            .max_pending_acks = 0,  /* 禁用 reliability */
            .pending_acks     = NULL,
            .pending_ack_data = NULL,
        },
        .pool_bitmap      = pool_bitmap,
        .pool_storage     = pool_storage,
        .pool_block_count = POOL_BLOCKS,
        .stats            = &stats,
    };
    route_frag_init(&frag, &fcfg);

    /* 手动连接层间回调 */
    route_router_set_deliver_cb(&router, router_to_frag, NULL);
    route_frag_set_lower_send(&frag, frag_to_router, NULL);
    route_frag_set_complete_cb(&frag, on_message, NULL);
}

void app_loop(uint32_t now_ms)
{
    route_router_tick(&router, now_ms);
    route_frag_tick(&frag, now_ms);
    route_router_poll(&router);
}

/* UART 中断接收 */
void uart_rx_isr(const uint8_t *data, uint16_t len)
{
    route_router_input(&router, data, len, 0);
}

/* 发送大消息（自动分片） */
void send_large_message(uint8_t dest, const uint8_t *data, uint16_t len)
{
    static uint8_t seq = 0;
    route_frag_send(&frag, dest, 0, seq++, ROUTE_TYPE_REQUEST, data, len);
}
