/**
 * 示例 1：仅使用 Router（纯转发/广播节点）
 *
 * 演示：初始化 router、注册 port、设置路由表、收发帧。
 */
#include <stdio.h>
#include <string.h>
#include "router/route_router.h"

/* ---- 存储分配 ---- */
static route_port_t ports[2];
static route_entry_t route_table[8];
static route_seen_entry_t seen_table[16];

#define BLOCK_SIZE  (ROUTE_HEADER_SIZE + 32)  /* header + frag_size */
#define QUEUE_SIZE  16

static uint8_t rq_data[QUEUE_SIZE * BLOCK_SIZE];
static uint16_t rq_lengths[QUEUE_SIZE];
static uint8_t rq_from_port[QUEUE_SIZE];

/* 帧缓冲区 */
static uint8_t send_frame_buf[BLOCK_SIZE];
static uint8_t fwd_frame_buf[BLOCK_SIZE];
static uint8_t rx_frame_buf[BLOCK_SIZE];

/* ---- port send 实现 ---- */
static int uart_send(uint8_t port_id, const uint8_t *buf, uint16_t len)
{
    (void)port_id;
    /* 硬件发送: hal_uart_write(buf, len); */
    printf("[UART TX] %u bytes\n", len);
    return ROUTE_OK;
}

/* ---- 收到帧回调 ---- */
static void on_frame_deliver(void *ctx, const route_header_t *hdr,
                             const uint8_t *payload, uint16_t payload_len)
{
    (void)ctx;
    printf("Received from node 0x%02X: %.*s\n", hdr->src, payload_len, payload);
}

/* ---- 全局上下文 ---- */
static route_router_ctx_t router;
static route_stats_t stats;

void app_init(void)
{
    route_router_config_t cfg = {
        .node_id        = 0x01,
        .default_ttl    = 8,
        .frag_size      = 32,
        .codec          = NULL,  /* 使用内置默认 codec */
        .max_ports      = 2,
        .max_nodes      = 8,
        .seen_table_size = 16,
        .seen_expire_ms = 10000,
        .recv_queue_size = QUEUE_SIZE,
        .ports          = ports,
        .route_table    = route_table,
        .seen_table     = seen_table,
        .recv_queue_data      = rq_data,
        .recv_queue_lengths   = rq_lengths,
        .recv_queue_from_port = rq_from_port,
        .send_frame_buf = send_frame_buf,
        .fwd_frame_buf  = fwd_frame_buf,
        .rx_frame_buf   = rx_frame_buf,
        .stats          = &stats,
        .queue_lock     = NULL,
        .queue_unlock   = NULL,
        .queue_lock_ctx = NULL,
    };
    route_router_init(&router, &cfg);

    /* 注册端口 */
    route_port_t uart_port = { .port_id = 0, .send = uart_send, .ctx = NULL };
    route_router_port_register(&router, &uart_port);

    /* 设置路由表 */
    route_entry_t entries[] = {
        { .dest_id = 0x02, .port_id = 0 },
        { .dest_id = 0x03, .port_id = 0 },
    };
    route_router_table_set(&router, entries, 2);

    /* 设置接收回调 */
    route_router_set_deliver_cb(&router, on_frame_deliver, NULL);
}

void app_loop(uint32_t now_ms)
{
    route_router_tick(&router, now_ms);
    route_router_poll(&router);
}

/* UART 中断接收 */
void uart_rx_isr(const uint8_t *data, uint16_t len)
{
    route_router_input(&router, data, len, 0);
}

/* 发送广播 */
void send_broadcast(const uint8_t *data, uint16_t len)
{
    route_header_t hdr = {
        .src        = 0x01,
        .dst        = ROUTE_BROADCAST_ADDR,
        .type       = ROUTE_TYPE_REQUEST,
        .trans_id   = 0,
        .seq        = 0,
        .ttl        = 8,
        .frag_idx   = 0,
        .frag_total = 1,
    };
    route_router_send(&router, &hdr, data, len);
}
