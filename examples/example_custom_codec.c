/**
 * 示例 4：自定义 Codec（替换帧格式）
 *
 * 演示：4 字节紧凑 header + payload + 1 字节 XOR checksum。
 */
#include <stdio.h>
#include <string.h>
#include "router/route_router.h"

/* ---- 自定义编解码 ---- */
static uint16_t my_encode(const route_header_t *hdr, const uint8_t *payload,
                          uint16_t payload_len, uint8_t *frame)
{
    frame[0] = (hdr->src << 4) | (hdr->dst & 0x0F);
    frame[1] = (hdr->type << 6) | (hdr->ttl & 0x3F);
    frame[2] = hdr->seq;
    frame[3] = (hdr->frag_idx << 4) | (hdr->frag_total & 0x0F);
    if (payload_len > 0) {
        memcpy(&frame[4], payload, payload_len);
    }
    /* XOR checksum */
    uint8_t cksum = 0;
    for (uint16_t i = 0; i < 4 + payload_len; i++) {
        cksum ^= frame[i];
    }
    frame[4 + payload_len] = cksum;
    return (uint16_t)(5 + payload_len);
}

static int my_decode(const uint8_t *frame, uint16_t frame_len,
                     route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len)
{
    if (frame_len < 5) return ROUTE_ERR_PARAM;
    /* 校验 checksum */
    uint8_t cksum = 0;
    for (uint16_t i = 0; i < frame_len - 1; i++) {
        cksum ^= frame[i];
    }
    if (cksum != frame[frame_len - 1]) return ROUTE_ERR_CRC;

    hdr->src        = frame[0] >> 4;
    hdr->dst        = frame[0] & 0x0F;
    hdr->type       = frame[1] >> 6;
    hdr->ttl        = frame[1] & 0x3F;
    hdr->seq        = frame[2];
    hdr->trans_id   = 0;
    hdr->frag_idx   = frame[3] >> 4;
    hdr->frag_total = frame[3] & 0x0F;
    *payload_len    = frame_len - 5;
    *payload        = (*payload_len > 0) ? &frame[4] : NULL;
    return ROUTE_OK;
}

static const route_codec_t my_codec = {
    .encode   = my_encode,
    .decode   = my_decode,
    .overhead = 5,  /* 4 header + 1 checksum */
};

/* ---- 使用 ---- */
static route_router_ctx_t router;
static route_stats_t stats;
static route_port_t ports[1];
static route_entry_t route_table[4];
static route_seen_entry_t seen_table[8];

#define FRAG_SIZE   32
#define BLOCK_SIZE  (5 + FRAG_SIZE)  /* 使用自定义 overhead */
#define QUEUE_SIZE  8

static uint8_t rq_data[QUEUE_SIZE * BLOCK_SIZE];
static uint16_t rq_lengths[QUEUE_SIZE];
static uint8_t rq_from_port[QUEUE_SIZE];
static uint8_t send_frame_buf[BLOCK_SIZE];
static uint8_t fwd_frame_buf[BLOCK_SIZE];
static uint8_t rx_frame_buf[BLOCK_SIZE];

static int my_port_send(uint8_t port_id, const uint8_t *buf, uint16_t len)
{
    (void)port_id;
    printf("[Custom TX] %u bytes\n", len);
    return ROUTE_OK;
}

void app_init(void)
{
    route_router_config_t cfg = {
        .node_id         = 0x01,
        .default_ttl     = 8,
        .frag_size       = FRAG_SIZE,
        .codec           = &my_codec,  /* ← 替换默认 codec */
        .max_ports       = 1,
        .max_nodes       = 4,
        .seen_table_size = 8,
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
    route_router_init(&router, &cfg);

    route_port_t p0 = { .port_id = 0, .send = my_port_send, .ctx = NULL };
    route_router_port_register(&router, &p0);

    route_entry_t entries[] = { { .dest_id = 0x02, .port_id = 0 } };
    route_router_table_set(&router, entries, 1);
}
