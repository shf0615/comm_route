#include "route_router.h"
#include "../common/route_crc.h"
#include <string.h>

// ============ 内置默认 Codec ============

static uint16_t default_encode(const route_header_t *hdr, const uint8_t *payload,
                               uint16_t payload_len, uint8_t *frame) {
    frame[0] = hdr->src;
    frame[1] = hdr->dst;
    frame[2] = (uint8_t)hdr->type;
    frame[3] = hdr->trans_id;
    frame[4] = hdr->seq;
    frame[5] = hdr->ttl;
    frame[6] = hdr->frag_idx;
    frame[7] = hdr->frag_total;
    if (payload_len > 0 && payload != NULL) {
        memcpy(&frame[8], payload, payload_len);
    }
    uint16_t crc = route_crc16(frame, 8 + payload_len);
    frame[8 + payload_len] = (crc >> 8) & 0xFF;
    frame[9 + payload_len] = crc & 0xFF;
    return 10 + payload_len;
}

static int default_decode(const uint8_t *frame, uint16_t frame_len,
                          route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len) {
    if (frame_len < ROUTE_HEADER_SIZE) return ROUTE_ERR_PARAM;
    uint16_t plen = frame_len - ROUTE_HEADER_SIZE;
    uint16_t crc_calc = route_crc16(frame, frame_len - 2);
    uint16_t crc_recv = ((uint16_t)frame[frame_len - 2] << 8) | frame[frame_len - 1];
    if (crc_calc != crc_recv) return ROUTE_ERR_PARAM;
    hdr->src = frame[0];
    hdr->dst = frame[1];
    hdr->type = frame[2];
    hdr->trans_id = frame[3];
    hdr->seq = frame[4];
    hdr->ttl = frame[5];
    hdr->frag_idx = frame[6];
    hdr->frag_total = frame[7];
    *payload = (plen > 0) ? &frame[8] : NULL;
    *payload_len = plen;
    return ROUTE_OK;
}

static const route_codec_t default_codec = {
    .encode = default_encode,
    .decode = default_decode,
    .overhead = ROUTE_HEADER_SIZE,
};

// ============ Init / Deinit ============

int route_router_init(route_router_ctx_t *ctx, const route_router_config_t *cfg) {
    if (ctx == NULL || cfg == NULL) return ROUTE_ERR_PARAM;
    if (cfg->max_ports == 0 || cfg->ports == NULL) return ROUTE_ERR_PARAM;
    if (cfg->max_nodes == 0 || cfg->route_table == NULL) return ROUTE_ERR_PARAM;
    if (cfg->seen_table_size == 0 || cfg->seen_table == NULL) return ROUTE_ERR_PARAM;
    if (cfg->recv_queue_size == 0) return ROUTE_ERR_PARAM;
    if (cfg->recv_queue_data == NULL || cfg->recv_queue_lengths == NULL || cfg->recv_queue_from_port == NULL) return ROUTE_ERR_PARAM;
    if (cfg->frag_size == 0) return ROUTE_ERR_PARAM;
    if (cfg->tx_frame_buf == NULL || cfg->rx_frame_buf == NULL) return ROUTE_ERR_PARAM;

    memset(ctx, 0, sizeof(*ctx));
    ctx->node_id = cfg->node_id;
    ctx->cfg_default_ttl = cfg->default_ttl;
    ctx->cfg_frag_size = cfg->frag_size;
    ctx->codec = cfg->codec ? cfg->codec : &default_codec;
    ctx->cfg_block_size = ctx->codec->overhead + cfg->frag_size;

    ctx->ports = cfg->ports;
    ctx->cfg_max_ports = cfg->max_ports;
    ctx->route_table = cfg->route_table;
    ctx->cfg_max_nodes = cfg->max_nodes;
    ctx->seen_table = cfg->seen_table;
    ctx->cfg_seen_table_size = cfg->seen_table_size;
    ctx->cfg_seen_expire_ms = cfg->seen_expire_ms;
    ctx->stats = cfg->stats;
    ctx->tx_frame_buf = cfg->tx_frame_buf;
    ctx->rx_frame_buf = cfg->rx_frame_buf;

    route_queue_init(&ctx->recv_queue, cfg->recv_queue_data, cfg->recv_queue_lengths,
                     cfg->recv_queue_from_port, cfg->recv_queue_size, ctx->cfg_block_size);

    return ROUTE_OK;
}

void route_router_deinit(route_router_ctx_t *ctx) {
    (void)ctx;
}

// ============ Configuration ============

int route_router_port_register(route_router_ctx_t *ctx, const route_port_t *port) {
    if (ctx->port_count >= ctx->cfg_max_ports) return ROUTE_ERR_FULL;
    ctx->ports[ctx->port_count] = *port;
    ctx->port_count++;
    return ROUTE_OK;
}

int route_router_table_set(route_router_ctx_t *ctx, const route_entry_t *entries, uint8_t count) {
    if (count > ctx->cfg_max_nodes) return ROUTE_ERR_PARAM;
    memcpy(ctx->route_table, entries, count * sizeof(route_entry_t));
    ctx->route_count = count;
    return ROUTE_OK;
}

void route_router_set_deliver_cb(route_router_ctx_t *ctx,
    void (*cb)(void *ctx, const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len),
    void *cb_ctx) {
    ctx->deliver_cb = cb;
    ctx->deliver_ctx = cb_ctx;
}

// ============ Internal ============

static const route_entry_t *router_lookup(route_router_ctx_t *ctx, uint8_t dest_id) {
    for (uint8_t i = 0; i < ctx->route_count; i++) {
        if (ctx->route_table[i].dest_id == dest_id) {
            return &ctx->route_table[i];
        }
    }
    return NULL;
}

static int router_send_to_port(route_router_ctx_t *ctx, uint8_t port_id,
                               const uint8_t *frame, uint16_t frame_len) {
    for (uint8_t i = 0; i < ctx->port_count; i++) {
        if (ctx->ports[i].port_id == port_id) {
            return ctx->ports[i].send(port_id, frame, frame_len);
        }
    }
    return ROUTE_ERR_NO_PORT;
}

// 转发去重：仅记录需要转发的广播帧（防止广播风暴）
static int router_seen_check_and_add(route_router_ctx_t *ctx, uint8_t src_id, uint8_t seq) {
    uint32_t now = ctx->current_ms;
    for (uint8_t i = 0; i < ctx->cfg_seen_table_size; i++) {
        if (!ctx->seen_table[i].valid) continue;
        if ((int32_t)(now - ctx->seen_table[i].timestamp_ms) >= (int32_t)ctx->cfg_seen_expire_ms) {
            ctx->seen_table[i].valid = 0;
            continue;
        }
        if (ctx->seen_table[i].src_id == src_id && ctx->seen_table[i].seq == seq) {
            return 1;  // duplicate
        }
    }
    ctx->seen_table[ctx->seen_index].src_id = src_id;
    ctx->seen_table[ctx->seen_index].seq = seq;
    ctx->seen_table[ctx->seen_index].valid = 1;
    ctx->seen_table[ctx->seen_index].timestamp_ms = now;
    ctx->seen_index = (ctx->seen_index + 1) % ctx->cfg_seen_table_size;
    return 0;
}

static int router_handle_frame(route_router_ctx_t *ctx, const uint8_t *frame,
                               uint16_t frame_len, uint8_t from_port) {
    route_header_t hdr;
    const uint8_t *payload;
    uint16_t payload_len;

    int rc = ctx->codec->decode(frame, frame_len, &hdr, &payload, &payload_len);
    if (rc != ROUTE_OK) {
        if (ctx->stats) ctx->stats->crc_errors++;
        return rc;
    }

    if (hdr.ttl == 0) {
        if (ctx->stats) ctx->stats->drop_ttl++;
        return ROUTE_ERR_TIMEOUT;
    }

    // 广播帧
    if (hdr.dst == ROUTE_BROADCAST_ADDR) {
        // 转发去重
        if (router_seen_check_and_add(ctx, hdr.src, hdr.seq)) {
            if (ctx->stats) ctx->stats->drop_duplicate++;
            return 0;
        }
        // 转发到其他端口
        hdr.ttl--;
        uint16_t fwd_len = ctx->codec->encode(&hdr, payload, payload_len, ctx->tx_frame_buf);
        for (uint8_t i = 0; i < ctx->port_count; i++) {
            if (ctx->ports[i].port_id != from_port) {
                if (ctx->ports[i].send(ctx->ports[i].port_id, ctx->tx_frame_buf, fwd_len) == ROUTE_OK) {
                    if (ctx->stats) { ctx->stats->tx_packets++; ctx->stats->tx_bytes += fwd_len; }
                }
            }
        }
        // 送达本机
        hdr.ttl++;
        if (ctx->stats) { ctx->stats->rx_packets++; ctx->stats->rx_bytes += ROUTE_HEADER_SIZE + payload_len; }
        if (ctx->deliver_cb) {
            ctx->deliver_cb(ctx->deliver_ctx, &hdr, payload, payload_len);
        }
        return 1;
    }

    // 送达本机
    if (hdr.dst == ctx->node_id) {
        if (ctx->stats) { ctx->stats->rx_packets++; ctx->stats->rx_bytes += ROUTE_HEADER_SIZE + payload_len; }
        if (ctx->deliver_cb) {
            ctx->deliver_cb(ctx->deliver_ctx, &hdr, payload, payload_len);
        }
        return 1;
    }

    // 转发
    const route_entry_t *entry = router_lookup(ctx, hdr.dst);
    if (entry == NULL) {
        if (ctx->stats) ctx->stats->drop_no_route++;
        return ROUTE_ERR_NO_ROUTE;
    }
    hdr.ttl--;
    uint16_t fwd_len = ctx->codec->encode(&hdr, payload, payload_len, ctx->tx_frame_buf);
    int fwd_rc = router_send_to_port(ctx, entry->port_id, ctx->tx_frame_buf, fwd_len);
    if (fwd_rc == ROUTE_OK && ctx->stats) {
        ctx->stats->tx_packets++;
        ctx->stats->tx_bytes += fwd_len;
    }
    return fwd_rc;
}

// ============ Public API ============

int route_router_send(route_router_ctx_t *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len) {
    uint16_t frame_len = ctx->codec->encode(hdr, payload, payload_len, ctx->tx_frame_buf);

    if (hdr->dst == ROUTE_BROADCAST_ADDR) {
        int last_err = ROUTE_OK;
        for (uint8_t i = 0; i < ctx->port_count; i++) {
            int rc = ctx->ports[i].send(ctx->ports[i].port_id, ctx->tx_frame_buf, frame_len);
            if (rc != ROUTE_OK) last_err = rc;
        }
        return last_err;
    }

    const route_entry_t *entry = router_lookup(ctx, hdr->dst);
    if (entry == NULL) {
        if (ctx->stats) ctx->stats->drop_no_route++;
        return ROUTE_ERR_NO_ROUTE;
    }
    return router_send_to_port(ctx, entry->port_id, ctx->tx_frame_buf, frame_len);
}

int route_router_input(route_router_ctx_t *ctx, const uint8_t *data, uint16_t len, uint8_t port_id) {
    int rc = route_queue_push(&ctx->recv_queue, data, len, port_id);
    if (rc != 0 && ctx->stats) {
        ctx->stats->drop_queue_full++;
    }
    return rc == 0 ? ROUTE_OK : ROUTE_ERR_FULL;
}

void route_router_poll(route_router_ctx_t *ctx) {
    uint16_t frame_len;
    uint8_t from_port;

    while (route_queue_pop(&ctx->recv_queue, ctx->rx_frame_buf, &frame_len, &from_port) == 0) {
        router_handle_frame(ctx, ctx->rx_frame_buf, frame_len, from_port);
    }
}

void route_router_tick(route_router_ctx_t *ctx, uint32_t now_ms) {
    ctx->current_ms = now_ms;
}
