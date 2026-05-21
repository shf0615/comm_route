#include "route_frag.h"
#include <string.h>

// ============ Lock helpers ============

static inline void frag_lock(route_frag_ctx_t *ctx) {
    if (ctx->lock) ctx->lock(ctx->lock_ctx);
}

static inline void frag_unlock(route_frag_ctx_t *ctx) {
    if (ctx->unlock) ctx->unlock(ctx->lock_ctx);
}

// ============ Init ============

int route_frag_init(route_frag_ctx_t *ctx, const route_frag_config_t *cfg) {
    if (ctx == NULL || cfg == NULL) return ROUTE_ERR_PARAM;
    if (cfg->frag_size == 0 || cfg->max_payload == 0) return ROUTE_ERR_PARAM;
    if (cfg->max_frags_per_msg == 0 || cfg->reasm.max_slots == 0) return ROUTE_ERR_PARAM;
    if (cfg->reasm.slots == NULL || cfg->reasm.buf == NULL) return ROUTE_ERR_PARAM;
    if (cfg->reasm.frag_ptrs == NULL || cfg->reasm.frag_lens == NULL) return ROUTE_ERR_PARAM;
    if (cfg->pool_bitmap == NULL || cfg->pool_storage == NULL) return ROUTE_ERR_PARAM;

    memset(ctx, 0, sizeof(*ctx));
    ctx->node_id = cfg->node_id;
    ctx->cfg_frag_size = cfg->frag_size;
    ctx->cfg_max_payload = cfg->max_payload;
    ctx->cfg_max_frags_per_msg = cfg->max_frags_per_msg;
    ctx->cfg_max_reasm_slots = cfg->reasm.max_slots;
    ctx->cfg_reasm_timeout_ms = cfg->reasm.timeout_ms;
    ctx->cfg_default_ttl = cfg->default_ttl;
    ctx->reasm_slots = cfg->reasm.slots;
    ctx->reasm_buf = cfg->reasm.buf;
    ctx->stats = cfg->stats;

    // Init reassembly slots
    for (uint8_t i = 0; i < cfg->reasm.max_slots; i++) {
        ctx->reasm_slots[i].active = 0;
        ctx->reasm_slots[i].fragments = &cfg->reasm.frag_ptrs[i * cfg->max_frags_per_msg];
        ctx->reasm_slots[i].frag_lens = &cfg->reasm.frag_lens[i * cfg->max_frags_per_msg];
    }

    // Init pool
    uint16_t block_size = ROUTE_HEADER_SIZE + cfg->frag_size;
    route_pool_init(&ctx->pool, cfg->pool_storage, cfg->pool_bitmap,
                    cfg->pool_block_count, block_size);

    // Reliability (optional)
    const route_reliability_config_t *rel = &cfg->reliability;
    if (rel->max_pending_acks > 0 && rel->pending_acks != NULL && rel->pending_ack_data != NULL) {
        ctx->reliability_enabled = 1;
        ctx->pending_acks = rel->pending_acks;
        ctx->cfg_max_pending_acks = rel->max_pending_acks;
        ctx->cfg_ack_timeout_ms = rel->ack_timeout_ms;
        ctx->cfg_ack_retry_max = rel->ack_retry_max;
        for (uint8_t i = 0; i < rel->max_pending_acks; i++) {
            ctx->pending_acks[i].active = 0;
            ctx->pending_acks[i].data = &rel->pending_ack_data[i * cfg->frag_size];
        }
    }

    // Optional lock
    ctx->lock = cfg->lock;
    ctx->unlock = cfg->unlock;
    ctx->lock_ctx = cfg->lock_ctx;

    return ROUTE_OK;
}

void route_frag_deinit(route_frag_ctx_t *ctx) {
    if (ctx == NULL) return;
    // Free pool blocks held by active reasm slots
    for (uint8_t i = 0; i < ctx->cfg_max_reasm_slots; i++) {
        if (ctx->reasm_slots[i].active) {
            for (uint8_t j = 0; j < ctx->reasm_slots[i].frag_total; j++) {
                if (ctx->reasm_slots[i].fragments[j] != NULL) {
                    route_pool_free(&ctx->pool, ctx->reasm_slots[i].fragments[j]);
                    ctx->reasm_slots[i].fragments[j] = NULL;
                }
            }
            ctx->reasm_slots[i].active = 0;
        }
    }
}

// ============ Callbacks ============

void route_frag_set_lower_send(route_frag_ctx_t *ctx,
    int (*send)(void *c, const route_header_t *hdr, const uint8_t *payload, uint16_t len),
    void *send_ctx) {
    ctx->lower_send = send;
    ctx->lower_send_ctx = send_ctx;
}

void route_frag_set_complete_cb(route_frag_ctx_t *ctx,
    void (*cb)(void *c, const route_header_t *hdr, const uint8_t *data, uint16_t len),
    void *cb_ctx) {
    ctx->complete_cb = cb;
    ctx->complete_ctx = cb_ctx;
}

// ============ Reliability Internal ============

static int reliability_register(route_frag_ctx_t *ctx, uint8_t dest, uint8_t seq,
                                uint8_t frag_idx, uint8_t frag_total, uint8_t type,
                                uint8_t trans_id, const uint8_t *data, uint16_t len) {
    if (!ctx->reliability_enabled) return ROUTE_OK;
    if (len > ctx->cfg_frag_size) return ROUTE_ERR_PARAM;
    for (uint8_t i = 0; i < ctx->cfg_max_pending_acks; i++) {
        if (!ctx->pending_acks[i].active) {
            route_pending_ack_t *pa = &ctx->pending_acks[i];
            pa->active = 1;
            pa->dest_id = dest;
            pa->seq = seq;
            pa->frag_idx = frag_idx;
            pa->frag_total = frag_total;
            pa->trans_id = trans_id;
            pa->type = type;
            pa->retry_count = 0;
            pa->next_retry_ms = 0;
            if (data && len > 0) memcpy(pa->data, data, len);
            pa->len = len;
            return ROUTE_OK;
        }
    }
    return ROUTE_ERR_FULL;
}

static void reliability_on_ack(route_frag_ctx_t *ctx, uint8_t src, uint8_t seq, uint8_t frag_idx) {
    if (!ctx->reliability_enabled) return;
    for (uint8_t i = 0; i < ctx->cfg_max_pending_acks; i++) {
        if (ctx->pending_acks[i].active &&
            ctx->pending_acks[i].dest_id == src &&
            ctx->pending_acks[i].seq == seq &&
            ctx->pending_acks[i].frag_idx == frag_idx) {
            ctx->pending_acks[i].active = 0;
            return;
        }
    }
}

static int reliability_send_ack(route_frag_ctx_t *ctx, uint8_t dest, uint8_t seq, uint8_t frag_idx) {
    if (ctx->lower_send == NULL) return ROUTE_ERR_PARAM;
    route_header_t hdr = {
        .src = ctx->node_id,
        .dst = dest,
        .type = ROUTE_TYPE_ACK,
        .trans_id = 0,
        .seq = seq,
        .ttl = ctx->cfg_default_ttl,
        .frag_idx = frag_idx,
        .frag_total = 1,
    };
    return ctx->lower_send(ctx->lower_send_ctx, &hdr, NULL, 0);
}

static void reliability_tick(route_frag_ctx_t *ctx, uint32_t now_ms) {
    if (!ctx->reliability_enabled) return;
    for (uint8_t i = 0; i < ctx->cfg_max_pending_acks; i++) {
        if (!ctx->pending_acks[i].active) continue;
        route_pending_ack_t *pa = &ctx->pending_acks[i];
        if (pa->next_retry_ms == 0) {
            pa->next_retry_ms = now_ms + ctx->cfg_ack_timeout_ms;
            continue;
        }
        if ((int32_t)(now_ms - pa->next_retry_ms) < 0) continue;
        if (pa->retry_count >= ctx->cfg_ack_retry_max) {
            pa->active = 0;
            continue;
        }
        // Retransmit
        if (ctx->lower_send) {
            route_header_t hdr = {
                .src = ctx->node_id,
                .dst = pa->dest_id,
                .type = pa->type,
                .trans_id = pa->trans_id,
                .seq = pa->seq,
                .ttl = ctx->cfg_default_ttl,
                .frag_idx = pa->frag_idx,
                .frag_total = pa->frag_total,
            };
            ctx->lower_send(ctx->lower_send_ctx, &hdr, pa->data, pa->len);
            if (ctx->stats) ctx->stats->retransmissions++;
        }
        pa->retry_count++;
        uint8_t shift = pa->retry_count < 3 ? pa->retry_count : 3;
        pa->next_retry_ms = now_ms + ctx->cfg_ack_timeout_ms * (1u << shift);
    }
}

// ============ Reassembly Internal ============

static route_reasm_ctx_t *find_reasm_slot(route_frag_ctx_t *ctx, uint8_t src_id, uint8_t seq) {
    for (uint8_t i = 0; i < ctx->cfg_max_reasm_slots; i++) {
        if (ctx->reasm_slots[i].active &&
            ctx->reasm_slots[i].src_id == src_id &&
            ctx->reasm_slots[i].seq == seq) {
            return &ctx->reasm_slots[i];
        }
    }
    return NULL;
}

static route_reasm_ctx_t *alloc_reasm_slot(route_frag_ctx_t *ctx) {
    for (uint8_t i = 0; i < ctx->cfg_max_reasm_slots; i++) {
        if (!ctx->reasm_slots[i].active) return &ctx->reasm_slots[i];
    }
    return NULL;
}

static void free_reasm_slot(route_frag_ctx_t *ctx, route_reasm_ctx_t *slot) {
    for (uint8_t i = 0; i < slot->frag_total; i++) {
        if (slot->fragments[i] != NULL) {
            route_pool_free(&ctx->pool, slot->fragments[i]);
            slot->fragments[i] = NULL;
        }
    }
    slot->active = 0;
}

static int frag_reassemble(route_frag_ctx_t *ctx, const route_header_t *hdr,
                           const uint8_t *payload, uint16_t payload_len,
                           uint8_t *out_buf, uint16_t *out_len, route_header_t *out_hdr) {
    if (hdr->frag_total == 0 || hdr->frag_total > ctx->cfg_max_frags_per_msg) return ROUTE_ERR_PARAM;

    // 单帧：直接返回
    if (hdr->frag_total == 1) {
        if (payload_len > ctx->cfg_max_payload) return ROUTE_ERR_PARAM;
        if (payload_len > 0) {
            if (payload == NULL) return ROUTE_ERR_PARAM;
            memcpy(out_buf, payload, payload_len);
        }
        *out_len = payload_len;
        *out_hdr = *hdr;
        return 1;
    }

    route_reasm_ctx_t *slot = find_reasm_slot(ctx, hdr->src, hdr->seq);
    if (slot == NULL) {
        slot = alloc_reasm_slot(ctx);
        if (slot == NULL) {
            if (ctx->stats) ctx->stats->drop_no_mem++;
            return ROUTE_ERR_FULL;
        }
        slot->active = 1;
        slot->src_id = hdr->src;
        slot->seq = hdr->seq;
        slot->frag_total = hdr->frag_total;
        slot->received_count = 0;
        slot->start_ms = ctx->current_ms;
        memset(slot->fragments, 0, ctx->cfg_max_frags_per_msg * sizeof(uint8_t *));
        memset(slot->frag_lens, 0, ctx->cfg_max_frags_per_msg * sizeof(uint16_t));
    }

    if (hdr->frag_idx >= slot->frag_total) return ROUTE_ERR_PARAM;
    if (payload_len > ctx->cfg_frag_size) return ROUTE_ERR_PARAM;
    if (slot->fragments[hdr->frag_idx] != NULL) return 0;  // duplicate fragment

    uint8_t *blk = route_pool_alloc(&ctx->pool);
    if (blk == NULL) {
        free_reasm_slot(ctx, slot);
        if (ctx->stats) ctx->stats->drop_no_mem++;
        return ROUTE_ERR_NO_MEM;
    }
    memcpy(blk, payload, payload_len);
    slot->fragments[hdr->frag_idx] = blk;
    slot->frag_lens[hdr->frag_idx] = payload_len;
    slot->received_count++;

    if (slot->received_count == slot->frag_total) {
        *out_len = 0;
        for (uint8_t i = 0; i < slot->frag_total; i++) {
            if (*out_len + slot->frag_lens[i] > ctx->cfg_max_payload) {
                free_reasm_slot(ctx, slot);
                return ROUTE_ERR_REASM;
            }
            memcpy(out_buf + *out_len, slot->fragments[i], slot->frag_lens[i]);
            *out_len += slot->frag_lens[i];
        }
        *out_hdr = *hdr;
        out_hdr->frag_idx = 0;
        out_hdr->frag_total = 1;
        free_reasm_slot(ctx, slot);
        return 1;
    }
    return 0;
}

// ============ Public API ============

int route_frag_send(route_frag_ctx_t *ctx, uint8_t dest, uint8_t trans_id,
                    uint8_t seq, route_frame_type_t type,
                    const uint8_t *data, uint16_t len) {
    if (ctx->lower_send == NULL) return ROUTE_ERR_PARAM;

    uint8_t frag_total = (len + ctx->cfg_frag_size - 1) / ctx->cfg_frag_size;
    if (frag_total == 0) frag_total = 1;

    for (uint8_t i = 0; i < frag_total; i++) {
        uint16_t offset = i * ctx->cfg_frag_size;
        uint16_t chunk = len - offset;
        if (chunk > ctx->cfg_frag_size) chunk = ctx->cfg_frag_size;

        route_header_t hdr = {
            .src = ctx->node_id,
            .dst = dest,
            .type = type,
            .trans_id = trans_id,
            .seq = seq,
            .ttl = ctx->cfg_default_ttl,
            .frag_idx = i,
            .frag_total = frag_total,
        };

        const uint8_t *chunk_ptr = (data != NULL && chunk > 0) ? &data[offset] : NULL;

        int rc = ctx->lower_send(ctx->lower_send_ctx, &hdr, chunk_ptr, chunk);
        if (rc != ROUTE_OK) return rc;

        // 注册 reliability 跟踪（需要锁保护 pending_acks）
        frag_lock(ctx);
        int rel_rc = reliability_register(ctx, dest, seq, i, frag_total, type, trans_id, chunk_ptr, chunk);
        frag_unlock(ctx);
        if (rel_rc == ROUTE_ERR_FULL && ctx->stats) {
            ctx->stats->drop_no_mem++;
        }
    }
    return ROUTE_OK;
}

void route_frag_input(route_frag_ctx_t *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len) {
    // ACK 帧 → 清除对应 pending_ack
    if (hdr->type == ROUTE_TYPE_ACK) {
        frag_lock(ctx);
        reliability_on_ack(ctx, hdr->src, hdr->seq, hdr->frag_idx);
        frag_unlock(ctx);
        return;
    }

    // 数据帧 → 逐片发 ACK
    if (ctx->reliability_enabled) {
        reliability_send_ack(ctx, hdr->src, hdr->seq, hdr->frag_idx);
    }

    // 重组
    frag_lock(ctx);
    uint16_t reasm_len = 0;
    route_header_t reasm_hdr;
    int rc = frag_reassemble(ctx, hdr, payload, payload_len,
                             ctx->reasm_buf, &reasm_len, &reasm_hdr);
    frag_unlock(ctx);

    if (rc == 1 && ctx->complete_cb) {
        ctx->complete_cb(ctx->complete_ctx, &reasm_hdr, ctx->reasm_buf, reasm_len);
    }
}

void route_frag_tick(route_frag_ctx_t *ctx, uint32_t now_ms) {
    ctx->current_ms = now_ms;

    frag_lock(ctx);
    // Reassembly timeout
    for (uint8_t i = 0; i < ctx->cfg_max_reasm_slots; i++) {
        if (ctx->reasm_slots[i].active) {
            if ((int32_t)(now_ms - ctx->reasm_slots[i].start_ms) >= (int32_t)ctx->cfg_reasm_timeout_ms) {
                free_reasm_slot(ctx, &ctx->reasm_slots[i]);
                if (ctx->stats) ctx->stats->reasm_timeouts++;
            }
        }
    }
    // Reliability retransmit timeout
    reliability_tick(ctx, now_ms);
    frag_unlock(ctx);
}
