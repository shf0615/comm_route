#include "route_frag.h"
#include "../common/route_pool.h"
#include <string.h>

void route_frag_set_lower_send(route_instance_t *inst, route_lower_send_t send_fn) {
    inst->frag_lower_send = send_fn;
}

void route_frag_set_complete_cb(route_instance_t *inst, route_frag_complete_cb_t cb) {
    inst->frag_complete_cb = cb;
}

int route_frag_send_typed(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                          uint8_t seq, route_frame_type_t type,
                          const uint8_t *data, uint16_t len) {
    if (inst->frag_lower_send == NULL) return ROUTE_ERR_PARAM;

    uint8_t frag_total = (len + ROUTE_FRAG_SIZE - 1) / ROUTE_FRAG_SIZE;
    if (frag_total == 0) frag_total = 1;

    for (uint8_t i = 0; i < frag_total; i++) {
        uint16_t offset = i * ROUTE_FRAG_SIZE;
        uint16_t chunk = len - offset;
        if (chunk > ROUTE_FRAG_SIZE) chunk = ROUTE_FRAG_SIZE;

        route_header_t hdr = {
            .src = inst->node_id,
            .dst = dest,
            .type = (uint8_t)type,
            .trans_id = trans_id,
            .seq = seq,
            .ttl = ROUTE_DEFAULT_TTL,
            .frag_idx = i,
            .frag_total = frag_total,
        };

        int rc = inst->frag_lower_send(inst, &hdr, &data[offset], chunk);
        if (rc != ROUTE_OK) return rc;
    }
    return ROUTE_OK;
}

int route_frag_send(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                    uint8_t seq, const uint8_t *data, uint16_t len) {
    return route_frag_send_typed(inst, dest, trans_id, seq, ROUTE_TYPE_REQUEST, data, len);
}

static route_reasm_ctx_t *find_reasm_slot(route_instance_t *inst, uint8_t src_id, uint8_t seq) {
    for (uint8_t i = 0; i < ROUTE_MAX_REASM_SLOTS; i++) {
        if (inst->reasm_slots[i].active &&
            inst->reasm_slots[i].src_id == src_id &&
            inst->reasm_slots[i].seq == seq) {
            return &inst->reasm_slots[i];
        }
    }
    return NULL;
}

static route_reasm_ctx_t *alloc_reasm_slot(route_instance_t *inst) {
    for (uint8_t i = 0; i < ROUTE_MAX_REASM_SLOTS; i++) {
        if (!inst->reasm_slots[i].active) {
            return &inst->reasm_slots[i];
        }
    }
    return NULL;
}

static void free_reasm_slot(route_instance_t *inst, route_reasm_ctx_t *slot) {
    for (uint8_t i = 0; i < slot->frag_total; i++) {
        if (slot->fragments[i] != NULL) {
            route_pool_free(&inst->pool, slot->fragments[i]);
            slot->fragments[i] = NULL;
        }
    }
    slot->active = 0;
}

int route_frag_recv(route_instance_t *inst, const route_header_t *hdr,
                    const uint8_t *payload, uint16_t payload_len,
                    uint8_t *out_buf, uint16_t *out_len, route_header_t *out_hdr) {
    // 安全检查：frag_total 不能为 0 或超过 fragments[] 数组容量
    uint8_t max_frags = (ROUTE_MAX_PAYLOAD / ROUTE_FRAG_SIZE) + 1;
    if (hdr->frag_total == 0 || hdr->frag_total > max_frags) return ROUTE_ERR_PARAM;

    if (hdr->frag_total == 1) {
        if (payload_len > ROUTE_MAX_PAYLOAD) return ROUTE_ERR_PARAM;
        memcpy(out_buf, payload, payload_len);
        *out_len = payload_len;
        *out_hdr = *hdr;
        return 1;
    }

    route_reasm_ctx_t *slot = find_reasm_slot(inst, hdr->src, hdr->seq);
    if (slot == NULL) {
        slot = alloc_reasm_slot(inst);
        if (slot == NULL) return ROUTE_ERR_FULL;
        slot->active = 1;
        slot->src_id = hdr->src;
        slot->seq = hdr->seq;
        slot->frag_total = hdr->frag_total;
        slot->received_count = 0;
        slot->start_ms = 0;
        memset(slot->fragments, 0, sizeof(slot->fragments));
        memset(slot->frag_lens, 0, sizeof(slot->frag_lens));
    }

    if (hdr->frag_idx >= slot->frag_total) return ROUTE_ERR_PARAM;
    if (payload_len > ROUTE_FRAG_SIZE) return ROUTE_ERR_PARAM;
    if (slot->fragments[hdr->frag_idx] != NULL) return 0;

    uint8_t *blk = route_pool_alloc(&inst->pool);
    if (blk == NULL) return ROUTE_ERR_NO_MEM;
    memcpy(blk, payload, payload_len);
    slot->fragments[hdr->frag_idx] = blk;
    slot->frag_lens[hdr->frag_idx] = (uint8_t)payload_len;
    slot->received_count++;

    if (slot->received_count == slot->frag_total) {
        *out_len = 0;
        for (uint8_t i = 0; i < slot->frag_total; i++) {
            // 安全检查：防止重组数据溢出 out_buf
            if (*out_len + slot->frag_lens[i] > ROUTE_MAX_PAYLOAD) {
                free_reasm_slot(inst, slot);
                return ROUTE_ERR_REASM;
            }
            memcpy(out_buf + *out_len, slot->fragments[i], slot->frag_lens[i]);
            *out_len += slot->frag_lens[i];
        }
        *out_hdr = *hdr;
        out_hdr->frag_idx = 0;
        out_hdr->frag_total = 1;
        free_reasm_slot(inst, slot);
        return 1;
    }
    return 0;
}

void route_frag_tick(route_instance_t *inst, uint32_t now_ms) {
    for (uint8_t i = 0; i < ROUTE_MAX_REASM_SLOTS; i++) {
        if (inst->reasm_slots[i].active) {
            if (inst->reasm_slots[i].start_ms == 0) {
                inst->reasm_slots[i].start_ms = now_ms;
            }
            if ((int32_t)(now_ms - inst->reasm_slots[i].start_ms) >= (int32_t)ROUTE_REASM_TIMEOUT_MS) {
                free_reasm_slot(inst, &inst->reasm_slots[i]);
            }
        }
    }
}

void route_frag_input(route_instance_t *inst, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len) {
    uint16_t reasm_len = 0;
    route_header_t reasm_hdr;

    int rc = route_frag_recv(inst, hdr, payload, payload_len, inst->reasm_buf, &reasm_len, &reasm_hdr);
    if (rc == 1 && inst->frag_complete_cb) {
        inst->frag_complete_cb(inst, &reasm_hdr, inst->reasm_buf, reasm_len);
    }
}
