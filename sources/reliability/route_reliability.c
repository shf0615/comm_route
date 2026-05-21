#include "route_reliability.h"
#include <string.h>

void route_reliability_set_lower_send(route_instance_t *inst, route_lower_send_t send_fn) {
    inst->reliability_lower_send = send_fn;
}

static int e2e_on_send(route_instance_t *inst, uint8_t dest, uint8_t seq,
                       uint8_t trans_id, const uint8_t *data, uint16_t len) {
    if (len > ROUTE_FRAG_SIZE) return ROUTE_ERR_PARAM;  // only single-frag data
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        if (!inst->pending_acks[i].active) {
            inst->pending_acks[i].active = 1;
            inst->pending_acks[i].dest_id = dest;
            inst->pending_acks[i].seq = seq;
            inst->pending_acks[i].trans_id = trans_id;
            inst->pending_acks[i].retry_count = 0;
            inst->pending_acks[i].next_retry_ms = 0;  // 0 表示尚未设置，首次 tick 时设为 now + timeout
            if (data && len > 0) {
                memcpy(inst->pending_acks[i].data, data, len);
            }
            inst->pending_acks[i].len = len;
            return ROUTE_OK;
        }
    }
    return ROUTE_ERR_FULL;
}

static void e2e_on_recv_ack(route_instance_t *inst, uint8_t src, uint8_t seq) {
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        if (inst->pending_acks[i].active &&
            inst->pending_acks[i].dest_id == src &&
            inst->pending_acks[i].seq == seq) {
            inst->pending_acks[i].active = 0;
            return;
        }
    }
}

static void e2e_on_tick(route_instance_t *inst, uint32_t now_ms) {
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        if (!inst->pending_acks[i].active) continue;

        // 首次 tick 时设置绝对超时时间
        if (inst->pending_acks[i].next_retry_ms == 0) {
            inst->pending_acks[i].next_retry_ms = now_ms + ROUTE_ACK_TIMEOUT_MS;
            continue;
        }

        if ((int32_t)(now_ms - inst->pending_acks[i].next_retry_ms) < 0) continue;

        if (inst->pending_acks[i].retry_count >= ROUTE_ACK_RETRY_MAX) {
            inst->pending_acks[i].active = 0;
            continue;
        }

        if (inst->reliability_lower_send) {
            route_header_t hdr = {
                .src = inst->node_id,
                .dst = inst->pending_acks[i].dest_id,
                .type = ROUTE_TYPE_REQUEST,
                .trans_id = inst->pending_acks[i].trans_id,
                .seq = inst->pending_acks[i].seq,
                .ttl = ROUTE_DEFAULT_TTL,
                .frag_idx = 0,
                .frag_total = 1,
            };
            inst->reliability_lower_send(inst, &hdr, inst->pending_acks[i].data, inst->pending_acks[i].len);
        }
        inst->pending_acks[i].retry_count++;
        inst->pending_acks[i].next_retry_ms = now_ms + ROUTE_ACK_TIMEOUT_MS;
    }
}

static const reliability_strategy_t e2e_strategy = {
    .on_send = e2e_on_send,
    .on_recv_ack = e2e_on_recv_ack,
    .on_tick = e2e_on_tick,
};

const reliability_strategy_t *route_reliability_e2e_strategy(void) {
    return &e2e_strategy;
}

int route_reliability_send_ack(route_instance_t *inst, uint8_t dest, uint8_t seq) {
    if (inst->reliability_lower_send == NULL) return ROUTE_ERR_PARAM;
    route_header_t hdr = {
        .src = inst->node_id,
        .dst = dest,
        .type = ROUTE_TYPE_ACK,
        .trans_id = 0,
        .seq = seq,
        .ttl = ROUTE_DEFAULT_TTL,
        .frag_idx = 0,
        .frag_total = 1,
    };
    return inst->reliability_lower_send(inst, &hdr, NULL, 0);
}
