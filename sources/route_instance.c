#include "route.h"
#include "common/route_pool.h"
#include "common/route_queue.h"
#include "router/route_router.h"
#include "frag/route_frag.h"
#include "reliability/route_reliability.h"
#include "transaction/route_transaction.h"
#include <string.h>

static route_instance_t instances[ROUTE_MAX_INSTANCES];
static uint8_t instance_used[ROUTE_MAX_INSTANCES];

// ============ 内部层间适配回调 ============

// Router deliver → 分流 ACK / Frag
static void internal_router_deliver(route_instance_t *inst, const route_header_t *hdr,
                                    const uint8_t *payload, uint16_t payload_len) {
    // ACK → reliability
    if (hdr->type == ROUTE_TYPE_ACK) {
        if (inst->reliability) {
            inst->reliability->on_recv_ack(inst, hdr->src, hdr->seq);
        }
        return;
    }
    // 其他帧 → frag 重组
    route_frag_input(inst, hdr, payload, payload_len);
}

// Frag complete → 分流 RESPONSE / REQUEST
static void internal_frag_complete(route_instance_t *inst, const route_header_t *hdr,
                                   const uint8_t *data, uint16_t len) {
    // 发送 ACK
    if (inst->reliability) {
        route_reliability_send_ack(inst, hdr->src, hdr->seq);
    }

    // RESPONSE → transaction
    if (hdr->type == ROUTE_TYPE_RESPONSE) {
        route_transaction_on_response(inst, hdr->src, hdr->trans_id, data, len);
        return;
    }

    // REQUEST → 应用回调
    if (inst->on_recv_cb) {
        inst->on_recv_cb(inst, hdr->src, hdr->trans_id, data, len);
    }
}

// Transaction lower send 适配 frag_send
static int internal_transaction_send(route_instance_t *inst, uint8_t dest,
                                     uint8_t trans_id, uint8_t seq,
                                     const uint8_t *data, uint16_t len) {
    return route_frag_send(inst, dest, trans_id, seq, data, len);
}

// ============ 生命周期 ============

route_instance_t *route_create(uint8_t node_id, const route_config_t *config) {
    for (uint8_t i = 0; i < ROUTE_MAX_INSTANCES; i++) {
        if (!instance_used[i]) {
            instance_used[i] = 1;
            route_instance_t *inst = &instances[i];
            memset(inst, 0, sizeof(*inst));
            inst->node_id = node_id;
            inst->os = config ? config->os : NULL;
            route_pool_init(&inst->pool, inst->pool_storage, ROUTE_POOL_BLOCK_COUNT, ROUTE_BLOCK_SIZE);
            route_queue_init(&inst->recv_queue);
            if (inst->os) {
                inst->trans_mutex = inst->os->mutex_create();
            }

            // 串联各层
            route_router_set_deliver_cb(inst, internal_router_deliver);
            route_frag_set_lower_send(inst, route_router_send);
            route_frag_set_complete_cb(inst, internal_frag_complete);
            route_reliability_set_lower_send(inst, route_router_send);
            route_transaction_set_lower_send(inst, internal_transaction_send);
            // transaction reply 也通过 frag_lower_send（即 route_router_send）

            return inst;
        }
    }
    return NULL;
}

void route_destroy(route_instance_t *inst) {
    if (inst == NULL) return;

    // 释放活跃的重组 slot 中的 pool block
    for (uint8_t i = 0; i < ROUTE_MAX_REASM_SLOTS; i++) {
        if (inst->reasm_slots[i].active) {
            for (uint8_t j = 0; j < inst->reasm_slots[i].frag_total; j++) {
                if (inst->reasm_slots[i].fragments[j] != NULL) {
                    route_pool_free(&inst->pool, inst->reasm_slots[i].fragments[j]);
                    inst->reasm_slots[i].fragments[j] = NULL;
                }
            }
            inst->reasm_slots[i].active = 0;
        }
    }

    // 超时所有活跃 transaction（唤醒阻塞的同步调用者）
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        transaction_t *t = &inst->trans_table[i];
        if (t->state == TRANS_STATE_WAITING) {
            if (t->callback) {
                t->callback(ROUTE_ERR_TIMEOUT, NULL, 0, t->user_data);
            } else if (t->sync_sem && inst->os) {
                t->result = ROUTE_ERR_TIMEOUT;
                inst->os->sem_post(t->sync_sem);
            }
            t->state = TRANS_STATE_IDLE;
        }
    }

    if (inst->os && inst->trans_mutex) {
        inst->os->mutex_destroy(inst->trans_mutex);
    }
    for (uint8_t i = 0; i < ROUTE_MAX_INSTANCES; i++) {
        if (&instances[i] == inst) {
            instance_used[i] = 0;
            break;
        }
    }
}

// ============ 配置 ============

int route_port_register(route_instance_t *inst, const route_port_t *port) {
    if (inst->port_count >= ROUTE_MAX_PORTS) return ROUTE_ERR_FULL;
    inst->ports[inst->port_count] = *port;
    inst->port_count++;
    return ROUTE_OK;
}

int route_table_set(route_instance_t *inst, const route_entry_t *entries, uint8_t count) {
    if (count > ROUTE_MAX_NODES) return ROUTE_ERR_PARAM;
    memcpy(inst->route_table, entries, count * sizeof(route_entry_t));
    inst->route_count = count;
    return ROUTE_OK;
}

void route_reliability_set(route_instance_t *inst, const reliability_strategy_t *strategy) {
    inst->reliability = strategy;
}

void route_on_recv(route_instance_t *inst,
                   void (*cb)(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                              const uint8_t *data, uint16_t len)) {
    inst->on_recv_cb = cb;
}

// ============ 发送 ============

int route_send_sync(route_instance_t *inst, uint8_t dest,
                    const uint8_t *data, uint16_t len,
                    uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    return route_transaction_send_sync(inst, dest, data, len, resp_buf, resp_len, timeout_ms);
}

int route_send_async(route_instance_t *inst, uint8_t dest,
                     const uint8_t *data, uint16_t len,
                     void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                     void *user_data) {
    return route_transaction_send_async(inst, dest, data, len, cb, user_data);
}

int route_broadcast(route_instance_t *inst, const uint8_t *data, uint16_t len,
                    void (*cb)(uint8_t src_id, int result, void *user_data),
                    void *user_data) {
    (void)cb; (void)user_data;
    if (len > ROUTE_FRAG_SIZE) return ROUTE_ERR_PARAM;  // 广播不支持分片
    uint8_t seq = inst->seq_counter++;
    route_header_t hdr = {
        .src = inst->node_id,
        .dst = ROUTE_BROADCAST_ADDR,
        .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0,
        .seq = seq,
        .ttl = ROUTE_DEFAULT_TTL,
        .frag_idx = 0,
        .frag_total = 1,
    };
    return route_router_send(inst, &hdr, data, len);
}

int route_reply(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                const uint8_t *data, uint16_t len) {
    return route_transaction_reply(inst, dest, trans_id, data, len);
}

// ============ 驱动 ============

void route_tick(route_instance_t *inst, uint32_t now_ms) {
    inst->current_ms = now_ms;
    route_frag_tick(inst, now_ms);
    if (inst->reliability) {
        inst->reliability->on_tick(inst, now_ms);
    }
    route_transaction_tick(inst, now_ms);
}

void route_poll(route_instance_t *inst) {
    route_router_poll(inst);
}
