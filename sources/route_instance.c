#include "route.h"
#include "common/route_pool.h"
#include "common/route_queue.h"
#include "router/route_router.h"
#include "frag/route_frag.h"
#include "reliability/route_reliability.h"
#include "transaction/route_transaction.h"
#include <string.h>

// ============ 内部层间适配回调 ============

// Router deliver → 分流 ACK / Frag
static void internal_router_deliver(route_instance_t *inst, const route_header_t *hdr,
                                    const uint8_t *payload, uint16_t payload_len) {
    if (hdr->type == ROUTE_TYPE_ACK) {
        if (inst->reliability) {
            inst->reliability->on_recv_ack(inst, hdr->src, hdr->seq, hdr->frag_idx);
        }
        return;
    }
    route_frag_input(inst, hdr, payload, payload_len);
}

// Frag complete → 分流 RESPONSE / REQUEST
static void internal_frag_complete(route_instance_t *inst, const route_header_t *hdr,
                                   const uint8_t *data, uint16_t len) {
    if (inst->reliability) {
        route_reliability_send_ack(inst, hdr->src, hdr->seq, hdr->frag_idx);
    }

    if (hdr->type == ROUTE_TYPE_RESPONSE) {
        route_transaction_on_response(inst, hdr->src, hdr->trans_id, data, len);
        return;
    }

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

int route_init(route_instance_t *inst, uint8_t node_id, const route_config_t *config) {
    if (inst == NULL || config == NULL) return ROUTE_ERR_PARAM;

    // Validate required storage pointers
    if (config->max_ports == 0 || config->ports == NULL) return ROUTE_ERR_PARAM;
    if (config->max_nodes == 0 || config->route_table == NULL) return ROUTE_ERR_PARAM;
    if (config->seen_table_size == 0 || config->seen_table == NULL) return ROUTE_ERR_PARAM;
    if (config->max_reasm_slots == 0 || config->reasm_slots == NULL) return ROUTE_ERR_PARAM;
    if (config->max_payload == 0 || config->reasm_buf == NULL) return ROUTE_ERR_PARAM;
    if (config->frag_size == 0) return ROUTE_ERR_PARAM;
    if (config->max_frags_per_msg == 0) return ROUTE_ERR_PARAM;
    if (config->reasm_frag_ptrs == NULL || config->reasm_frag_lens == NULL) return ROUTE_ERR_PARAM;
    if (config->max_concurrent_trans == 0 || config->pending_acks == NULL) return ROUTE_ERR_PARAM;
    if (config->pending_ack_data == NULL) return ROUTE_ERR_PARAM;
    if (config->trans_table == NULL) return ROUTE_ERR_PARAM;
    if (config->pool_block_count == 0 || config->pool_free_list == NULL || config->pool_storage == NULL) return ROUTE_ERR_PARAM;
    if (config->recv_queue_size == 0 || config->recv_queue_data == NULL) return ROUTE_ERR_PARAM;
    if (config->recv_queue_lengths == NULL || config->recv_queue_from_port == NULL) return ROUTE_ERR_PARAM;

    memset(inst, 0, sizeof(*inst));
    inst->node_id = node_id;
    inst->os = config->os;

    // Copy capacities
    inst->cfg_max_ports = config->max_ports;
    inst->cfg_max_nodes = config->max_nodes;
    inst->cfg_max_concurrent_trans = config->max_concurrent_trans;
    inst->cfg_max_reasm_slots = config->max_reasm_slots;
    inst->cfg_pool_block_count = config->pool_block_count;
    inst->cfg_seen_table_size = config->seen_table_size;
    inst->cfg_recv_queue_size = config->recv_queue_size;
    inst->cfg_max_payload = config->max_payload;
    inst->cfg_frag_size = config->frag_size;
    inst->cfg_block_size = ROUTE_HEADER_SIZE + config->frag_size;
    inst->cfg_max_frags_per_msg = config->max_frags_per_msg;
    inst->cfg_ack_timeout_ms = config->ack_timeout_ms;
    inst->cfg_ack_retry_max = config->ack_retry_max;
    inst->cfg_default_ttl = config->default_ttl;
    inst->cfg_seen_expire_ms = config->seen_expire_ms;
    inst->cfg_reasm_timeout_ms = config->reasm_timeout_ms;

    // Bind external storage
    inst->ports = config->ports;
    inst->route_table = config->route_table;
    inst->seen_table = config->seen_table;
    inst->reasm_slots = config->reasm_slots;
    inst->reasm_buf = config->reasm_buf;
    inst->pending_acks = config->pending_acks;
    inst->trans_table = config->trans_table;

    // Initialize reassembly slots — wire up per-slot fragment pointer arrays
    for (uint8_t i = 0; i < config->max_reasm_slots; i++) {
        inst->reasm_slots[i].active = 0;
        inst->reasm_slots[i].fragments = &config->reasm_frag_ptrs[i * config->max_frags_per_msg];
        inst->reasm_slots[i].frag_lens = &config->reasm_frag_lens[i * config->max_frags_per_msg];
    }

    // Initialize pending_ack data pointers
    for (uint8_t i = 0; i < config->max_concurrent_trans; i++) {
        inst->pending_acks[i].active = 0;
        inst->pending_acks[i].data = &config->pending_ack_data[i * config->frag_size];
    }

    // Initialize memory pool
    route_pool_init(&inst->pool, config->pool_free_list, config->pool_storage,
                    config->pool_block_count, inst->cfg_block_size);

    // Initialize receive queue
    route_queue_init(&inst->recv_queue, config->recv_queue_data, config->recv_queue_lengths,
                     config->recv_queue_from_port, config->recv_queue_size, inst->cfg_block_size);

    if (inst->os) {
        inst->trans_mutex = inst->os->mutex_create();
    }

    // 串联各层
    route_router_set_deliver_cb(inst, internal_router_deliver);
    route_frag_set_lower_send(inst, route_router_send);
    route_frag_set_complete_cb(inst, internal_frag_complete);
    route_reliability_set_lower_send(inst, route_router_send);
    route_transaction_set_lower_send(inst, internal_transaction_send);

    return ROUTE_OK;
}

void route_deinit(route_instance_t *inst) {
    if (inst == NULL) return;

    // 释放活跃的重组 slot 中的 pool block
    for (uint8_t i = 0; i < inst->cfg_max_reasm_slots; i++) {
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

    // 清除活跃的 reliability pending ACK 条目
    for (uint8_t i = 0; i < inst->cfg_max_concurrent_trans; i++) {
        inst->pending_acks[i].active = 0;
    }

    // 超时所有活跃 transaction
    if (inst->os && inst->trans_mutex) {
        inst->os->mutex_lock(inst->trans_mutex);
    }
    for (uint8_t i = 0; i < inst->cfg_max_concurrent_trans; i++) {
        transaction_t *t = &inst->trans_table[i];
        if (t->state == TRANS_STATE_WAITING || t->state == TRANS_STATE_SENDING) {
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
        inst->os->mutex_unlock(inst->trans_mutex);
        inst->os->mutex_destroy(inst->trans_mutex);
    }
}

// ============ 配置 ============

int route_port_register(route_instance_t *inst, const route_port_t *port) {
    if (inst->port_count >= inst->cfg_max_ports) return ROUTE_ERR_FULL;
    inst->ports[inst->port_count] = *port;
    inst->port_count++;
    return ROUTE_OK;
}

int route_table_set(route_instance_t *inst, const route_entry_t *entries, uint8_t count) {
    if (count > inst->cfg_max_nodes) return ROUTE_ERR_PARAM;
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
    if (len > inst->cfg_frag_size) return ROUTE_ERR_PARAM;
    uint8_t seq = inst->bcast_seq_counter++;
    route_header_t hdr = {
        .src = inst->node_id,
        .dst = ROUTE_BROADCAST_ADDR,
        .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0,
        .seq = seq,
        .ttl = inst->cfg_default_ttl,
        .frag_idx = 0,
        .frag_total = 1,
    };
    int rc = route_router_send(inst, &hdr, data, len);
    if (rc == ROUTE_OK) {
        inst->stats.tx_packets += inst->port_count;
        inst->stats.tx_bytes += (ROUTE_HEADER_SIZE + len) * inst->port_count;
    }
    if (cb) {
        cb(inst->node_id, rc, user_data);
    }
    return rc;
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

// ============ 统计 ============

const route_stats_t *route_get_stats(const route_instance_t *inst) {
    return &inst->stats;
}

void route_reset_stats(route_instance_t *inst) {
    memset(&inst->stats, 0, sizeof(inst->stats));
}
