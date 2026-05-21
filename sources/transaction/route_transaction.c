#include "route_transaction.h"
#include "../frag/route_frag.h"
#include <string.h>

void route_transaction_set_lower_send(route_instance_t *inst, route_transaction_lower_send_t send_fn) {
    inst->transaction_lower_send = send_fn;
}

static int alloc_transaction(route_instance_t *inst) {
    for (uint8_t i = 0; i < inst->cfg_max_concurrent_trans; i++) {
        if (inst->trans_table[i].state == TRANS_STATE_IDLE) {
            return i;
        }
    }
    return -1;
}

static inline void trans_lock(route_instance_t *inst) {
    if (inst->os && inst->trans_mutex) inst->os->mutex_lock(inst->trans_mutex);
}

static inline void trans_unlock(route_instance_t *inst) {
    if (inst->os && inst->trans_mutex) inst->os->mutex_unlock(inst->trans_mutex);
}

int route_transaction_send_async(route_instance_t *inst, uint8_t dest,
                                 const uint8_t *data, uint16_t len,
                                 void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                                 void *user_data) {
    trans_lock(inst);

    int idx = alloc_transaction(inst);
    if (idx < 0) {
        trans_unlock(inst);
        return ROUTE_ERR_FULL;
    }

    transaction_t *t = &inst->trans_table[idx];
    t->state = TRANS_STATE_SENDING;  // 防止 on_response 在发送期间触发
    t->dest_id = dest;
    t->callback = cb;
    t->user_data = user_data;
    t->timeout_ms = 0;
    t->timeout_duration = inst->cfg_ack_timeout_ms * (inst->cfg_ack_retry_max + 2);
    t->sync_sem = NULL;

    uint8_t seq = inst->seq_counter++;  // protected by trans_lock

    trans_unlock(inst);

    int rc = inst->transaction_lower_send ? inst->transaction_lower_send(inst, dest, (uint8_t)idx, seq, data, len) : ROUTE_ERR_PARAM;
    if (rc != ROUTE_OK) {
        trans_lock(inst);
        t->state = TRANS_STATE_IDLE;
        trans_unlock(inst);
        return rc;
    }

    // 注册 reliability（每个分片均独立跟踪 ACK）
    if (inst->reliability) {
        uint8_t frag_total = (len + inst->cfg_frag_size - 1) / inst->cfg_frag_size;
        if (frag_total == 0) frag_total = 1;
        for (uint8_t fi = 0; fi < frag_total; fi++) {
            uint16_t offset = fi * inst->cfg_frag_size;
            uint16_t chunk = len - offset;
            if (chunk > inst->cfg_frag_size) chunk = inst->cfg_frag_size;
            inst->reliability->on_send(inst, dest, seq, fi, frag_total, ROUTE_TYPE_REQUEST, (uint8_t)idx, &data[offset], chunk);
        }
    }

    // 发送完成，切换到 WAITING 状态
    trans_lock(inst);
    t->state = TRANS_STATE_WAITING;
    trans_unlock(inst);

    return ROUTE_OK;
}

int route_transaction_send_sync(route_instance_t *inst, uint8_t dest,
                                const uint8_t *data, uint16_t len,
                                uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    if (inst->os == NULL) return ROUTE_ERR_PARAM;

    trans_lock(inst);

    int idx = alloc_transaction(inst);
    if (idx < 0) {
        trans_unlock(inst);
        return ROUTE_ERR_FULL;
    }

    transaction_t *t = &inst->trans_table[idx];
    t->state = TRANS_STATE_SENDING;
    t->dest_id = dest;
    t->callback = NULL;
    t->user_data = NULL;
    t->resp_buf = resp_buf;
    t->resp_len = resp_len;
    t->resp_max_len = *resp_len;
    t->result = ROUTE_ERR_TIMEOUT;
    t->timeout_ms = 0;
    t->timeout_duration = timeout_ms;
    t->sync_sem = inst->os->sem_create();

    uint8_t seq = inst->seq_counter++;  // protected by trans_lock

    trans_unlock(inst);

    int rc = inst->transaction_lower_send ? inst->transaction_lower_send(inst, dest, (uint8_t)idx, seq, data, len) : ROUTE_ERR_PARAM;
    if (rc != ROUTE_OK) {
        inst->os->sem_destroy(t->sync_sem);
        trans_lock(inst);
        t->sync_sem = NULL;
        t->state = TRANS_STATE_IDLE;
        trans_unlock(inst);
        return rc;
    }

    // 注册 reliability（每个分片均独立跟踪 ACK）
    if (inst->reliability) {
        uint8_t frag_total = (len + inst->cfg_frag_size - 1) / inst->cfg_frag_size;
        if (frag_total == 0) frag_total = 1;
        for (uint8_t fi = 0; fi < frag_total; fi++) {
            uint16_t offset = fi * inst->cfg_frag_size;
            uint16_t chunk = len - offset;
            if (chunk > inst->cfg_frag_size) chunk = inst->cfg_frag_size;
            inst->reliability->on_send(inst, dest, seq, fi, frag_total, ROUTE_TYPE_REQUEST, (uint8_t)idx, &data[offset], chunk);
        }
    }

    // 切换到 WAITING
    trans_lock(inst);
    t->state = TRANS_STATE_WAITING;
    trans_unlock(inst);

    inst->os->sem_wait(t->sync_sem, timeout_ms);

    trans_lock(inst);
    int result = t->result;
    inst->os->sem_destroy(t->sync_sem);
    t->sync_sem = NULL;
    t->state = TRANS_STATE_IDLE;
    trans_unlock(inst);

    return result;
}

int route_transaction_reply(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                            const uint8_t *data, uint16_t len) {
    trans_lock(inst);
    uint8_t seq = inst->seq_counter++;
    trans_unlock(inst);
    return route_frag_send_typed(inst, dest, trans_id, seq, ROUTE_TYPE_RESPONSE, data, len);
}

void route_transaction_on_response(route_instance_t *inst, uint8_t src,
                                   uint8_t trans_id, const uint8_t *data, uint16_t len) {
    if (trans_id >= inst->cfg_max_concurrent_trans) return;

    trans_lock(inst);

    transaction_t *t = &inst->trans_table[trans_id];
    if (t->state != TRANS_STATE_WAITING) {
        inst->stats.drop_duplicate++;
        trans_unlock(inst);
        return;
    }
    if (t->dest_id != src) {
        inst->stats.drop_duplicate++;
        trans_unlock(inst);
        return;
    }

    if (t->callback) {
        // 记录回调信息，在锁外调用
        void (*cb)(int, const uint8_t *, uint16_t, void *) = t->callback;
        void *ud = t->user_data;
        t->state = TRANS_STATE_IDLE;
        trans_unlock(inst);
        cb(ROUTE_OK, data, len, ud);
    } else if (t->sync_sem) {
        uint16_t copy_len = len;
        if (copy_len > t->resp_max_len) copy_len = t->resp_max_len;
        if (t->resp_buf && copy_len > 0) memcpy(t->resp_buf, data, copy_len);
        if (t->resp_len) *t->resp_len = copy_len;
        t->result = ROUTE_OK;
        inst->os->sem_post(t->sync_sem);
        // state 由 send_sync 在 sem_wait 返回后设为 IDLE
        trans_unlock(inst);
    } else {
        t->state = TRANS_STATE_IDLE;
        trans_unlock(inst);
    }
}

void route_transaction_tick(route_instance_t *inst, uint32_t now_ms) {
    for (uint8_t i = 0; i < inst->cfg_max_concurrent_trans; i++) {
        trans_lock(inst);

        transaction_t *t = &inst->trans_table[i];
        if (t->state != TRANS_STATE_WAITING) {
            trans_unlock(inst);
            continue;
        }

        // Set absolute deadline on first tick
        if (t->timeout_ms == 0 && t->timeout_duration > 0) {
            t->timeout_ms = now_ms + t->timeout_duration;
            trans_unlock(inst);
            continue;
        }
        if (t->timeout_ms == 0) {
            trans_unlock(inst);
            continue;
        }
        if ((int32_t)(now_ms - t->timeout_ms) < 0) {
            trans_unlock(inst);
            continue;
        }

        // Timeout
        inst->stats.trans_timeouts++;
        if (t->callback) {
            void (*cb)(int, const uint8_t *, uint16_t, void *) = t->callback;
            void *ud = t->user_data;
            t->state = TRANS_STATE_IDLE;
            trans_unlock(inst);
            cb(ROUTE_ERR_TIMEOUT, NULL, 0, ud);
        } else if (t->sync_sem) {
            t->result = ROUTE_ERR_TIMEOUT;
            inst->os->sem_post(t->sync_sem);
            trans_unlock(inst);
        } else {
            t->state = TRANS_STATE_IDLE;
            trans_unlock(inst);
        }
    }
}
