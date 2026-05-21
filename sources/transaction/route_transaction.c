#include "route_transaction.h"
#include "../frag/route_frag.h"
#include <string.h>

void route_transaction_set_lower_send(route_instance_t *inst, route_transaction_lower_send_t send_fn) {
    inst->transaction_lower_send = send_fn;
}

static int alloc_transaction(route_instance_t *inst) {
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
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
    t->timeout_duration = ROUTE_ACK_TIMEOUT_MS * (ROUTE_ACK_RETRY_MAX + 2);
    t->sync_sem = NULL;

    uint8_t seq = inst->seq_counter++;

    trans_unlock(inst);

    int rc = inst->transaction_lower_send ? inst->transaction_lower_send(inst, dest, (uint8_t)idx, seq, data, len) : ROUTE_ERR_PARAM;
    if (rc != ROUTE_OK) {
        trans_lock(inst);
        t->state = TRANS_STATE_IDLE;
        trans_unlock(inst);
        return rc;
    }

    // 只对单分片数据注册 reliability（多分片靠 transaction 超时保证）
    if (inst->reliability && len <= ROUTE_FRAG_SIZE) {
        inst->reliability->on_send(inst, dest, seq, (uint8_t)idx, data, len);
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

    uint8_t seq = inst->seq_counter++;

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

    // 只对单分片数据注册 reliability
    if (inst->reliability && len <= ROUTE_FRAG_SIZE) {
        inst->reliability->on_send(inst, dest, seq, (uint8_t)idx, data, len);
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
    uint8_t seq = inst->seq_counter++;
    return route_frag_send_typed(inst, dest, trans_id, seq, ROUTE_TYPE_RESPONSE, data, len);
}

void route_transaction_on_response(route_instance_t *inst, uint8_t src,
                                   uint8_t trans_id, const uint8_t *data, uint16_t len) {
    if (trans_id >= ROUTE_MAX_CONCURRENT_TRANSACTIONS) return;

    trans_lock(inst);

    transaction_t *t = &inst->trans_table[trans_id];
    if (t->state != TRANS_STATE_WAITING) {
        trans_unlock(inst);
        return;
    }
    if (t->dest_id != src) {
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
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
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
