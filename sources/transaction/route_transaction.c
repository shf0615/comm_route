#include "route_transaction.h"
#include "../frag/route_frag.h"
#include <string.h>

void route_transaction_set_lower_send(route_instance_t *inst, route_transaction_lower_send_t send_fn) {
    inst->transaction_lower_send = send_fn;
}

void route_transaction_set_reply_send(route_instance_t *inst, route_lower_send_t send_fn) {
    (void)inst; (void)send_fn;
    // reply_send stored via reliability_lower_send (shared)
}

static int alloc_transaction(route_instance_t *inst) {
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        if (inst->trans_table[i].state == TRANS_STATE_IDLE) {
            return i;
        }
    }
    return -1;
}

int route_transaction_send_async(route_instance_t *inst, uint8_t dest,
                                 const uint8_t *data, uint16_t len,
                                 void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                                 void *user_data) {
    if (inst->os) inst->os->mutex_lock(inst->trans_mutex);

    int idx = alloc_transaction(inst);
    if (idx < 0) {
        if (inst->os) inst->os->mutex_unlock(inst->trans_mutex);
        return ROUTE_ERR_FULL;
    }

    transaction_t *t = &inst->trans_table[idx];
    t->state = TRANS_STATE_WAITING;
    t->dest_id = dest;
    t->callback = cb;
    t->user_data = user_data;
    t->timeout_ms = 0;  // will be set on first tick
    t->timeout_duration = ROUTE_ACK_TIMEOUT_MS * (ROUTE_ACK_RETRY_MAX + 2);
    t->sync_sem = NULL;

    uint8_t seq = inst->seq_counter++;

    if (inst->os) inst->os->mutex_unlock(inst->trans_mutex);

    int rc = inst->transaction_lower_send ? inst->transaction_lower_send(inst, dest, (uint8_t)idx, seq, data, len) : ROUTE_ERR_PARAM;
    if (rc != ROUTE_OK) {
        if (inst->os) inst->os->mutex_lock(inst->trans_mutex);
        t->state = TRANS_STATE_IDLE;
        if (inst->os) inst->os->mutex_unlock(inst->trans_mutex);
        return rc;
    }

    if (inst->reliability) {
        inst->reliability->on_send(inst, dest, seq, (uint8_t)idx, data, len);
    }

    return ROUTE_OK;
}

int route_transaction_send_sync(route_instance_t *inst, uint8_t dest,
                                const uint8_t *data, uint16_t len,
                                uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    if (inst->os == NULL) return ROUTE_ERR_PARAM;

    inst->os->mutex_lock(inst->trans_mutex);

    int idx = alloc_transaction(inst);
    if (idx < 0) {
        inst->os->mutex_unlock(inst->trans_mutex);
        return ROUTE_ERR_FULL;
    }

    transaction_t *t = &inst->trans_table[idx];
    t->state = TRANS_STATE_WAITING;
    t->dest_id = dest;
    t->callback = NULL;
    t->user_data = NULL;
    t->resp_buf = resp_buf;
    t->resp_len = resp_len;
    t->resp_max_len = *resp_len;
    t->result = ROUTE_ERR_TIMEOUT;
    t->sync_sem = inst->os->sem_create();

    uint8_t seq = inst->seq_counter++;

    inst->os->mutex_unlock(inst->trans_mutex);

    int rc = inst->transaction_lower_send ? inst->transaction_lower_send(inst, dest, (uint8_t)idx, seq, data, len) : ROUTE_ERR_PARAM;
    if (rc != ROUTE_OK) {
        inst->os->sem_destroy(t->sync_sem);
        t->state = TRANS_STATE_IDLE;
        return rc;
    }

    if (inst->reliability) {
        inst->reliability->on_send(inst, dest, seq, (uint8_t)idx, data, len);
    }

    inst->os->sem_wait(t->sync_sem, timeout_ms);
    inst->os->sem_destroy(t->sync_sem);
    t->sync_sem = NULL;

    int result = t->result;
    t->state = TRANS_STATE_IDLE;
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

    transaction_t *t = &inst->trans_table[trans_id];
    if (t->state != TRANS_STATE_WAITING) return;
    if (t->dest_id != src) return;

    if (t->callback) {
        t->callback(ROUTE_OK, data, len, t->user_data);
        t->state = TRANS_STATE_IDLE;
    } else if (t->sync_sem) {
        uint16_t copy_len = len;
        if (copy_len > t->resp_max_len) copy_len = t->resp_max_len;
        if (t->resp_buf && copy_len > 0) memcpy(t->resp_buf, data, copy_len);
        if (t->resp_len) *t->resp_len = copy_len;
        t->result = ROUTE_OK;
        inst->os->sem_post(t->sync_sem);
    }
}

void route_transaction_tick(route_instance_t *inst, uint32_t now_ms) {
    for (uint8_t i = 0; i < ROUTE_MAX_CONCURRENT_TRANSACTIONS; i++) {
        transaction_t *t = &inst->trans_table[i];
        if (t->state != TRANS_STATE_WAITING) continue;

        // Set absolute deadline on first tick
        if (t->timeout_ms == 0 && t->timeout_duration > 0) {
            t->timeout_ms = now_ms + t->timeout_duration;
            continue;
        }
        if (t->timeout_ms == 0) continue;
        if ((int32_t)(now_ms - t->timeout_ms) < 0) continue;

        // Timeout
        if (t->callback) {
            t->callback(ROUTE_ERR_TIMEOUT, NULL, 0, t->user_data);
            t->state = TRANS_STATE_IDLE;
        } else if (t->sync_sem) {
            t->result = ROUTE_ERR_TIMEOUT;
            inst->os->sem_post(t->sync_sem);
        }
    }
}
