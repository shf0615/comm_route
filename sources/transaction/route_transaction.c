#include "route_transaction.h"
#include <string.h>

// ============ Internal ============

static inline void trans_lock(route_transaction_ctx_t *ctx) {
    if (ctx->os && ctx->mutex) ctx->os->mutex_lock(ctx->mutex);
}

static inline void trans_unlock(route_transaction_ctx_t *ctx) {
    if (ctx->os && ctx->mutex) ctx->os->mutex_unlock(ctx->mutex);
}

static int alloc_transaction(route_transaction_ctx_t *ctx) {
    for (uint8_t i = 0; i < ctx->cfg_max_concurrent_trans; i++) {
        if (ctx->trans_table[i].state == TRANS_STATE_IDLE) return i;
    }
    return -1;
}

// ============ Init / Deinit ============

int route_transaction_init(route_transaction_ctx_t *ctx, const route_transaction_config_t *cfg) {
    if (ctx == NULL || cfg == NULL) return ROUTE_ERR_PARAM;
    if (cfg->max_concurrent_trans == 0 || cfg->trans_table == NULL) return ROUTE_ERR_PARAM;

    memset(ctx, 0, sizeof(*ctx));
    ctx->cfg_max_concurrent_trans = cfg->max_concurrent_trans;
    ctx->cfg_default_timeout_ms = cfg->default_timeout_ms;
    ctx->os = cfg->os;
    ctx->trans_table = cfg->trans_table;
    ctx->lower_send = cfg->lower_send;
    ctx->lower_send_ctx = cfg->lower_send_ctx;
    ctx->stats = cfg->stats;

    if (ctx->os) {
        ctx->mutex = ctx->os->mutex_create();
    }
    ctx->shutdown = 0;

    return ROUTE_OK;
}

void route_transaction_deinit(route_transaction_ctx_t *ctx) {
    if (ctx == NULL) return;
    ctx->shutdown = 1;

    // 在锁内收集、设状态，锁外通知
    trans_lock(ctx);
    // 使用 trans_table 本身记录需要通知的信息（state 已设为 IDLE）
    // 先收集 callback/sem 信息到局部变量（大小由实际 max 决定）
    uint8_t max = ctx->cfg_max_concurrent_trans;
    trans_unlock(ctx);

    for (uint8_t i = 0; i < max; i++) {
        void (*cb)(int, const uint8_t *, uint16_t, void *) = NULL;
        void *ud = NULL;

        trans_lock(ctx);
        transaction_t *t = &ctx->trans_table[i];
        if (t->state == TRANS_STATE_WAITING || t->state == TRANS_STATE_SENDING) {
            if (t->callback) {
                cb = t->callback;
                ud = t->user_data;
            } else if (t->sync_sem && ctx->os) {
                t->result = ROUTE_ERR_TIMEOUT;
                // sem_post 在锁内，防止与 send_sync 超时后 sem_destroy 竞态
                ctx->os->sem_post(t->sync_sem);
            }
            t->state = TRANS_STATE_IDLE;
        }
        trans_unlock(ctx);

        // 锁外回调（回调可能耗时，不适合锁内）
        if (cb) {
            cb(ROUTE_ERR_TIMEOUT, NULL, 0, ud);
        }
    }

    if (ctx->os && ctx->mutex) {
        ctx->os->mutex_destroy(ctx->mutex);
        ctx->mutex = NULL;
    }
}

// ============ Send ============

int route_transaction_send_async(route_transaction_ctx_t *ctx, uint8_t dest,
                                 const uint8_t *data, uint16_t len,
                                 void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                                 void *user_data) {
    // Pre-fill slot fields before start_transaction sends
    trans_lock(ctx);
    int idx = alloc_transaction(ctx);
    if (idx < 0) { trans_unlock(ctx); return ROUTE_ERR_FULL; }
    transaction_t *t = &ctx->trans_table[idx];
    t->state = TRANS_STATE_SENDING;
    t->dest_id = dest;
    t->callback = cb;
    t->user_data = user_data;
    t->timeout_ms = 0;
    t->timeout_duration = ctx->cfg_default_timeout_ms;
    t->sync_sem = NULL;
    uint8_t seq = ctx->seq_counter++;
    t->expected_seq = seq;
    trans_unlock(ctx);

    // trans_id = slot index, 对端必须原样回传
    int rc = ctx->lower_send ?
        ctx->lower_send(ctx->lower_send_ctx, dest, (uint8_t)idx, seq, ROUTE_TYPE_REQUEST, data, len) :
        ROUTE_ERR_PARAM;

    if (rc != ROUTE_OK) {
        trans_lock(ctx);
        t->state = TRANS_STATE_IDLE;
        trans_unlock(ctx);
        return rc;
    }

    trans_lock(ctx);
    t->state = TRANS_STATE_WAITING;
    trans_unlock(ctx);
    return ROUTE_OK;
}

int route_transaction_send_sync(route_transaction_ctx_t *ctx, uint8_t dest,
                                const uint8_t *data, uint16_t len,
                                uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    if (ctx->os == NULL) return ROUTE_ERR_PARAM;

    trans_lock(ctx);
    int idx = alloc_transaction(ctx);
    if (idx < 0) { trans_unlock(ctx); return ROUTE_ERR_FULL; }
    transaction_t *t = &ctx->trans_table[idx];
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
    t->sync_sem = ctx->os->sem_create();
    if (t->sync_sem == NULL) {
        t->state = TRANS_STATE_IDLE;
        trans_unlock(ctx);
        return ROUTE_ERR_NO_MEM;
    }
    uint8_t seq = ctx->seq_counter++;
    t->expected_seq = seq;
    trans_unlock(ctx);

    // trans_id = slot index, 对端必须原样回传
    int rc = ctx->lower_send ?
        ctx->lower_send(ctx->lower_send_ctx, dest, (uint8_t)idx, seq, ROUTE_TYPE_REQUEST, data, len) :
        ROUTE_ERR_PARAM;

    if (rc != ROUTE_OK) {
        ctx->os->sem_destroy(t->sync_sem);
        trans_lock(ctx);
        t->sync_sem = NULL;
        t->state = TRANS_STATE_IDLE;
        trans_unlock(ctx);
        return rc;
    }

    trans_lock(ctx);
    t->state = TRANS_STATE_WAITING;
    trans_unlock(ctx);

    ctx->os->sem_wait(t->sync_sem, timeout_ms);

    trans_lock(ctx);
    int result = t->result;
    ctx->os->sem_destroy(t->sync_sem);
    t->sync_sem = NULL;
    t->state = TRANS_STATE_IDLE;
    trans_unlock(ctx);

    return result;
}

int route_transaction_reply(route_transaction_ctx_t *ctx, uint8_t dest, uint8_t trans_id,
                            const uint8_t *data, uint16_t len) {
    trans_lock(ctx);
    uint8_t seq = ctx->seq_counter++;
    trans_unlock(ctx);

    if (ctx->lower_send == NULL) return ROUTE_ERR_PARAM;
    return ctx->lower_send(ctx->lower_send_ctx, dest, trans_id, seq, ROUTE_TYPE_RESPONSE, data, len);
}

// ============ Response Handling ============

void route_transaction_on_response(route_transaction_ctx_t *ctx, uint8_t src,
                                   uint8_t trans_id, uint8_t seq,
                                   const uint8_t *data, uint16_t len) {
    if (ctx->shutdown) return;
    if (trans_id >= ctx->cfg_max_concurrent_trans) return;

    trans_lock(ctx);
    transaction_t *t = &ctx->trans_table[trans_id];

    if (t->state != TRANS_STATE_WAITING || t->dest_id != src || t->expected_seq != seq) {
        if (ctx->stats) ctx->stats->drop_duplicate++;
        trans_unlock(ctx);
        return;
    }

    if (t->callback) {
        void (*cb)(int, const uint8_t *, uint16_t, void *) = t->callback;
        void *ud = t->user_data;
        t->state = TRANS_STATE_IDLE;
        trans_unlock(ctx);
        cb(ROUTE_OK, data, len, ud);
    } else if (t->sync_sem) {
        uint16_t copy_len = len;
        if (copy_len > t->resp_max_len) copy_len = t->resp_max_len;
        if (t->resp_buf && copy_len > 0) memcpy(t->resp_buf, data, copy_len);
        if (t->resp_len) *t->resp_len = copy_len;
        t->result = ROUTE_OK;
        // sem_post 在锁内执行，防止与 send_sync 超时后 sem_destroy 竞态
        ctx->os->sem_post(t->sync_sem);
        trans_unlock(ctx);
    } else {
        t->state = TRANS_STATE_IDLE;
        trans_unlock(ctx);
    }
}

// ============ Tick ============

void route_transaction_tick(route_transaction_ctx_t *ctx, uint32_t now_ms) {
    if (ctx->shutdown) return;

    // 收集超时回调，锁外统一通知避免回调死锁；
    // sem_post 在锁内执行（短操作），防止与 send_sync 超时后 sem_destroy 竞态
#define TRANS_TICK_MAX_BATCH 32
    void (*timeout_cbs[TRANS_TICK_MAX_BATCH])(int, const uint8_t *, uint16_t, void *);
    void *timeout_uds[TRANS_TICK_MAX_BATCH];
    uint8_t timeout_count = 0;

    trans_lock(ctx);
    for (uint8_t i = 0; i < ctx->cfg_max_concurrent_trans; i++) {
        transaction_t *t = &ctx->trans_table[i];
        if (t->state != TRANS_STATE_WAITING) continue;

        if (t->timeout_ms == 0 && t->timeout_duration > 0) {
            t->timeout_ms = now_ms + t->timeout_duration;
            if (t->timeout_ms == 0) t->timeout_ms = 1;  // 避免与哨兵值冲突
            continue;
        }
        if (t->timeout_ms == 0 || (int32_t)(now_ms - t->timeout_ms) < 0) continue;

        // Timeout
        if (ctx->stats) ctx->stats->trans_timeouts++;

        if (t->callback) {
            if (timeout_count < TRANS_TICK_MAX_BATCH) {
                timeout_cbs[timeout_count] = t->callback;
                timeout_uds[timeout_count] = t->user_data;
                timeout_count++;
                t->state = TRANS_STATE_IDLE;
            }
            // else: batch full, leave in WAITING for next tick
        } else if (t->sync_sem) {
            t->result = ROUTE_ERR_TIMEOUT;
            // sem_post 在锁内，确保 send_sync 的 sem_destroy 与此序列化
            ctx->os->sem_post(t->sync_sem);
            t->state = TRANS_STATE_IDLE;
        } else {
            t->state = TRANS_STATE_IDLE;
        }
    }
    trans_unlock(ctx);

    // 锁外回调（回调可能耗时长，不适合在锁内执行）
    for (uint8_t i = 0; i < timeout_count; i++) {
        timeout_cbs[i](ROUTE_ERR_TIMEOUT, NULL, 0, timeout_uds[i]);
    }
}
