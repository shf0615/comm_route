#ifndef ROUTE_FRAG_H
#define ROUTE_FRAG_H

#include "../common/route_types.h"
#include "../common/route_pool.h"

// ============ Reassembly Slot ============

typedef struct {
    uint8_t src_id;
    uint8_t seq;
    uint8_t frag_total;
    uint8_t received_count;
    uint32_t start_ms;
    uint8_t **fragments;
    uint16_t *frag_lens;
    uint8_t active;
} route_reasm_ctx_t;

// ============ Pending ACK (reliability) ============

typedef struct {
    uint8_t active;
    uint8_t dest_id;
    uint8_t seq;
    uint8_t frag_idx;
    uint8_t frag_total;
    uint8_t trans_id;
    uint8_t type;
    uint8_t retry_count;
    uint32_t next_retry_ms;
    uint8_t *data;      // 指向 pending_ack_data 中的分片存储
    uint16_t len;
} route_pending_ack_t;

// ============ Frag Config ============

typedef struct {
    route_reasm_ctx_t *slots;
    uint8_t *buf;                       // [max_payload]
    uint8_t **frag_ptrs;                // [max_reasm_slots * max_frags_per_msg]
    uint16_t *frag_lens;                // [max_reasm_slots * max_frags_per_msg]
    uint8_t max_slots;
    uint32_t timeout_ms;
} route_reasm_config_t;

typedef struct {
    uint8_t max_pending_acks;           // 0 = 禁用 reliability
    uint32_t ack_timeout_ms;
    uint8_t ack_retry_max;
    route_pending_ack_t *pending_acks;
    uint8_t *pending_ack_data;          // [max_pending_acks * frag_size]
} route_reliability_config_t;

typedef struct {
    uint8_t node_id;
    uint16_t frag_size;
    uint16_t max_payload;
    uint8_t max_frags_per_msg;
    uint8_t default_ttl;

    route_reasm_config_t reasm;
    route_reliability_config_t reliability;

    // Memory pool
    uint32_t *pool_bitmap;              // [(pool_block_count + 31) / 32]
    uint8_t *pool_storage;
    uint8_t pool_block_count;

    route_stats_t *stats;

    // 可选线程安全（NULL = 无锁，单线程使用）
    void (*lock)(void *lock_ctx);
    void (*unlock)(void *lock_ctx);
    void *lock_ctx;
} route_frag_config_t;

// ============ Frag Context ============

typedef struct {
    uint8_t node_id;
    uint16_t cfg_frag_size;
    uint16_t cfg_max_payload;
    uint8_t cfg_max_frags_per_msg;
    uint8_t cfg_max_reasm_slots;
    uint32_t cfg_reasm_timeout_ms;
    uint8_t cfg_default_ttl;
    uint32_t current_ms;

    // Reassembly
    route_reasm_ctx_t *reasm_slots;
    uint8_t *reasm_buf;
    route_pool_t pool;

    // Reliability (内嵌)
    uint8_t reliability_enabled;
    route_pending_ack_t *pending_acks;
    uint8_t cfg_max_pending_acks;
    uint32_t cfg_ack_timeout_ms;
    uint8_t cfg_ack_retry_max;

    // 下行：发送帧
    int (*lower_send)(void *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t len);
    void *lower_send_ctx;

    // 上行：重组完成（或单帧直接送达）
    void (*complete_cb)(void *ctx, const route_header_t *hdr,
                        const uint8_t *data, uint16_t len);
    void *complete_ctx;

    route_stats_t *stats;

    // 可选锁
    void (*lock)(void *lock_ctx);
    void (*unlock)(void *lock_ctx);
    void *lock_ctx;
} route_frag_ctx_t;

// ============ API ============

int route_frag_init(route_frag_ctx_t *ctx, const route_frag_config_t *cfg);
void route_frag_deinit(route_frag_ctx_t *ctx);

void route_frag_set_lower_send(route_frag_ctx_t *ctx,
    int (*send)(void *ctx, const route_header_t *hdr, const uint8_t *payload, uint16_t len),
    void *send_ctx);

void route_frag_set_complete_cb(route_frag_ctx_t *ctx,
    void (*cb)(void *ctx, const route_header_t *hdr, const uint8_t *data, uint16_t len),
    void *cb_ctx);

// 分片发送（自动注册 reliability 跟踪）
int route_frag_send(route_frag_ctx_t *ctx, uint8_t dest, uint8_t trans_id,
                    uint8_t seq, route_frame_type_t type,
                    const uint8_t *data, uint16_t len);

// 输入一帧（来自 router deliver_cb）
void route_frag_input(route_frag_ctx_t *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len);

// 驱动：重组超时 + 重传超时
void route_frag_tick(route_frag_ctx_t *ctx, uint32_t now_ms);

#endif // ROUTE_FRAG_H
