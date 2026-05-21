#ifndef ROUTE_STACK_H
#define ROUTE_STACK_H

#include "router/route_router.h"
#include "frag/route_frag.h"
#include "transaction/route_transaction.h"

// ============ Stack (便捷组合层) ============

typedef struct {
    route_router_ctx_t router;
    route_frag_ctx_t frag;
    route_transaction_ctx_t transaction;
    route_stats_t stats;
    uint8_t node_id;
    uint8_t bcast_seq_counter;

    // 应用层接收回调（REQUEST 帧到达）
    void (*on_recv_cb)(void *ctx, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len);
    void *on_recv_ctx;
} route_stack_t;

// ============ Stack Config ============

typedef struct {
    uint8_t node_id;
    const route_os_t *os;
    uint8_t default_ttl;

    // Codec (可选)
    const route_codec_t *codec;

    // Router
    uint16_t frag_size;
    uint8_t max_ports;
    uint8_t max_nodes;
    uint8_t seen_table_size;
    uint32_t seen_expire_ms;
    uint8_t recv_queue_size;
    route_port_t *ports;
    route_entry_t *route_table;
    route_seen_entry_t *seen_table;
    uint8_t *recv_queue_data;
    uint16_t *recv_queue_lengths;
    uint8_t *recv_queue_from_port;

    // Frag
    uint16_t max_payload;
    uint8_t max_frags_per_msg;
    uint8_t max_reasm_slots;
    uint32_t reasm_timeout_ms;
    route_reasm_ctx_t *reasm_slots;
    uint8_t *reasm_buf;
    uint8_t **reasm_frag_ptrs;
    uint8_t *reasm_frag_lens;
    uint8_t **pool_free_list;
    uint8_t *pool_storage;
    uint8_t pool_block_count;

    // Reliability (可选，设 0 禁用)
    uint8_t max_pending_acks;
    uint32_t ack_timeout_ms;
    uint8_t ack_retry_max;
    route_pending_ack_t *pending_acks;
    uint8_t *pending_ack_data;

    // Transaction
    uint8_t max_concurrent_trans;
    uint32_t default_timeout_ms;
    transaction_t *trans_table;
} route_stack_config_t;

// ============ API ============

int route_stack_init(route_stack_t *stack, const route_stack_config_t *cfg);
void route_stack_deinit(route_stack_t *stack);

void route_stack_set_recv_cb(route_stack_t *stack,
    void (*cb)(void *ctx, uint8_t src, uint8_t trans_id, const uint8_t *data, uint16_t len),
    void *cb_ctx);

void route_stack_tick(route_stack_t *stack, uint32_t now_ms);
void route_stack_poll(route_stack_t *stack);

// 便捷发送（代理 transaction）
int route_stack_send_sync(route_stack_t *stack, uint8_t dest,
                          const uint8_t *data, uint16_t len,
                          uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms);

int route_stack_send_async(route_stack_t *stack, uint8_t dest,
                           const uint8_t *data, uint16_t len,
                           void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                           void *user_data);

int route_stack_reply(route_stack_t *stack, uint8_t dest, uint8_t trans_id,
                      const uint8_t *data, uint16_t len);

int route_stack_broadcast(route_stack_t *stack, const uint8_t *data, uint16_t len);

// 输入原始帧（从 ISR / 接收线程调用）
int route_stack_input(route_stack_t *stack, const uint8_t *data, uint16_t len, uint8_t port_id);

#endif // ROUTE_STACK_H
