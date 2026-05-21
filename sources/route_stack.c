#include "route_stack.h"
#include <string.h>

// ============ 层间适配器 ============

// Router deliver → Frag input
static void stack_router_deliver(void *ctx, const route_header_t *hdr,
                                 const uint8_t *payload, uint16_t payload_len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    route_frag_input(&stack->frag, hdr, payload, payload_len);
}

// Frag lower_send → Router send（适配签名）
static int stack_frag_to_router(void *ctx, const route_header_t *hdr,
                                const uint8_t *payload, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    return route_router_send(&stack->router, hdr, payload, len);
}

// Frag complete → 分发 RESPONSE / REQUEST
static void stack_frag_complete(void *ctx, const route_header_t *hdr,
                                const uint8_t *data, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    if (hdr->type == ROUTE_TYPE_RESPONSE) {
        route_transaction_on_response(&stack->transaction, hdr->src, hdr->trans_id, data, len);
        return;
    }
    // REQUEST → 应用回调
    if (stack->on_recv_cb) {
        stack->on_recv_cb(stack->on_recv_ctx, hdr->src, hdr->trans_id, data, len);
    }
}

// Transaction lower_send → Frag send（适配签名）
static int stack_trans_to_frag(void *ctx, uint8_t dest, uint8_t trans_id,
                               uint8_t seq, route_frame_type_t type,
                               const uint8_t *data, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    return route_frag_send(&stack->frag, dest, trans_id, seq, type, data, len);
}

// ============ Init / Deinit ============

int route_stack_init(route_stack_t *stack, const route_stack_config_t *cfg) {
    if (stack == NULL || cfg == NULL) return ROUTE_ERR_PARAM;

    memset(stack, 0, sizeof(*stack));
    stack->node_id = cfg->node_id;
    memset(&stack->stats, 0, sizeof(stack->stats));

    // Init Router
    route_router_config_t rcfg = {
        .node_id = cfg->node_id,
        .default_ttl = cfg->default_ttl,
        .frag_size = cfg->frag_size,
        .codec = cfg->codec,
        .max_ports = cfg->max_ports,
        .max_nodes = cfg->max_nodes,
        .seen_table_size = cfg->seen_table_size,
        .seen_expire_ms = cfg->seen_expire_ms,
        .recv_queue_size = cfg->recv_queue_size,
        .ports = cfg->ports,
        .route_table = cfg->route_table,
        .seen_table = cfg->seen_table,
        .recv_queue_data = cfg->recv_queue_data,
        .recv_queue_lengths = cfg->recv_queue_lengths,
        .recv_queue_from_port = cfg->recv_queue_from_port,
        .stats = &stack->stats,
    };
    int rc = route_router_init(&stack->router, &rcfg);
    if (rc != ROUTE_OK) return rc;

    // Init Frag
    route_frag_config_t fcfg = {
        .node_id = cfg->node_id,
        .frag_size = cfg->frag_size,
        .max_payload = cfg->max_payload,
        .max_frags_per_msg = cfg->max_frags_per_msg,
        .max_reasm_slots = cfg->max_reasm_slots,
        .reasm_timeout_ms = cfg->reasm_timeout_ms,
        .default_ttl = cfg->default_ttl,
        .max_pending_acks = cfg->max_pending_acks,
        .ack_timeout_ms = cfg->ack_timeout_ms,
        .ack_retry_max = cfg->ack_retry_max,
        .pending_acks = cfg->pending_acks,
        .pending_ack_data = cfg->pending_ack_data,
        .reasm_slots = cfg->reasm_slots,
        .reasm_buf = cfg->reasm_buf,
        .reasm_frag_ptrs = cfg->reasm_frag_ptrs,
        .reasm_frag_lens = cfg->reasm_frag_lens,
        .pool_free_list = cfg->pool_free_list,
        .pool_storage = cfg->pool_storage,
        .pool_block_count = cfg->pool_block_count,
        .stats = &stack->stats,
    };
    rc = route_frag_init(&stack->frag, &fcfg);
    if (rc != ROUTE_OK) return rc;

    // Init Transaction
    route_transaction_config_t tcfg = {
        .max_concurrent_trans = cfg->max_concurrent_trans,
        .default_timeout_ms = cfg->default_timeout_ms,
        .os = cfg->os,
        .trans_table = cfg->trans_table,
        .lower_send = stack_trans_to_frag,
        .lower_send_ctx = stack,
        .stats = &stack->stats,
    };
    rc = route_transaction_init(&stack->transaction, &tcfg);
    if (rc != ROUTE_OK) return rc;

    // Wire callbacks
    route_router_set_deliver_cb(&stack->router, stack_router_deliver, stack);
    route_frag_set_lower_send(&stack->frag, stack_frag_to_router, stack);
    route_frag_set_complete_cb(&stack->frag, stack_frag_complete, stack);

    return ROUTE_OK;
}

void route_stack_deinit(route_stack_t *stack) {
    if (stack == NULL) return;
    route_transaction_deinit(&stack->transaction);
    route_frag_deinit(&stack->frag);
    route_router_deinit(&stack->router);
}

// ============ Configuration ============

void route_stack_set_recv_cb(route_stack_t *stack,
    void (*cb)(void *ctx, uint8_t src, uint8_t trans_id, const uint8_t *data, uint16_t len),
    void *cb_ctx) {
    stack->on_recv_cb = cb;
    stack->on_recv_ctx = cb_ctx;
}

// ============ Driver ============

void route_stack_tick(route_stack_t *stack, uint32_t now_ms) {
    route_router_tick(&stack->router, now_ms);
    route_frag_tick(&stack->frag, now_ms);
    route_transaction_tick(&stack->transaction, now_ms);
}

void route_stack_poll(route_stack_t *stack) {
    route_router_poll(&stack->router);
}

// ============ Send ============

int route_stack_send_sync(route_stack_t *stack, uint8_t dest,
                          const uint8_t *data, uint16_t len,
                          uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    return route_transaction_send_sync(&stack->transaction, dest, data, len,
                                       resp_buf, resp_len, timeout_ms);
}

int route_stack_send_async(route_stack_t *stack, uint8_t dest,
                           const uint8_t *data, uint16_t len,
                           void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                           void *user_data) {
    return route_transaction_send_async(&stack->transaction, dest, data, len, cb, user_data);
}

int route_stack_reply(route_stack_t *stack, uint8_t dest, uint8_t trans_id,
                      const uint8_t *data, uint16_t len) {
    return route_transaction_reply(&stack->transaction, dest, trans_id, data, len);
}

int route_stack_broadcast(route_stack_t *stack, const uint8_t *data, uint16_t len) {
    if (len > stack->frag.cfg_frag_size) return ROUTE_ERR_PARAM;
    uint8_t seq = stack->bcast_seq_counter++;
    route_header_t hdr = {
        .src = stack->node_id,
        .dst = ROUTE_BROADCAST_ADDR,
        .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0,
        .seq = seq,
        .ttl = stack->router.cfg_default_ttl,
        .frag_idx = 0,
        .frag_total = 1,
    };
    int rc = route_router_send(&stack->router, &hdr, data, len);
    if (rc == ROUTE_OK) {
        stack->stats.tx_packets += stack->router.port_count;
        stack->stats.tx_bytes += (ROUTE_HEADER_SIZE + len) * stack->router.port_count;
    }
    return rc;
}

int route_stack_input(route_stack_t *stack, const uint8_t *data, uint16_t len, uint8_t port_id) {
    return route_router_input(&stack->router, data, len, port_id);
}
