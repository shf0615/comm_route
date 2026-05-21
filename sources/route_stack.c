#include "route_stack.h"
#include <string.h>

// ============ 层间适配器 ============

static void stack_router_deliver(void *ctx, const route_header_t *hdr,
                                 const uint8_t *payload, uint16_t payload_len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    if (stack->frag) {
        route_frag_input(stack->frag, hdr, payload, payload_len);
    } else {
        // 无 frag 层，直接送达
        if (hdr->type == ROUTE_TYPE_RESPONSE && stack->transaction) {
            route_transaction_on_response(stack->transaction, hdr->src, hdr->trans_id, hdr->seq, payload, payload_len);
        } else if (stack->on_recv_cb) {
            stack->on_recv_cb(stack->on_recv_ctx, hdr->src, hdr->trans_id, payload, payload_len);
        }
    }
}

static int stack_frag_to_router(void *ctx, const route_header_t *hdr,
                                const uint8_t *payload, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    return route_router_send(stack->router, hdr, payload, len);
}

static void stack_frag_complete(void *ctx, const route_header_t *hdr,
                                const uint8_t *data, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    if (hdr->type == ROUTE_TYPE_RESPONSE && stack->transaction) {
        route_transaction_on_response(stack->transaction, hdr->src, hdr->trans_id, hdr->seq, data, len);
    } else if (stack->on_recv_cb) {
        stack->on_recv_cb(stack->on_recv_ctx, hdr->src, hdr->trans_id, data, len);
    }
}

static int stack_trans_to_frag(void *ctx, uint8_t dest, uint8_t trans_id,
                               uint8_t seq, route_frame_type_t type,
                               const uint8_t *data, uint16_t len) {
    route_stack_t *stack = (route_stack_t *)ctx;
    if (stack->frag) {
        return route_frag_send(stack->frag, dest, trans_id, seq, type, data, len);
    }
    // 无 frag 层，直接构建单帧发 router
    route_header_t hdr = {
        .src = stack->node_id,
        .dst = dest,
        .type = type,
        .trans_id = trans_id,
        .seq = seq,
        .ttl = route_router_get_default_ttl(stack->router),
        .frag_idx = 0,
        .frag_total = 1,
    };
    return route_router_send(stack->router, &hdr, data, len);
}

// ============ Wire ============

int route_stack_wire(route_stack_t *stack, route_router_ctx_t *router,
                     route_frag_ctx_t *frag, route_transaction_ctx_t *transaction) {
    if (stack == NULL || router == NULL) return ROUTE_ERR_PARAM;

    stack->router = router;
    stack->frag = frag;
    stack->transaction = transaction;
    stack->node_id = router->node_id;
    stack->bcast_seq_counter = 0;

    // Router → stack
    route_router_set_deliver_cb(router, stack_router_deliver, stack);

    // Frag ↔ Router
    if (frag) {
        route_frag_set_lower_send(frag, stack_frag_to_router, stack);
        route_frag_set_complete_cb(frag, stack_frag_complete, stack);
    }

    // Transaction → Frag/Router
    if (transaction) {
        transaction->lower_send = stack_trans_to_frag;
        transaction->lower_send_ctx = stack;
    }

    return ROUTE_OK;
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
    route_router_tick(stack->router, now_ms);
    if (stack->frag) route_frag_tick(stack->frag, now_ms);
    if (stack->transaction) route_transaction_tick(stack->transaction, now_ms);
}

void route_stack_poll(route_stack_t *stack) {
    route_router_poll(stack->router);
}

// ============ Send ============

int route_stack_send_sync(route_stack_t *stack, uint8_t dest,
                          const uint8_t *data, uint16_t len,
                          uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms) {
    if (stack->transaction == NULL) return ROUTE_ERR_PARAM;
    return route_transaction_send_sync(stack->transaction, dest, data, len,
                                       resp_buf, resp_len, timeout_ms);
}

int route_stack_send_async(route_stack_t *stack, uint8_t dest,
                           const uint8_t *data, uint16_t len,
                           void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                           void *user_data) {
    if (stack->transaction == NULL) return ROUTE_ERR_PARAM;
    return route_transaction_send_async(stack->transaction, dest, data, len, cb, user_data);
}

int route_stack_reply(route_stack_t *stack, uint8_t dest, uint8_t trans_id,
                      const uint8_t *data, uint16_t len) {
    if (stack->transaction == NULL) return ROUTE_ERR_PARAM;
    return route_transaction_reply(stack->transaction, dest, trans_id, data, len);
}

int route_stack_broadcast(route_stack_t *stack, const uint8_t *data, uint16_t len) {
    if (len > route_router_get_frag_size(stack->router)) return ROUTE_ERR_PARAM;
    uint8_t seq = stack->bcast_seq_counter++;
    route_header_t hdr = {
        .src = stack->node_id,
        .dst = ROUTE_BROADCAST_ADDR,
        .type = ROUTE_TYPE_REQUEST,
        .trans_id = 0,
        .seq = seq,
        .ttl = route_router_get_default_ttl(stack->router),
        .frag_idx = 0,
        .frag_total = 1,
    };
    return route_router_send(stack->router, &hdr, data, len);
}

int route_stack_input(route_stack_t *stack, const uint8_t *data, uint16_t len, uint8_t port_id) {
    return route_router_input(stack->router, data, len, port_id);
}
