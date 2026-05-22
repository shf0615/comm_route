#ifndef ROUTE_STACK_H
#define ROUTE_STACK_H

#include "router/route_router.h"
#include "frag/route_frag.h"
#include "transaction/route_transaction.h"

typedef struct {
    route_router_ctx_t *router;
    route_frag_ctx_t *frag;                 
    route_transaction_ctx_t *transaction;   

    uint8_t node_id;
    uint8_t bcast_seq_counter;

    void (*on_recv_cb)(void *ctx, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len);
    void *on_recv_ctx;
} route_stack_t;

int route_stack_wire(route_stack_t *stack, route_router_ctx_t *router,
                     route_frag_ctx_t *frag, route_transaction_ctx_t *transaction);

void route_stack_set_recv_cb(route_stack_t *stack,
    void (*cb)(void *ctx, uint8_t src, uint8_t trans_id, const uint8_t *data, uint16_t len),
    void *cb_ctx);

void route_stack_tick(route_stack_t *stack, uint32_t now_ms);
void route_stack_poll(route_stack_t *stack);

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

int route_stack_input(route_stack_t *stack, const uint8_t *data, uint16_t len, uint8_t port_id);

#define ROUTE_STACK_WIRE_AND_CB(stack, router, frag, trans, cb, ctx) do { \
    route_stack_wire(&(stack), (router), (frag), (trans));               \
    route_stack_set_recv_cb(&(stack), (cb), (ctx));                      \
} while(0)

#endif 
