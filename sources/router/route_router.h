#ifndef ROUTE_ROUTER_H
#define ROUTE_ROUTER_H

#include "../common/route_types.h"
#include "../common/route_queue.h"

typedef struct {
    
    uint16_t (*encode)(const route_header_t *hdr, const uint8_t *payload,
                       uint16_t payload_len, uint8_t *frame);
    
    int (*decode)(const uint8_t *frame, uint16_t frame_len,
                  route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len);
    uint16_t overhead;  
} route_codec_t;

typedef struct {
    uint8_t src_id;
    uint8_t seq;
    uint8_t valid;
    uint32_t timestamp_ms;
} route_seen_entry_t;

typedef struct {
    uint8_t node_id;
    uint8_t default_ttl;
    uint16_t frag_size;         

    
    const route_codec_t *codec;

    
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

    
    uint8_t *send_frame_buf;    
    uint8_t *fwd_frame_buf;     
    uint8_t *rx_frame_buf;      

    route_stats_t *stats;       

    
    void (*queue_lock)(void *lock_ctx);
    void (*queue_unlock)(void *lock_ctx);
    void *queue_lock_ctx;
} route_router_config_t;

typedef struct {
    uint8_t node_id;
    uint8_t cfg_default_ttl;
    uint16_t cfg_block_size;    
    uint16_t cfg_frag_size;

    route_port_t *ports;
    uint8_t port_count;
    uint8_t cfg_max_ports;

    route_entry_t *route_table;
    uint8_t route_count;
    uint8_t cfg_max_nodes;

    route_seen_entry_t *seen_table;
    uint8_t seen_index;
    uint8_t cfg_seen_table_size;
    uint32_t cfg_seen_expire_ms;
    volatile uint32_t current_ms;

    route_recv_queue_t recv_queue;

    
    uint8_t *send_frame_buf;    
    uint8_t *fwd_frame_buf;     
    uint8_t *rx_frame_buf;      

    const route_codec_t *codec;

    
    void (*deliver_cb)(void *ctx, const route_header_t *hdr,
                       const uint8_t *payload, uint16_t payload_len);
    void *deliver_ctx;

    route_stats_t *stats;

    
    void (*queue_lock)(void *lock_ctx);
    void (*queue_unlock)(void *lock_ctx);
    void *queue_lock_ctx;
} route_router_ctx_t;

int route_router_init(route_router_ctx_t *ctx, const route_router_config_t *cfg);
void route_router_deinit(route_router_ctx_t *ctx);

int route_router_port_register(route_router_ctx_t *ctx, const route_port_t *port);
int route_router_table_set(route_router_ctx_t *ctx, const route_entry_t *entries, uint8_t count);

void route_router_set_deliver_cb(route_router_ctx_t *ctx,
    void (*cb)(void *ctx, const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len),
    void *cb_ctx);

int route_router_send(route_router_ctx_t *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len);

int route_router_input(route_router_ctx_t *ctx, const uint8_t *data, uint16_t len, uint8_t port_id);

void route_router_poll(route_router_ctx_t *ctx);

void route_router_tick(route_router_ctx_t *ctx, uint32_t now_ms);

static inline uint8_t route_router_get_default_ttl(const route_router_ctx_t *ctx) { return ctx->cfg_default_ttl; }
static inline uint16_t route_router_get_frag_size(const route_router_ctx_t *ctx) { return ctx->cfg_frag_size; }

#endif 
