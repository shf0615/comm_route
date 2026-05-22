#ifndef ROUTE_TRANSACTION_H
#define ROUTE_TRANSACTION_H

#include "../common/route_types.h"
#include "../common/route_os.h"

typedef enum {
    TRANS_STATE_IDLE = 0,
    TRANS_STATE_SENDING,
    TRANS_STATE_WAITING,
} trans_state_t;

typedef struct {
    uint8_t state;
    uint8_t dest_id;
    uint8_t expected_seq;
    uint32_t timeout_ms;
    uint32_t timeout_duration;
    void (*callback)(int result, const uint8_t *data, uint16_t len, void *user_data);
    void *user_data;
    void *sync_sem;
    int result;
    uint8_t *resp_buf;
    uint16_t *resp_len;
    uint16_t resp_max_len;
} transaction_t;

typedef struct {
    uint8_t max_concurrent_trans;
    uint32_t default_timeout_ms;
    const route_os_t *os;           

    transaction_t *trans_table;     

    
    int (*lower_send)(void *ctx, uint8_t dest, uint8_t trans_id,
                      uint8_t seq, route_frame_type_t type,
                      const uint8_t *data, uint16_t len);
    void *lower_send_ctx;

    route_stats_t *stats;
} route_transaction_config_t;

typedef struct {
    uint8_t cfg_max_concurrent_trans;
    uint32_t cfg_default_timeout_ms;
    const route_os_t *os;

    transaction_t *trans_table;
    uint8_t seq_counter;
    uint8_t shutdown;
    void *mutex;

    int (*lower_send)(void *ctx, uint8_t dest, uint8_t trans_id,
                      uint8_t seq, route_frame_type_t type,
                      const uint8_t *data, uint16_t len);
    void *lower_send_ctx;

    route_stats_t *stats;
} route_transaction_ctx_t;

int route_transaction_init(route_transaction_ctx_t *ctx, const route_transaction_config_t *cfg);
void route_transaction_deinit(route_transaction_ctx_t *ctx);

int route_transaction_send_sync(route_transaction_ctx_t *ctx, uint8_t dest,
                                const uint8_t *data, uint16_t len,
                                uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms);

int route_transaction_send_async(route_transaction_ctx_t *ctx, uint8_t dest,
                                 const uint8_t *data, uint16_t len,
                                 void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                                 void *user_data);

int route_transaction_reply(route_transaction_ctx_t *ctx, uint8_t dest, uint8_t trans_id,
                            const uint8_t *data, uint16_t len);

void route_transaction_on_response(route_transaction_ctx_t *ctx, uint8_t src,
                                   uint8_t trans_id, uint8_t seq,
                                   const uint8_t *data, uint16_t len);

void route_transaction_tick(route_transaction_ctx_t *ctx, uint32_t now_ms);

#endif 
