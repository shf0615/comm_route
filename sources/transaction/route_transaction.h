#ifndef ROUTE_TRANSACTION_H
#define ROUTE_TRANSACTION_H

#include "../common/route_types.h"

// 设置下层发送函数（对接 route_frag_send）
typedef int (*route_transaction_lower_send_t)(route_instance_t *inst, uint8_t dest,
    uint8_t trans_id, uint8_t seq, const uint8_t *data, uint16_t len);
void route_transaction_set_lower_send(route_instance_t *inst, route_transaction_lower_send_t send_fn);

// 设置 reply 下层发送（对接 route_router_send 或 frag）
void route_transaction_set_reply_send(route_instance_t *inst, route_lower_send_t send_fn);

int route_transaction_send_async(route_instance_t *inst, uint8_t dest,
                                 const uint8_t *data, uint16_t len,
                                 void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                                 void *user_data);

int route_transaction_send_sync(route_instance_t *inst, uint8_t dest,
                                const uint8_t *data, uint16_t len,
                                uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms);

int route_transaction_reply(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                            const uint8_t *data, uint16_t len);

void route_transaction_on_response(route_instance_t *inst, uint8_t src,
                                   uint8_t trans_id, const uint8_t *data, uint16_t len);

void route_transaction_tick(route_instance_t *inst, uint32_t now_ms);

#endif // ROUTE_TRANSACTION_H
