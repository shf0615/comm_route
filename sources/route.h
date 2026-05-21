#ifndef ROUTE_H
#define ROUTE_H

#include "common/route_types.h"

// === Lifecycle ===
route_instance_t *route_create(uint8_t node_id, const route_config_t *config);
void route_destroy(route_instance_t *inst);

// === Configuration ===
int route_port_register(route_instance_t *inst, const route_port_t *port);
int route_table_set(route_instance_t *inst, const route_entry_t *entries, uint8_t count);
void route_reliability_set(route_instance_t *inst, const reliability_strategy_t *strategy);
void route_on_recv(route_instance_t *inst,
                   void (*cb)(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                              const uint8_t *data, uint16_t len));

// === Send ===
int route_send_sync(route_instance_t *inst, uint8_t dest,
                    const uint8_t *data, uint16_t len,
                    uint8_t *resp_buf, uint16_t *resp_len, uint32_t timeout_ms);

int route_send_async(route_instance_t *inst, uint8_t dest,
                     const uint8_t *data, uint16_t len,
                     void (*cb)(int result, const uint8_t *data, uint16_t len, void *user_data),
                     void *user_data);

int route_broadcast(route_instance_t *inst, const uint8_t *data, uint16_t len,
                    void (*cb)(uint8_t src_id, int result, void *user_data),
                    void *user_data);

int route_reply(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                const uint8_t *data, uint16_t len);

// === Driver ===
void route_tick(route_instance_t *inst, uint32_t now_ms);
void route_poll(route_instance_t *inst);

#endif // ROUTE_H
