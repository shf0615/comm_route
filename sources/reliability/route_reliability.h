#ifndef ROUTE_RELIABILITY_H
#define ROUTE_RELIABILITY_H

#include "../common/route_types.h"

// 设置下层发送函数（用于发送 ACK 和重传）
void route_reliability_set_lower_send(route_instance_t *inst, route_lower_send_t send_fn);

const reliability_strategy_t *route_reliability_e2e_strategy(void);
int route_reliability_send_ack(route_instance_t *inst, uint8_t dest, uint8_t seq);

#endif // ROUTE_RELIABILITY_H
