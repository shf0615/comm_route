#ifndef ROUTE_QUEUE_H
#define ROUTE_QUEUE_H

#include "route_types.h"

void route_queue_init(route_recv_queue_t *q);
int route_queue_push(route_recv_queue_t *q, const uint8_t *data, uint16_t len, uint8_t from_port);
int route_queue_pop(route_recv_queue_t *q, uint8_t *data, uint16_t *len, uint8_t *from_port);

#endif // ROUTE_QUEUE_H
