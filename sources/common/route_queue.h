#ifndef ROUTE_QUEUE_H
#define ROUTE_QUEUE_H

#include "route_types.h"

typedef struct {
    uint8_t *data;
    uint16_t *lengths;
    uint8_t *from_port;
    uint8_t head;
    uint8_t tail;
    uint8_t count;
    uint8_t capacity;
    uint16_t block_size;
} route_recv_queue_t;

void route_queue_init(route_recv_queue_t *q, uint8_t *data, uint16_t *lengths,
                      uint8_t *from_port, uint8_t capacity, uint16_t block_size);
int route_queue_push(route_recv_queue_t *q, const uint8_t *data, uint16_t len, uint8_t from_port);
int route_queue_pop(route_recv_queue_t *q, uint8_t *data, uint16_t *len, uint8_t *from_port);

#endif 
