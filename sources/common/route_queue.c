#include "route_queue.h"
#include <string.h>

void route_queue_init(route_recv_queue_t *q) {
    q->head = 0;
    q->tail = 0;
    q->count = 0;
}

int route_queue_push(route_recv_queue_t *q, const uint8_t *data, uint16_t len, uint8_t from_port) {
    if (q->count >= ROUTE_RECV_QUEUE_SIZE) {
        return -1;
    }
    if (len > ROUTE_BLOCK_SIZE) {
        return -1;
    }
    memcpy(q->data[q->tail], data, len);
    q->lengths[q->tail] = len;
    q->from_port[q->tail] = from_port;
    q->tail = (q->tail + 1) % ROUTE_RECV_QUEUE_SIZE;
    q->count++;
    return 0;
}

int route_queue_pop(route_recv_queue_t *q, uint8_t *data, uint16_t *len, uint8_t *from_port) {
    if (q->count == 0) {
        return -1;
    }
    *len = q->lengths[q->head];
    *from_port = q->from_port[q->head];
    memcpy(data, q->data[q->head], *len);
    q->head = (q->head + 1) % ROUTE_RECV_QUEUE_SIZE;
    q->count--;
    return 0;
}
