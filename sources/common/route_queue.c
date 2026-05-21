#include "route_queue.h"
#include <string.h>

void route_queue_init(route_recv_queue_t *q, uint8_t *data, uint16_t *lengths,
                      uint8_t *from_port, uint8_t capacity, uint16_t block_size) {
    q->data = data;
    q->lengths = lengths;
    q->from_port = from_port;
    q->capacity = capacity;
    q->block_size = block_size;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
}

/**
 * route_queue_push - 将数据压入接收队列。
 *
 * 线程安全说明：本函数不包含内部锁保护。
 * 调用者必须在外部持有锁的情况下调用 push，或保证单生产者访问。
 * pop 同理，需在锁保护下调用或保证单消费者访问。
 */
int route_queue_push(route_recv_queue_t *q, const uint8_t *data, uint16_t len, uint8_t from_port) {
    if (q->count >= q->capacity) {
        return -1;
    }
    if (len > q->block_size) {
        return -1;
    }
    memcpy(q->data + (q->tail * q->block_size), data, len);
    q->lengths[q->tail] = len;
    q->from_port[q->tail] = from_port;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    return 0;
}

int route_queue_pop(route_recv_queue_t *q, uint8_t *data, uint16_t *len, uint8_t *from_port) {
    if (q->count == 0) {
        return -1;
    }
    *len = q->lengths[q->head];
    *from_port = q->from_port[q->head];
    memcpy(data, q->data + (q->head * q->block_size), *len);
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    return 0;
}
