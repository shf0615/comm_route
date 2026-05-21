#include "route_pool.h"

void route_pool_init(route_pool_t *pool, uint8_t *storage,
                     uint8_t block_count, uint16_t block_size) {
    pool->buffer = storage;
    pool->free_count = block_count;
    for (uint8_t i = 0; i < block_count; i++) {
        pool->free_list[i] = storage + (i * block_size);
    }
}

uint8_t *route_pool_alloc(route_pool_t *pool) {
    if (pool->free_count == 0) {
        return NULL;
    }
    pool->free_count--;
    return pool->free_list[pool->free_count];
}

void route_pool_free(route_pool_t *pool, uint8_t *block) {
    if (block == NULL || pool->free_count >= ROUTE_POOL_BLOCK_COUNT) {
        return;
    }
    pool->free_list[pool->free_count] = block;
    pool->free_count++;
}
