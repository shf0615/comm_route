#ifndef ROUTE_POOL_H
#define ROUTE_POOL_H

#include "route_types.h"

// Bitmap-based memory pool — saves pointer array overhead
typedef struct {
    uint8_t *storage;
    uint32_t *bitmap;       // 1 bit per block: 1=free, 0=allocated
    uint16_t block_size;
    uint8_t block_count;
} route_pool_t;

// bitmap: caller provides uint32_t[(block_count + 31) / 32]
void route_pool_init(route_pool_t *pool, uint8_t *storage, uint32_t *bitmap,
                     uint8_t block_count, uint16_t block_size);
uint8_t *route_pool_alloc(route_pool_t *pool);
void route_pool_free(route_pool_t *pool, uint8_t *block);

#endif // ROUTE_POOL_H
