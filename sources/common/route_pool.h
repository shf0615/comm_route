#ifndef ROUTE_POOL_H
#define ROUTE_POOL_H

#include "route_types.h"

typedef struct {
    uint8_t *storage;
    uint32_t *bitmap;       
    uint16_t block_size;
    uint8_t block_count;
} route_pool_t;

void route_pool_init(route_pool_t *pool, uint8_t *storage, uint32_t *bitmap,
                     uint8_t block_count, uint16_t block_size);
uint8_t *route_pool_alloc(route_pool_t *pool);
void route_pool_free(route_pool_t *pool, uint8_t *block);

#endif 
