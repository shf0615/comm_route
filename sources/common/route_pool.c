#include "route_pool.h"

void route_pool_init(route_pool_t *pool, uint8_t *storage, uint32_t *bitmap,
                     uint8_t block_count, uint16_t block_size) {
    pool->storage = storage;
    pool->bitmap = bitmap;
    pool->block_count = block_count;
    pool->block_size = block_size;
    // Mark all blocks as free (bit = 1)
    uint8_t words = (block_count + 31) / 32;
    for (uint8_t i = 0; i < words; i++) {
        pool->bitmap[i] = 0xFFFFFFFF;
    }
    // Clear bits beyond block_count
    uint8_t remainder = block_count % 32;
    if (remainder > 0) {
        pool->bitmap[words - 1] = (1u << remainder) - 1;
    }
}

uint8_t *route_pool_alloc(route_pool_t *pool) {
    uint8_t words = (pool->block_count + 31) / 32;
    for (uint8_t w = 0; w < words; w++) {
        if (pool->bitmap[w] == 0) continue;
        // Find first set bit
        uint32_t bits = pool->bitmap[w];
        uint8_t bit = 0;
        while (!(bits & (1u << bit))) bit++;
        uint8_t idx = w * 32 + bit;
        if (idx >= pool->block_count) return NULL;
        pool->bitmap[w] &= ~(1u << bit);  // Mark allocated
        return pool->storage + (idx * pool->block_size);
    }
    return NULL;
}

void route_pool_free(route_pool_t *pool, uint8_t *block) {
    if (block == NULL) return;
    if (block < pool->storage) return;
    ptrdiff_t offset = block - pool->storage;
    if (offset % pool->block_size != 0) return;
    uint16_t idx = (uint16_t)(offset / pool->block_size);
    if (idx >= pool->block_count) return;
    uint8_t w = idx / 32;
    uint8_t bit = idx % 32;
    pool->bitmap[w] |= (1u << bit);  // Mark free
}
