#ifndef ROUTE_TYPES_H
#define ROUTE_TYPES_H

#include <stdint.h>
#include <stddef.h>

// ============ Constants ============

#define ROUTE_BROADCAST_ADDR    0xFF
#define ROUTE_HEADER_SIZE       10

// ============ Error Codes ============

enum {
    ROUTE_OK             =  0,
    ROUTE_ERR_TIMEOUT    = -1,
    ROUTE_ERR_NO_ROUTE   = -2,
    ROUTE_ERR_NO_MEM     = -3,
    ROUTE_ERR_FULL       = -4,
    ROUTE_ERR_REASM      = -5,
    ROUTE_ERR_PARAM      = -6,
    ROUTE_ERR_NO_PORT    = -7,
};

// ============ Frame Types ============

typedef enum {
    ROUTE_TYPE_REQUEST  = 0,
    ROUTE_TYPE_RESPONSE = 1,
    ROUTE_TYPE_ACK      = 2,
} route_frame_type_t;

// ============ Frame Header ============

typedef struct {
    uint8_t src;
    uint8_t dst;
    uint8_t type;
    uint8_t trans_id;
    uint8_t seq;
    uint8_t ttl;
    uint8_t frag_idx;
    uint8_t frag_total;
} route_header_t;

// ============ Port ============

typedef struct {
    uint8_t port_id;
    int (*send)(uint8_t port_id, const uint8_t *buf, uint16_t len);
    void *ctx;
} route_port_t;

// ============ Route Table Entry ============

typedef struct {
    uint8_t dest_id;
    uint8_t port_id;
} route_entry_t;

// ============ Statistics ============

typedef struct {
    uint32_t tx_packets;
    uint32_t rx_packets;
    uint32_t tx_bytes;
    uint32_t rx_bytes;
    uint32_t crc_errors;
    uint32_t drop_no_route;
    uint32_t drop_ttl;
    uint32_t drop_duplicate;
    uint32_t drop_queue_full;
    uint32_t drop_no_mem;
    uint32_t retransmissions;
    uint32_t reasm_timeouts;
    uint32_t trans_timeouts;
} route_stats_t;

#endif // ROUTE_TYPES_H
