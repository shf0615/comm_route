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
    uint8_t type;       // route_frame_type_t value, stored as uint8_t for wire compatibility
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

// ============ Route Table ============

typedef struct {
    uint8_t dest_id;
    uint8_t port_id;
} route_entry_t;

// ============ Seen Table (dedup) ============

typedef struct {
    uint8_t src_id;
    uint8_t seq;
    uint8_t trans_id;
    uint8_t valid;
    uint32_t timestamp_ms;
} route_seen_entry_t;

// ============ Reassembly ============

typedef struct {
    uint8_t src_id;
    uint8_t seq;
    uint8_t frag_total;
    uint8_t received_count;
    uint32_t start_ms;
    uint8_t **fragments;    // array of pointers, size = max_frags_per_msg
    uint8_t *frag_lens;     // array, size = max_frags_per_msg
    uint8_t active;
} route_reasm_ctx_t;

// ============ Reliability Strategy ============

typedef struct route_instance route_instance_t;

typedef struct {
    int (*on_send)(route_instance_t *inst, uint8_t dest, uint8_t seq, uint8_t frag_idx,
                   uint8_t frag_total, uint8_t type, uint8_t trans_id, const uint8_t *data, uint16_t len);
    void (*on_recv_ack)(route_instance_t *inst, uint8_t src, uint8_t seq, uint8_t frag_idx);
    void (*on_tick)(route_instance_t *inst, uint32_t now_ms);
} reliability_strategy_t;

// ============ Pending ACK (for reliability) ============

typedef struct {
    uint8_t active;
    uint8_t dest_id;
    uint8_t seq;
    uint8_t frag_idx;
    uint8_t frag_total;
    uint8_t trans_id;
    uint8_t type;
    uint8_t retry_count;
    uint32_t next_retry_ms;
    uint8_t *data;          // points into pending_ack_data_storage, size = frag_size
    uint16_t len;
} route_pending_ack_t;

// ============ Transaction ============

typedef enum {
    TRANS_STATE_IDLE = 0,
    TRANS_STATE_SENDING,
    TRANS_STATE_WAITING,
} trans_state_t;

typedef struct {
    uint8_t state;
    uint8_t dest_id;
    uint32_t timeout_ms;
    uint32_t timeout_duration;
    void (*callback)(int result, const uint8_t *data, uint16_t len, void *user_data);
    void *user_data;
    void *sync_sem;
    int result;
    uint8_t *resp_buf;
    uint16_t *resp_len;
    uint16_t resp_max_len;
} transaction_t;

// ============ OS Abstraction ============

typedef struct {
    void *(*mutex_create)(void);
    void (*mutex_lock)(void *mutex);
    void (*mutex_unlock)(void *mutex);
    void (*mutex_destroy)(void *mutex);
    void *(*sem_create)(void);
    int (*sem_wait)(void *sem, uint32_t timeout_ms);
    void (*sem_post)(void *sem);
    void (*sem_destroy)(void *sem);
} route_os_t;

// ============ Memory Pool ============

typedef struct {
    uint8_t **free_list;    // array of pointers, size = pool_block_count
    uint8_t free_count;
    uint8_t max_count;      // capacity
} route_pool_t;

// ============ Receive Queue ============

typedef struct {
    uint8_t *data;          // flat buffer: recv_queue_size * block_size bytes
    uint16_t *lengths;      // array, size = recv_queue_size
    uint8_t *from_port;     // array, size = recv_queue_size
    uint8_t head;
    uint8_t tail;
    uint8_t count;
    uint8_t capacity;       // recv_queue_size
    uint16_t block_size;    // HEADER_SIZE + frag_size
} route_recv_queue_t;

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

// ============ Layer Callback Types ============

typedef void (*route_router_deliver_cb_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len);

typedef void (*route_frag_complete_cb_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *data, uint16_t len);

typedef int (*route_lower_send_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len);

// ============ Config ============

typedef struct {
    const route_os_t *os;

    // Capacity parameters (all required, no defaults)
    uint8_t max_ports;
    uint8_t max_nodes;
    uint8_t max_concurrent_trans;
    uint8_t max_reasm_slots;
    uint8_t pool_block_count;
    uint8_t seen_table_size;
    uint8_t recv_queue_size;
    uint16_t max_payload;
    uint16_t frag_size;
    uint8_t max_frags_per_msg;      // typically (max_payload / frag_size) + 1

    // Timing parameters
    uint32_t ack_timeout_ms;
    uint8_t ack_retry_max;
    uint8_t default_ttl;
    uint32_t seen_expire_ms;
    uint32_t reasm_timeout_ms;

    // External storage (all required, caller-allocated)
    route_port_t *ports;                    // [max_ports]
    route_entry_t *route_table;             // [max_nodes]
    route_seen_entry_t *seen_table;         // [seen_table_size]
    route_reasm_ctx_t *reasm_slots;         // [max_reasm_slots]
    uint8_t *reasm_buf;                     // [max_payload]

    // Reassembly fragment pointer arrays: max_reasm_slots * max_frags_per_msg pointers
    uint8_t **reasm_frag_ptrs;              // [max_reasm_slots * max_frags_per_msg]
    uint8_t *reasm_frag_lens;              // [max_reasm_slots * max_frags_per_msg]

    route_pending_ack_t *pending_acks;      // [max_concurrent_trans]
    uint8_t *pending_ack_data;              // [max_concurrent_trans * frag_size]
    transaction_t *trans_table;             // [max_concurrent_trans]

    // Memory pool storage
    uint8_t **pool_free_list;               // [pool_block_count]
    uint8_t *pool_storage;                  // [pool_block_count * (ROUTE_HEADER_SIZE + frag_size)]

    // Receive queue storage
    uint8_t *recv_queue_data;               // [recv_queue_size * (ROUTE_HEADER_SIZE + frag_size)]
    uint16_t *recv_queue_lengths;           // [recv_queue_size]
    uint8_t *recv_queue_from_port;          // [recv_queue_size]
} route_config_t;

// ============ Instance ============

struct route_instance {
    uint8_t node_id;
    uint8_t seq_counter;
    uint8_t bcast_seq_counter;

    const route_os_t *os;

    // Effective capacities
    uint8_t cfg_max_ports;
    uint8_t cfg_max_nodes;
    uint8_t cfg_max_concurrent_trans;
    uint8_t cfg_max_reasm_slots;
    uint8_t cfg_pool_block_count;
    uint8_t cfg_seen_table_size;
    uint8_t cfg_recv_queue_size;
    uint16_t cfg_max_payload;
    uint16_t cfg_frag_size;
    uint16_t cfg_block_size;        // ROUTE_HEADER_SIZE + cfg_frag_size
    uint8_t cfg_max_frags_per_msg;
    uint32_t cfg_ack_timeout_ms;
    uint8_t cfg_ack_retry_max;
    uint8_t cfg_default_ttl;
    uint32_t cfg_seen_expire_ms;
    uint32_t cfg_reasm_timeout_ms;

    // Port layer (external storage)
    route_port_t *ports;
    uint8_t port_count;

    // Routing layer (external storage)
    route_entry_t *route_table;
    uint8_t route_count;
    route_seen_entry_t *seen_table;
    uint8_t seen_index;
    uint32_t current_ms;

    // Fragmentation/reassembly (external storage)
    route_reasm_ctx_t *reasm_slots;
    uint8_t *reasm_buf;

    // Reliability (external storage)
    const reliability_strategy_t *reliability;
    route_pending_ack_t *pending_acks;

    // Transaction (external storage)
    transaction_t *trans_table;
    void *trans_mutex;

    // Receive queue
    route_recv_queue_t recv_queue;
    void (*on_recv_cb)(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len);

    // Layer callbacks
    route_router_deliver_cb_t router_deliver_cb;
    route_frag_complete_cb_t frag_complete_cb;
    route_lower_send_t frag_lower_send;
    route_lower_send_t reliability_lower_send;
    int (*transaction_lower_send)(route_instance_t *inst, uint8_t dest,
        uint8_t trans_id, uint8_t seq, const uint8_t *data, uint16_t len);

    // Memory pool
    route_pool_t pool;

    // Statistics
    route_stats_t stats;
};

#endif // ROUTE_TYPES_H
