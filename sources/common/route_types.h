#ifndef ROUTE_TYPES_H
#define ROUTE_TYPES_H

#include <stdint.h>
#include <stddef.h>

// ============ Configuration (override in route_config.h if needed) ============

#ifndef ROUTE_MAX_INSTANCES
#define ROUTE_MAX_INSTANCES               4
#endif
#ifndef ROUTE_MAX_NODES
#define ROUTE_MAX_NODES                   32
#endif
#ifndef ROUTE_MAX_PORTS
#define ROUTE_MAX_PORTS                   4
#endif
#ifndef ROUTE_MAX_CONCURRENT_TRANSACTIONS
#define ROUTE_MAX_CONCURRENT_TRANSACTIONS 8
#endif
#ifndef ROUTE_MAX_PAYLOAD
#define ROUTE_MAX_PAYLOAD                 256
#endif
#ifndef ROUTE_FRAG_SIZE
#define ROUTE_FRAG_SIZE                   32
#endif
#ifndef ROUTE_MAX_REASM_SLOTS
#define ROUTE_MAX_REASM_SLOTS             4
#endif
#ifndef ROUTE_POOL_BLOCK_COUNT
#define ROUTE_POOL_BLOCK_COUNT            16
#endif
#ifndef ROUTE_ACK_TIMEOUT_MS
#define ROUTE_ACK_TIMEOUT_MS              1000
#endif
#ifndef ROUTE_ACK_RETRY_MAX
#define ROUTE_ACK_RETRY_MAX               3
#endif
#ifndef ROUTE_DEFAULT_TTL
#define ROUTE_DEFAULT_TTL                 8
#endif
#ifndef ROUTE_SEEN_TABLE_SIZE
#define ROUTE_SEEN_TABLE_SIZE             16
#endif
#ifndef ROUTE_SEEN_EXPIRE_MS
#define ROUTE_SEEN_EXPIRE_MS              10000
#endif
#ifndef ROUTE_REASM_TIMEOUT_MS
#define ROUTE_REASM_TIMEOUT_MS            5000
#endif
#ifndef ROUTE_RECV_QUEUE_SIZE
#define ROUTE_RECV_QUEUE_SIZE             16
#endif

// ============ Constants ============

#define ROUTE_BROADCAST_ADDR    0xFF
#define ROUTE_HEADER_SIZE       10
#define ROUTE_BLOCK_SIZE        (ROUTE_HEADER_SIZE + ROUTE_FRAG_SIZE)

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
    uint8_t *fragments[ROUTE_MAX_PAYLOAD / ROUTE_FRAG_SIZE + 1];
    uint8_t frag_lens[ROUTE_MAX_PAYLOAD / ROUTE_FRAG_SIZE + 1];
    uint8_t active;
} route_reasm_ctx_t;

// ============ Reliability Strategy ============

typedef struct route_instance route_instance_t;

typedef struct {
    int (*on_send)(route_instance_t *inst, uint8_t dest, uint8_t seq,
                   uint8_t trans_id, const uint8_t *data, uint16_t len);
    void (*on_recv_ack)(route_instance_t *inst, uint8_t src, uint8_t seq);
    void (*on_tick)(route_instance_t *inst, uint32_t now_ms);
} reliability_strategy_t;

// ============ Pending ACK (for reliability) ============

typedef struct {
    uint8_t active;
    uint8_t dest_id;
    uint8_t seq;
    uint8_t trans_id;
    uint8_t retry_count;
    uint32_t next_retry_ms;
    uint8_t data[ROUTE_FRAG_SIZE];  // copy of payload (max one fragment)
    uint16_t len;
} route_pending_ack_t;

// ============ Transaction ============

typedef enum {
    TRANS_STATE_IDLE = 0,
    TRANS_STATE_WAITING,
} trans_state_t;

typedef struct {
    uint8_t state;
    uint8_t dest_id;
    uint32_t timeout_ms;       // absolute deadline (0 = not yet set, filled on first tick)
    uint32_t timeout_duration; // relative timeout duration
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

// ============ Config ============

typedef struct {
    const route_os_t *os;
} route_config_t;

// ============ Memory Pool ============

typedef struct {
    uint8_t *free_list[ROUTE_POOL_BLOCK_COUNT];
    uint8_t free_count;
} route_pool_t;

// ============ Receive Queue ============

typedef struct {
    uint8_t data[ROUTE_RECV_QUEUE_SIZE][ROUTE_BLOCK_SIZE];
    uint16_t lengths[ROUTE_RECV_QUEUE_SIZE];
    uint8_t from_port[ROUTE_RECV_QUEUE_SIZE];
    uint8_t head;
    uint8_t tail;
    uint8_t count;
} route_recv_queue_t;

// ============ Layer Callback Types ============

// Router 层：帧送达本机时调用
typedef void (*route_router_deliver_cb_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len);

// Frag 层：重组完成时调用
typedef void (*route_frag_complete_cb_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *data, uint16_t len);

// 通用下层发送接口
typedef int (*route_lower_send_t)(route_instance_t *inst,
    const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len);

// ============ Instance ============

struct route_instance {
    uint8_t node_id;
    uint8_t seq_counter;

    const route_os_t *os;

    route_port_t ports[ROUTE_MAX_PORTS];
    uint8_t port_count;

    route_entry_t route_table[ROUTE_MAX_NODES];
    uint8_t route_count;
    route_seen_entry_t seen_table[ROUTE_SEEN_TABLE_SIZE];
    uint8_t seen_index;
    uint32_t current_ms;  // updated by route_tick

    route_reasm_ctx_t reasm_slots[ROUTE_MAX_REASM_SLOTS];

    const reliability_strategy_t *reliability;
    route_pending_ack_t pending_acks[ROUTE_MAX_CONCURRENT_TRANSACTIONS];

    transaction_t trans_table[ROUTE_MAX_CONCURRENT_TRANSACTIONS];
    void *trans_mutex;

    route_recv_queue_t recv_queue;
    void (*on_recv_cb)(route_instance_t *inst, uint8_t src, uint8_t trans_id,
                       const uint8_t *data, uint16_t len);

    // 层间回调
    route_router_deliver_cb_t router_deliver_cb;   // router -> 上层
    route_frag_complete_cb_t frag_complete_cb;     // frag -> 上层
    route_lower_send_t frag_lower_send;           // frag 发送用（注入 router_send）
    route_lower_send_t reliability_lower_send;    // reliability 重传用
    int (*transaction_lower_send)(route_instance_t *inst, uint8_t dest,
        uint8_t trans_id, uint8_t seq, const uint8_t *data, uint16_t len);  // transaction -> frag

    route_pool_t pool;
    uint8_t pool_storage[ROUTE_POOL_BLOCK_COUNT * ROUTE_BLOCK_SIZE];

    // Reassembly output buffer (avoids large stack allocation)
    uint8_t reasm_buf[ROUTE_MAX_PAYLOAD];
};

#endif // ROUTE_TYPES_H
