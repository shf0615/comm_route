#ifndef ROUTE_ROUTER_H
#define ROUTE_ROUTER_H

#include "../common/route_types.h"
#include "../common/route_queue.h"

// ============ Codec (可替换帧编解码) ============

typedef struct {
    // 编码帧，返回 frame 总长度
    uint16_t (*encode)(const route_header_t *hdr, const uint8_t *payload,
                       uint16_t payload_len, uint8_t *frame);
    // 解码帧，成功返回 ROUTE_OK
    int (*decode)(const uint8_t *frame, uint16_t frame_len,
                  route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len);
    uint16_t overhead;  // header + checksum 字节数
} route_codec_t;

// ============ Seen Entry (转发去重) ============

typedef struct {
    uint8_t src_id;
    uint8_t seq;
    uint8_t valid;
    uint32_t timestamp_ms;
} route_seen_entry_t;

// ============ Router Config ============

typedef struct {
    uint8_t node_id;
    uint8_t default_ttl;
    uint16_t frag_size;         // 用于计算 block_size

    // 可选 codec（NULL = 使用内置默认）
    const route_codec_t *codec;

    // 容量
    uint8_t max_ports;
    uint8_t max_nodes;
    uint8_t seen_table_size;
    uint32_t seen_expire_ms;
    uint8_t recv_queue_size;

    // 外部存储
    route_port_t *ports;
    route_entry_t *route_table;
    route_seen_entry_t *seen_table;

    // 接收队列存储
    uint8_t *recv_queue_data;
    uint16_t *recv_queue_lengths;
    uint8_t *recv_queue_from_port;

    // 工作缓冲区（消除栈上 VLA）
    uint8_t *tx_frame_buf;      // [overhead + frag_size]，发送/转发编码用
    uint8_t *rx_frame_buf;      // [overhead + frag_size]，poll 取帧用

    route_stats_t *stats;       // 共享统计（NULL = 不统计）
} route_router_config_t;

// ============ Router Context ============

typedef struct {
    uint8_t node_id;
    uint8_t cfg_default_ttl;
    uint16_t cfg_block_size;    // overhead + frag_size
    uint16_t cfg_frag_size;

    route_port_t *ports;
    uint8_t port_count;
    uint8_t cfg_max_ports;

    route_entry_t *route_table;
    uint8_t route_count;
    uint8_t cfg_max_nodes;

    route_seen_entry_t *seen_table;
    uint8_t seen_index;
    uint8_t cfg_seen_table_size;
    uint32_t cfg_seen_expire_ms;
    uint32_t current_ms;

    route_recv_queue_t recv_queue;

    // 工作缓冲区
    uint8_t *tx_frame_buf;      // 发送/转发编码用
    uint8_t *rx_frame_buf;      // poll 取帧用

    const route_codec_t *codec;

    // 上行回调：帧送达本机时调用
    void (*deliver_cb)(void *ctx, const route_header_t *hdr,
                       const uint8_t *payload, uint16_t payload_len);
    void *deliver_ctx;

    route_stats_t *stats;
} route_router_ctx_t;

// ============ API ============

int route_router_init(route_router_ctx_t *ctx, const route_router_config_t *cfg);
void route_router_deinit(route_router_ctx_t *ctx);

int route_router_port_register(route_router_ctx_t *ctx, const route_port_t *port);
int route_router_table_set(route_router_ctx_t *ctx, const route_entry_t *entries, uint8_t count);

void route_router_set_deliver_cb(route_router_ctx_t *ctx,
    void (*cb)(void *ctx, const route_header_t *hdr, const uint8_t *payload, uint16_t payload_len),
    void *cb_ctx);

int route_router_send(route_router_ctx_t *ctx, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len);

// 输入一帧原始数据（从 ISR 或接收线程调用）
int route_router_input(route_router_ctx_t *ctx, const uint8_t *data, uint16_t len, uint8_t port_id);

// 处理接收队列（在主循环中调用）
void route_router_poll(route_router_ctx_t *ctx);

// 更新时间（驱动 seen_table 过期）
void route_router_tick(route_router_ctx_t *ctx, uint32_t now_ms);

#endif // ROUTE_ROUTER_H
