#ifndef ROUTE_FRAG_H
#define ROUTE_FRAG_H

#include "../common/route_types.h"

// 设置下层发送函数（注入 route_router_send 或其他）
void route_frag_set_lower_send(route_instance_t *inst, route_lower_send_t send_fn);

// 设置重组完成回调
void route_frag_set_complete_cb(route_instance_t *inst, route_frag_complete_cb_t cb);

// 分片发送
int route_frag_send(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                    uint8_t seq, const uint8_t *data, uint16_t len);

// 分片发送（指定帧类型）
int route_frag_send_typed(route_instance_t *inst, uint8_t dest, uint8_t trans_id,
                          uint8_t seq, route_frame_type_t type,
                          const uint8_t *data, uint16_t len);

// 接收分片（由上层或 router deliver 回调调用）
// Returns: 1=完成, 0=等待更多分片, <0=错误
int route_frag_recv(route_instance_t *inst, const route_header_t *hdr,
                    const uint8_t *payload, uint16_t payload_len,
                    uint8_t *out_buf, uint16_t *out_len, route_header_t *out_hdr);

// 作为 router_deliver_cb 的输入适配器（直接注册到 router）
void route_frag_input(route_instance_t *inst, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len);

void route_frag_tick(route_instance_t *inst, uint32_t now_ms);

#endif // ROUTE_FRAG_H
