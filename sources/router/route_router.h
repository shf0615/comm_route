#ifndef ROUTE_ROUTER_H
#define ROUTE_ROUTER_H

#include "../common/route_types.h"

int route_router_send(route_instance_t *inst, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len);

// 独立 poll：从 recv_queue 取帧处理，送达的帧通过 router_deliver_cb 上传
void route_router_poll(route_instance_t *inst);

// 设置 deliver 回调（帧送达本机时调用）
void route_router_set_deliver_cb(route_instance_t *inst, route_router_deliver_cb_t cb);

#endif // ROUTE_ROUTER_H
