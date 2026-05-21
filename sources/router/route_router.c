#include "route_router.h"
#include "../common/route_crc.h"
#include "../common/route_queue.h"
#include <string.h>

static void route_router_build_frame(const route_header_t *hdr, const uint8_t *payload,
                              uint16_t payload_len, uint8_t *frame, uint16_t *frame_len) {
    frame[0] = hdr->src;
    frame[1] = hdr->dst;
    frame[2] = (uint8_t)hdr->type;
    frame[3] = hdr->trans_id;
    frame[4] = hdr->seq;
    frame[5] = hdr->ttl;
    frame[6] = hdr->frag_idx;
    frame[7] = hdr->frag_total;
    if (payload_len > 0 && payload != NULL) {
        memcpy(&frame[8], payload, payload_len);
    }
    uint16_t crc = route_crc16(frame, 8 + payload_len);
    frame[8 + payload_len] = (crc >> 8) & 0xFF;
    frame[9 + payload_len] = crc & 0xFF;
    *frame_len = 10 + payload_len;
}

static int route_router_parse_frame(const uint8_t *frame, uint16_t frame_len,
                             route_header_t *hdr, const uint8_t **payload, uint16_t *payload_len) {
    if (frame_len < ROUTE_HEADER_SIZE) {
        return ROUTE_ERR_PARAM;
    }
    uint16_t plen = frame_len - ROUTE_HEADER_SIZE;
    uint16_t crc_calc = route_crc16(frame, frame_len - 2);
    uint16_t crc_recv = ((uint16_t)frame[frame_len - 2] << 8) | frame[frame_len - 1];
    if (crc_calc != crc_recv) {
        return ROUTE_ERR_PARAM;
    }
    hdr->src = frame[0];
    hdr->dst = frame[1];
    hdr->type = frame[2];
    hdr->trans_id = frame[3];
    hdr->seq = frame[4];
    hdr->ttl = frame[5];
    hdr->frag_idx = frame[6];
    hdr->frag_total = frame[7];
    *payload = (plen > 0) ? &frame[8] : NULL;
    *payload_len = plen;
    return ROUTE_OK;
}

static int route_seen_check_and_add(route_instance_t *inst, uint8_t src_id, uint8_t seq,
                                    uint8_t trans_id, uint8_t type) {
    uint32_t now = inst->current_ms;

    for (uint8_t i = 0; i < inst->cfg_seen_table_size; i++) {
        if (!inst->seen_table[i].valid) continue;
        if ((int32_t)(now - inst->seen_table[i].timestamp_ms) >= (int32_t)inst->cfg_seen_expire_ms) {
            inst->seen_table[i].valid = 0;
            continue;
        }
        if (inst->seen_table[i].src_id == src_id &&
            inst->seen_table[i].seq == seq &&
            inst->seen_table[i].trans_id == trans_id) {
            return 1;
        }
    }
    if (type == ROUTE_TYPE_REQUEST) {
        inst->seen_table[inst->seen_index].src_id = src_id;
        inst->seen_table[inst->seen_index].seq = seq;
        inst->seen_table[inst->seen_index].trans_id = trans_id;
        inst->seen_table[inst->seen_index].valid = 1;
        inst->seen_table[inst->seen_index].timestamp_ms = now;
        inst->seen_index = (inst->seen_index + 1) % inst->cfg_seen_table_size;
    }
    return 0;
}

static const route_entry_t *route_lookup(route_instance_t *inst, uint8_t dest_id) {
    for (uint8_t i = 0; i < inst->route_count; i++) {
        if (inst->route_table[i].dest_id == dest_id) {
            return &inst->route_table[i];
        }
    }
    return NULL;
}

static int route_send_to_port(route_instance_t *inst, uint8_t port_id,
                              const uint8_t *frame, uint16_t frame_len) {
    for (uint8_t i = 0; i < inst->port_count; i++) {
        if (inst->ports[i].port_id == port_id) {
            return inst->ports[i].send(port_id, frame, frame_len);
        }
    }
    return ROUTE_ERR_NO_PORT;
}

static int route_router_handle_frame(route_instance_t *inst, const uint8_t *frame,
                              uint16_t frame_len, uint8_t from_port,
                              route_header_t *out_hdr, const uint8_t **out_payload,
                              uint16_t *out_payload_len) {
    route_header_t hdr;
    const uint8_t *payload;
    uint16_t payload_len;

    int rc = route_router_parse_frame(frame, frame_len, &hdr, &payload, &payload_len);
    if (rc != ROUTE_OK) {
        inst->stats.crc_errors++;
        return rc;
    }

    if (hdr.ttl == 0) {
        inst->stats.drop_ttl++;
        return ROUTE_ERR_TIMEOUT;
    }

    if (hdr.type == ROUTE_TYPE_REQUEST) {
        if (route_seen_check_and_add(inst, hdr.src, hdr.seq, hdr.trans_id, hdr.type)) {
            inst->stats.drop_duplicate++;
            return 0;
        }
    }

    if (hdr.dst == ROUTE_BROADCAST_ADDR) {
        hdr.ttl--;
        uint8_t fwd_frame[inst->cfg_block_size];
        uint16_t fwd_len;
        route_router_build_frame(&hdr, payload, payload_len, fwd_frame, &fwd_len);
        for (uint8_t i = 0; i < inst->port_count; i++) {
            if (inst->ports[i].port_id != from_port) {
                if (inst->ports[i].send(inst->ports[i].port_id, fwd_frame, fwd_len) == ROUTE_OK) {
                    inst->stats.tx_packets++;
                    inst->stats.tx_bytes += fwd_len;
                }
            }
        }
        hdr.ttl++;
        *out_hdr = hdr;
        *out_payload = payload;
        *out_payload_len = payload_len;
        inst->stats.rx_packets++;
        inst->stats.rx_bytes += ROUTE_HEADER_SIZE + payload_len;
        return 1;
    }

    if (hdr.dst == inst->node_id) {
        *out_hdr = hdr;
        *out_payload = payload;
        *out_payload_len = payload_len;
        inst->stats.rx_packets++;
        inst->stats.rx_bytes += ROUTE_HEADER_SIZE + payload_len;
        return 1;
    }

    // 转发
    const route_entry_t *entry = route_lookup(inst, hdr.dst);
    if (entry == NULL) {
        inst->stats.drop_no_route++;
        return ROUTE_ERR_NO_ROUTE;
    }

    hdr.ttl--;
    uint8_t fwd_frame[inst->cfg_block_size];
    uint16_t fwd_len;
    route_router_build_frame(&hdr, payload, payload_len, fwd_frame, &fwd_len);
    int fwd_rc = route_send_to_port(inst, entry->port_id, fwd_frame, fwd_len);
    if (fwd_rc == ROUTE_OK) {
        inst->stats.tx_packets++;
        inst->stats.tx_bytes += fwd_len;
    }
    return fwd_rc;
}

int route_router_send(route_instance_t *inst, const route_header_t *hdr,
                      const uint8_t *payload, uint16_t payload_len) {
    uint8_t frame[inst->cfg_block_size];
    uint16_t frame_len;
    route_router_build_frame(hdr, payload, payload_len, frame, &frame_len);

    if (hdr->dst == ROUTE_BROADCAST_ADDR) {
        int last_err = ROUTE_OK;
        for (uint8_t i = 0; i < inst->port_count; i++) {
            int rc = inst->ports[i].send(inst->ports[i].port_id, frame, frame_len);
            if (rc != ROUTE_OK) last_err = rc;
        }
        return last_err;
    }

    const route_entry_t *entry = route_lookup(inst, hdr->dst);
    if (entry == NULL) {
        inst->stats.drop_no_route++;
        return ROUTE_ERR_NO_ROUTE;
    }
    return route_send_to_port(inst, entry->port_id, frame, frame_len);
}

void route_router_set_deliver_cb(route_instance_t *inst, route_router_deliver_cb_t cb) {
    inst->router_deliver_cb = cb;
}

void route_router_poll(route_instance_t *inst) {
    uint8_t frame[inst->cfg_block_size];
    uint16_t frame_len;
    uint8_t from_port;

    while (route_queue_pop(&inst->recv_queue, frame, &frame_len, &from_port) == 0) {
        route_header_t hdr;
        const uint8_t *payload;
        uint16_t payload_len;

        int delivered = route_router_handle_frame(inst, frame, frame_len, from_port,
                                                  &hdr, &payload, &payload_len);
        if (delivered == 1 && inst->router_deliver_cb) {
            uint8_t payload_copy[inst->cfg_frag_size];
            if (payload_len > 0 && payload != NULL) {
                memcpy(payload_copy, payload, payload_len);
            }
            inst->router_deliver_cb(inst, &hdr, payload_len > 0 ? payload_copy : NULL, payload_len);
        }
    }
}
