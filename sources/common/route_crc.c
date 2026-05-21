#include "route_crc.h"

uint16_t route_crc16(const uint8_t *data, uint16_t len) {
    uint16_t crc = 0xFFFF;
    if (data == NULL || len == 0) {
        return crc;
    }
    for (uint16_t i = 0; i < len; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (uint8_t j = 0; j < 8; j++) {
            if (crc & 0x8000) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}
