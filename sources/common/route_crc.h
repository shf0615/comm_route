#ifndef ROUTE_CRC_H
#define ROUTE_CRC_H

#include <stdint.h>
#include <stddef.h>

uint16_t route_crc16(const uint8_t *data, uint16_t len);

#endif // ROUTE_CRC_H
