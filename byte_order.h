#ifndef BYTE_ORDER_H
#define BYTE_ORDER_H

#include <stdint.h>
#include <endian.h>

struct BigEndian {
    static inline uint8_t htoe(uint8_t v) {
        return v;
    }
    static inline uint8_t etoh(uint8_t v) {
        return v;
    }

    static inline uint16_t htoe(uint16_t v) {
        return htobe16(v);
    }
    static inline uint16_t etoh(uint16_t v) {
        return be16toh(v);
    }

    static inline uint32_t htoe(uint32_t v) {
        return htobe32(v);
    }
    static inline uint32_t etoh(uint32_t v) {
        return be32toh(v);
    }

    static inline uint64_t htoe(uint64_t v) {
        return htobe64(v);
    }
    static inline uint64_t etoh(uint64_t v) {
        return be64toh(v);
    }
};

struct LittleEndian {
    static inline uint8_t htoe(uint8_t v) {
        return v;
    }
    static inline uint8_t etoh(uint8_t v) {
        return v;
    }

    static inline uint16_t htoe(uint16_t v) {
        return htole16(v);
    }
    static inline uint16_t etoh(uint16_t v) {
        return le16toh(v);
    }

    static inline uint32_t htoe(uint32_t v) {
        return htole32(v);
    }
    static inline uint32_t etoh(uint32_t v) {
        return le32toh(v);
    }

    static inline uint64_t htoe(uint64_t v) {
        return htole64(v);
    }
    static inline uint64_t etoh(uint64_t v) {
        return le64toh(v);
    }
};

#endif
