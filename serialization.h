#ifndef SERIALIZATION_H
#define SERIALIZATION_H

#include <stdint.h>
#include <string>

template<typename Endian>
class Serializer {
public:
    explicit Serializer(char *buf) : buf_(buf) {
    }

    char *buf() { return buf_; }

    template<typename T>
    Serializer& append(T v) {
        *static_cast<T *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;
    }

/*
    Serializer& append(uint8_t v) {
        *static_cast<uint8_t *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;
    }

    Serializer& (uint16_t v) {
        *static_cast<uint16_t *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;        
    }

    Serializer& (uint32_t v) {
        *static_cast<uint32_t *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;        
    }

    Serializer& (uint64_t v) {
        *static_cast<uint64_t *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;        
    }
*/
    template<typename LengthType>
    Serializer& append(const std::string& s) {
    }
private:
    char *buf_;
};


#endif
