#ifndef SERIALIZATION_H
#define SERIALIZATION_H

#include <stdint.h>
#include <string>
#include <cstring>
#include <algorithm>
#include <type_traits>

template<typename Endian>
class Serializer {
public:
    explicit Serializer(char *buf) : buf_(buf) {
    }

    char *buf() const { return buf_; }

    template<typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
    Serializer& serialize(T v) {
        *reinterpret_cast<T *>(buf_) = Endian::htoe(v);
        buf_ += sizeof(v);

        return *this;
    }

    template<typename LengthType>
    Serializer& serialize(const std::string& s) {
        LengthType len = s.length();

        serialize(len);
        std::memcpy(buf_, s.c_str(), len);
        buf_ += len;

        return *this;
    }

    Serializer& skip(unsigned int n) {
        buf_ += n;
        return *this;
    }

private:
    char *buf_;
};

template<typename Endian>
class Deserializer {
public:
    explicit Deserializer(char *buf, size_t len) : buf_(buf), len_(len), good_(true) {
    }

    char *buf() { return buf_; }

    size_t len() { return len_; }

    template<typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
    Deserializer& deserialize(T& v) {
        if (good_) {
            if (len_ >= sizeof(v)) {
                v = Endian::etoh(*reinterpret_cast<T *>(buf_));
                buf_ += sizeof(v);
                len_ -= sizeof(v);
            } else {
                good_ = false;
            }
        }

        return *this;
    }

    template<typename LengthType>
    Deserializer& deserialize(std::string& s) {
        LengthType len;
        deserialize(len);

        if (good_) {
            std::string::size_type n = len;
            if (len_ >= n) {
                s.assign(buf_, n);
                buf_ += n;
                len_ -= n;
            } else {
                good_ = false;
            }
        }

        return *this;
    }

    operator bool() {
        return good_;
    }

private:
    char *buf_;
    size_t len_;
    bool good_;
};


#endif
