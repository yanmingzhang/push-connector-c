#ifndef SHARED_BUFFER_H
#define SHARED_BUFFER_H

#include "uv.h"

class SharedBuffer {
public:
    SharedBuffer *make(size_t buf_size) {
        char *mem = new char[sizeof(SharedBuffer) + buf_size];
        return new(mem) SharedBuffer(buf_size);
    }

    void acquire() {
        ++ref_count_;
    }

    void release() {
        if (--ref_count_ == 0) {
            this->~SharedBuffer();
            delete reinterpret_cast<char *>(this);
        }
    }

private:
    SharedBuffer(size_t buf_size) : ref_count_(1) {
        buf_.base = reinterpret_cast<char *>(this + 1);
        buf_.len = buf_size;
    }

    ~SharedBuffer() = default;

private:
    int ref_count_;
    uv_buf_t buf_;
};


#endif // SHARED_BUFFER_H
