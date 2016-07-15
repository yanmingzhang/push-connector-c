#ifndef SHARED_BUFFER_H
#define SHARED_BUFFER_H

#include <stdlib.h>
#include <uv.h>

typedef struct {
    uv_buf_t buf;
    unsigned int ref_count;
} shared_buffer_t;


shared_buffer_t *shared_buffer_new(unsigned int buf_size) {
    shared_buffer_t *shared_buffer = (shared_buffer_t *)malloc(sizeof(shared_buffer_t) + buf_size);
    if (shared_buffer != NULL) {
        shared_buffer->ref_count = 1;
        shared_buffer->buf.base = (char *)(shared_buffer + 1);
        shared_buffer->buf.len = buf_size;
    }

    return shared_buffer;
}

void shared_buffer_acquire(shared_buffer_t *shared_buffer) {
    ++shared_buffer->ref_count;
}

void shared_buffer_release(shared_buffer_t *shared_buffer) {
    if (--shared_buffer->ref_count == 0) {
        free(shared_buffer);
    }
}

#endif // SHARED_BUFFER_H
