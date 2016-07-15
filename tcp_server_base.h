#ifndef TCP_SERVER_BASE_H
#define TCP_SERVER_BASE_H

#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <strings.h>
#include <string.h>
#include <stdint.h>
#include <uv.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <iostream>
#include <type_traits>
#include "defs.h"

template<typename T, typename msg_size_type>
class TcpServerBase
{
public:
    static_assert(std::is_same<msg_size_type, uint8_t>::value ||
                  std::is_same<msg_size_type, uint16_t>::value ||
                  std::is_same<msg_size_type, uint32_t>::value,
            "Incorrect message size type");

    typedef struct {
        uv_tcp_t tcp;
        // message parsing
        char static_buf[T::STATIC_BUF_SIZE];
        uv_buf_t buffer;
        msg_size_type write_index;
        msg_size_type msg_size;
        // timer
        uv_timer_t timer;
    } conn_base_t;

    explicit TcpServerBase(uint16_t id)
             : id_(id) {
        bzero(&server_socket_, sizeof(server_socket_));
    }

    ~TcpServerBase() = default;

    uint16_t id() {
        return id_;
    }

    uv_loop_t *loop() {
        return server_socket_.loop;
    }

    int initialize(uv_loop_t *loop, const struct sockaddr_in& addr, int backlog) {
        int rc;

        // Create socket early
        rc = uv_tcp_init_ex(loop, &server_socket_, AF_INET);
        if (rc != 0) {
            fprintf(stderr, "uv_tcp_init_ex: %s\n", uv_strerror(rc));
            return rc;
        }
        server_socket_.data = static_cast<T *>(this);

        uv_os_fd_t fd;
        uv_fileno((const uv_handle_t *)&server_socket_, &fd);

        int optval = 1;
        rc = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
        if (rc != 0) {
            perror("set SO_REUSEPORT socket option");
            return rc;
        }

//        uv_ip4_addr(ip, port, &addr);
        rc = uv_tcp_bind(&server_socket_, (const struct sockaddr *)&addr, 0);
        if (rc != 0) {
            fprintf(stderr, "uv_tcp_bind: %s\n", uv_strerror(rc));
            return rc;
        }

        rc = uv_listen((uv_stream_t *)&server_socket_, backlog, on_connect);
        if (rc != 0) {
            fprintf(stderr, "uv_listen: %s\n", uv_strerror(rc));
            return rc;
        }

        return 0;
    }

    static void on_close(uv_handle_t *handle) {
        conn_base_t *conn = CONTAINER_OF(handle, conn_base_t, tcp);
        fprintf(stderr, "Connection %p closed\n", conn);

        uv_close((uv_handle_t *)&conn->timer, nullptr);

        if (conn->buffer.base != conn->static_buf) {
            delete[] conn->buffer.base;
        }

        T::conn_close(conn);
    }

    static void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
        conn_base_t *conn = CONTAINER_OF(handle, conn_base_t, tcp);
        assert(conn->write_index < conn->buffer.len);

        buf->base = conn->buffer.base + conn->write_index;
        buf->len = conn->buffer.len - conn->write_index;
    }

    static void on_connect(uv_stream_t *server, int status) {
        if (status < 0) {
            fprintf(stderr, "New connection error %s\n", uv_strerror(status));
            return;
        }

        conn_base_t *conn = T::conn_new();
        if (conn == nullptr) {
            fprintf(stderr, "Allocate connection object failed\n");
            return;
        }

        fprintf(stderr, "Connection %p created\n", conn);

        uv_tcp_init(server->loop, &conn->tcp);
        conn->tcp.data = server->data;      // T *

        conn->buffer.base = conn->static_buf;
        conn->buffer.len = sizeof(conn->static_buf);
        conn->write_index = 0;
        conn->msg_size = 0;

        uv_timer_init(server->loop, &conn->timer);

        if (uv_accept(server, (uv_stream_t *)&conn->tcp) == 0) {
            uv_tcp_nodelay(&conn->tcp, 1);
            uv_read_start((uv_stream_t *)&conn->tcp, alloc_buffer, on_read);
            uv_timer_start(&conn->timer, conn_timer_expire, T::IDLE_TIMEOUT, 0);
        } else {
            uv_close((uv_handle_t *)&conn->tcp, on_close);
        }
    }

    static void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
    {
        conn_base_t *conn = CONTAINER_OF(stream, conn_base_t, tcp);

        if (nread > 0) {
            assert(buf->base == conn->buffer.base + conn->write_index);

            // parse message
            parse_message(conn, nread);

            uv_read_start(stream, alloc_buffer, on_read);
            uv_timer_start(&conn->timer, conn_timer_expire, T::IDLE_TIMEOUT, 0);
        } else if (nread < 0) {
            if (nread != UV_EOF) {
                fprintf(stderr, "Read error %s on connection %p\n", uv_err_name(nread), conn);
            }
            uv_close((uv_handle_t *)stream, on_close);
        }
    }

protected:
    static conn_base_t *conn_new() {
        return new conn_base_t();
    }

    static void conn_close(conn_base_t *conn) {
        delete conn;
    }

private:
    static void conn_timer_expire(uv_timer_t *handle) {
        conn_base_t *conn = CONTAINER_OF(handle, conn_base_t, timer);
        uv_close((uv_handle_t *)&conn->tcp, on_close);
    }

    static void parse_message(conn_base_t *conn, ssize_t nread) {
        // read_index always start from 0
        conn->write_index += nread;

        if (conn->buffer.base != conn->static_buf) {
            assert(conn->msg_size == conn->buffer.len);
            assert(conn->write_index <= conn->buffer.len);

            if (conn->write_index == conn->msg_size) {
                T::process_message(conn, conn->buffer.base, conn->msg_size);

                delete[] conn->buffer.base;

                conn->buffer.base = conn->static_buf;
                conn->buffer.len = sizeof(conn->static_buf);
                conn->write_index = 0;
                conn->msg_size = 0;
            }
            return;
        }

        uint32_t read_index = 0;
        while (true) {
            if (conn->msg_size == 0) {
                if (conn->write_index - read_index < sizeof(msg_size_type)) {
                    conn->write_index -= read_index;
                    if (read_index > 0 && conn->write_index > 0) {
                        memmove(conn->static_buf, conn->static_buf + read_index, conn->write_index);
                    }
                    return;
                }

                conn->msg_size = *(msg_size_type *)(conn->static_buf + read_index);
                if (std::is_same<msg_size_type, uint16_t>::value) {
                    conn->msg_size = be16toh(conn->msg_size);
                } else if (std::is_same<msg_size_type, uint32_t>::value) {
                    conn->msg_size = be32toh(conn->msg_size);
                }

                if (conn->msg_size < sizeof(msg_size_type)) {
                    // TODO: bad message, close connection
                    return;
                }

                if (conn->msg_size > sizeof(conn->static_buf)) {
                    // dynamiclly allocate buffer
                    conn->buffer.base = new char[conn->msg_size];
                    conn->buffer.len = conn->msg_size;

                    conn->write_index -= read_index;
                    memcpy(conn->buffer.base, conn->static_buf + read_index, conn->write_index);
                    return;
                }
            }

            if (conn->write_index - read_index < conn->msg_size) {
                if (read_index > 0) {
                    conn->write_index -= read_index;
                    if (conn->write_index > 0) {
                        memmove(conn->static_buf, conn->static_buf + read_index, conn->write_index);
                    }
                }
                return;
            }

            // received full message, process it!
            T::process_message(conn, conn->static_buf + read_index, conn->msg_size);
            //
            read_index += conn->msg_size;
            conn->msg_size = 0;
        }
    }

protected:
    uint16_t id_;
    uv_tcp_t server_socket_;
};

#endif
