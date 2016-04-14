#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <uv.h>
#include <string>
#include <mutex>
#include <memory>
#include <unordered_map>
#include <cassandra.h>
#include "defs.h"
#include "cassclient.h"

#define IDLE_TIMEOUT    300000

static std::unordered_map<std::string, conn_t *> g_map;
static std::mutex g_mutex;


static void conn_reset_buffer(conn_t *conn)
{
    conn->buffer.base = conn->prealloc_buf;
    conn->buffer.len = PREALLOC_BUFFER_SIZE;

    conn->write_index = 0;

    conn->msg_size = 0;
}

static void conn_init(conn_t *conn, uv_loop_t *loop)
{
    uv_timer_init(loop, &conn->timer);
    uv_tcp_init(loop, &conn->tcp);
    conn_reset_buffer(conn);
    conn->ref_count = 1;
}

static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    conn_t *conn = CONTAINER_OF(handle, conn_t, tcp);

    buf->base = conn->buffer.base + conn->write_index;
    buf->len = conn->buffer.len - conn->write_index;
}

static void close_cb(uv_handle_t *handle)
{
    conn_t *conn = CONTAINER_OF(handle, conn_t, tcp);

    // remove from device map
    if (!conn->device_id.empty()) {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_map.erase(conn->device_id);
    }
    
    uv_close((uv_handle_t *)&conn->timer, NULL);
    
    if (--conn->ref_count == 0) {
        delete conn;
    }
}

static void write_cb(uv_write_t *req, int status)
{
    delete (char *)req->data;
    delete req;
}

static void conn_timer_expire(uv_timer_t *handle)
{
    conn_t *conn = CONTAINER_OF(handle, conn_t, timer);
    uv_close((uv_handle_t *)&conn->tcp, close_cb);
}

static void read_notifications_cb(uv_work_t *req)
{
    read_notifications_work_t *work = CONTAINER_OF(req, read_notifications_work_t, work_req);
    CassClient *cass_client = work->cass_client;
    
    Device *pDevice;
    CassError rc = cass_client->get_device(work->device_id.c_str(), &pDevice);
    if (rc != CASS_OK)
        return;

    std::unique_ptr<Device> device(pDevice);
       
    if (device == NULL) {
        // TODO: insert new device?
        return;
    }

    CassUuid offset;    
    for (const std::string& topic: device->topics()) {
        auto itr = work->topic_offset_map.find(topic);
        if (itr == work->topic_offset_map.end()) {
            offset = cass_uuid_min_from_time(0, &offset);
        } else {
            offset = *itr;
        }
        
        // get notifications for this topic
        rc = cass_client->get_notifications(topic.c_str(), offset, work->notifications);
    }
}

static size_t calc_notification_push_size(const Notification& notification)
{
    return 3 + notification.estimate_size();
}

static char *write_notification_push_msg(char *out, const Notification& notification)
{
    size_t length;

    // message size
    *(uint16_t *)out = htobe16(calc_notification_push_size(notification));
    out += 2;

    // message type
    *(uint8_t *)out = MSG_NOTIFICATION_PUSH;
    out++;
    
    return notification.encode(out);
}

static void after_read_notifications_cb(uv_work_t *req, int status)
{
    read_notifications_work_t *work = CONTAINER_OF(req, read_notifications_work_t, work_req);
    conn_t *conn = work->conn;
    
    if (status != UV__ECANCELED && !work->notifications.empty() &&
        !uv_is_closing((uv_handle_t *)&conn->tcp)) {
        // write out results
        uv_write_t *write_req = new uv_write_t();
        uv_buf_t write_buf;

        std::vector<Notification>::const_iterator itr;
        size_t total_size = 0;
        for (const Notification& notification: work->notifications) {
            total_size += calc_notification_push_size(notification);
        }

        write_buf.base = new char[total_size];
        write_buf.len = total_size;
        char *out = write_buf.base;
        for (const Notification& notification: work->notifications) {
            out = write_notification_push_msg(out, notification);
        }

        write_req->data = write_buf.base;
        uv_write(write_req, (uv_stream_t *)&work->conn->tcp, &write_buf, 1, write_cb);
    }

    delete work;
    
    if (--conn->ref_count == 0) {
        delete conn;
    }
}

static void process_login(conn_t *conn, const char *buffer, uint16_t size)
{
    const char *end = buffer + size;
    size_t length;

    // device_id
    length = *(uint8_t *)buffer;
    buffer++;

    std::string device_id(buffer, length);
    buffer += length;

    // topic offset map
    std::unordered_map<std::string, CassUuid> topic_offset_map;

    size_t count = ntohs(*(uint16_t *)buffer);
    buffer += 2;

    for (size_t i = 0; i < count; ++i) {
        // topic
        length = ntohs(*(uint16_t *)buffer);
        buffer += 2;

        std::string topic(buffer, length);
        buffer += length;

        // offset
        CassUuid offset;
        offset.time_and_version = be64toh(*(uint64_t *)buffer);
        buffer += 8;
        offset.clock_seq_and_node = be64toh(*(uint64_t *)buffer);
        buffer += 8;

        topic_offset_map[topic] = offset;
    }

    read_notifications_work_t *work = new read_notifications_work_t;
    work->device_id = device_id;
    work->topic_offset_map = topic_offset_map;
    work->conn = conn;
    work->cass_client = (CassClient *)conn->tcp.loop->data;

    ++conn->ref_count;
    uv_queue_work(conn->tcp.loop, &work->work_req, read_notifications_cb, after_read_notifications_cb);

    // put to device map
    std::lock_guard<std::mutex> lock(g_mutex);
    g_map[conn->device_id] = conn;
}

static void process_notification_ack(conn_t *conn, const char *buffer, uint16_t size)
{

}

static void process_message(conn_t *conn, const char *buffer, uint16_t msg_size)
{
    if (msg_size == 2) {
        // heartbeat message
        uv_timer_start(&conn->timer, conn_timer_expire, IDLE_TIMEOUT, 0);
        return;
    }

    buffer += 2;    // skip message size field

    uint8_t msg_type = *(uint8_t *)buffer;
    buffer++;

    msg_size -= 3;

    switch (msg_type) {
    case MSG_LOGIN:
        process_login(conn, buffer, msg_size);
        break;
    case MSG_NOTIFICATION_ACK:
        process_notification_ack(conn, buffer, msg_size);
        break;
    default:
        fprintf(stderr, "Invalid message type %u\n", msg_type);
        uv_close((uv_handle_t *)&conn->tcp, close_cb);
        break;
    }
}

static void parse_message(conn_t *conn, ssize_t nread)
{
    // read_index always is 0
    conn->write_index += nread;

loop:
    if (conn->msg_size == 0) {
        assert(conn->buffer.base == conn->prealloc_buf);
        assert(conn->buffer.len == PREALLOC_BUFFER_SIZE);

        if (conn->write_index < 2)
            return;

        conn->msg_size = ntohs(*(uint16_t *)conn->buffer.base);

        if (conn->msg_size < 2) {
            // TODO: bad message, close connection
            return;
        }

        if (conn->msg_size > PREALLOC_BUFFER_SIZE) {
            // dynamiclly allocate buffer
            conn->buffer.base = new char[conn->msg_size];
            conn->buffer.len = conn->msg_size;

            memcpy(conn->buffer.base, conn->prealloc_buf, conn->write_index);
        }
    }

    if (conn->write_index >= conn->msg_size) {
        // received full message, process it!
        process_message(conn, conn->buffer.base, conn->msg_size);

        //
        if (conn->buffer.base != conn->prealloc_buf) {
            // dynamiclly allocated buffer won't read in more than one message
            delete conn->buffer.base;
        } else if (conn->write_index > conn->msg_size) {
            // we read in multiple messages!
            conn->write_index -= conn->msg_size;
            memmove(conn->buffer.base, conn->buffer.base + conn->msg_size, conn->write_index);
            conn->msg_size = 0;
            goto loop;
        }

        conn_reset_buffer(conn);
    }
}

static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    conn_t *conn = CONTAINER_OF(stream, conn_t, tcp);

    if (nread > 0) {
        uv_timer_start(&conn->timer, conn_timer_expire, IDLE_TIMEOUT, 0);

        assert(buf->base == conn->buffer.base + conn->write_index);

        // parse message
        parse_message(conn, nread);
    } else if (nread < 0) {
        if (nread != UV_EOF) {
            fprintf(stderr, "Read error %s\n", uv_err_name(nread));
        }

        if (buf->base != NULL) {
            free(buf->base);
        }

        uv_close((uv_handle_t *)stream, close_cb);
    }
}

static void connection_cb(uv_stream_t *server, int status)
{
    if (status < 0) {
        fprintf(stderr, "New connection error %s\n", uv_strerror(status));
        return;
    }

    conn_t *conn = new conn_t();
    conn_init(conn, server->loop);

    if (uv_accept(server, (uv_stream_t *)&conn->tcp) == 0) {
        uv_tcp_nodelay(&conn->tcp, 1);
        uv_read_start((uv_stream_t *)&conn->tcp, alloc_cb, read_cb);
        uv_timer_start(&conn->timer, conn_timer_expire, IDLE_TIMEOUT, 0);
    } else {
        uv_close((uv_handle_t *)&conn->tcp, close_cb);
    }
}

int main(int argc, char *argv[])
{
    uv_loop_t *loop;
    uv_tcp_t server;
    struct sockaddr_in addr;
    CassClient cass_client("10.240.225.101");
    
    if (!cass_client.connect()) {
        return 1;
    }
    
    loop = uv_default_loop();
    loop->data = &cass_client;
    uv_tcp_init(loop, &server);

    uv_ip4_addr("0.0.0.0", 52572, &addr);
    uv_tcp_bind(&server, (const struct sockaddr *)&addr, 0);

    uv_listen((uv_stream_t *)&server, 1024, connection_cb);

    uv_run(loop, UV_RUN_DEFAULT);
    uv_loop_close(loop);

    return 0;
}
