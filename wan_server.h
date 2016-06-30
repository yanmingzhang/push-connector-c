#ifndef WAN_SERVER_H
#define WAN_SERVER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <uv.h>
#include <unordered_map>
#include <mutex>
#include <vector>
#include "tcp_server_base.h"
#include "cassclient.h"
#include "shared_buffer.h"

class WanServer : public TcpServerBase<WanServer, uint16_t>
{
public:
    static constexpr int STATIC_BUF_SIZE = 4096;
    static constexpr int IDLE_TIMEOUT = 30000;     // milliseconds

    enum class MessageType : uint8_t {
        // device to server messages
        MSG_LOGIN = 1,
        MSG_NOTIFICATION_ACK = 2,

        // server to device messages
        MSG_NOTIFICATION_PUSH = 128
    };

    struct conn_t : conn_base_t {

        conn_t() : ref_count(1) {
        }

        std::string device_id;
        uint32_t ref_count;
    };

    struct ReadNotificationsWork {
        explicit ReadNotificationsWork(conn_t *conn, std::string&& device_id,
                    std::unordered_map<std::string, CassUuid>&& topic_offset_map)
        : conn(conn), device_id(std::move(device_id)),
          topic_offset_map(std::move(topic_offset_map))
        {
        }

        uv_work_t work_req;
        std::string device_id;
        std::unordered_map<std::string, CassUuid> topic_offset_map;
        conn_t *conn;
        // result
        std::vector<Notification> notifications;
    };

    // constructor
    explicit WanServer(unsigned int id, uv_loop_t *loop, CassClient&& cass_client)
        : TcpServerBase(id, loop), cass_client_(std::move(cass_client)) {
    }

    static conn_base_t *conn_new() {
        conn_t *conn = new conn_t();

        return static_cast<conn_base_t *>(conn);
    }

    static void conn_close(conn_base_t *conn_base) {
        conn_t *conn = static_cast<conn_t *>(conn_base);

        if (!conn->device_id.empty()) {
            WanServer *server = static_cast<WanServer *>(conn_base->tcp.loop->data);
            server->remove_device(conn->device_id, conn);
        }

        if (--conn->ref_count == 0) {
            delete conn;
        }
    }

    static int process_message(conn_base_t *conn_base, char *buffer, uint16_t size) {
        conn_t *conn = static_cast<conn_t *>(conn_base);

        fprintf(stdout, "Processing message with size %u\n", size);

        if (size == 2) {
            // heartbeat message
            return 0;
        }

        buffer += 2;    // skip message size field
        size -= 2;

        uint8_t msg_type = *(uint8_t *)buffer;
        buffer++;
        size--;

        switch (static_cast<int>(msg_type)) {
        case static_cast<int>(MessageType::MSG_LOGIN):
            process_login(conn, buffer, size);
            break;
        case static_cast<int>(MessageType::MSG_NOTIFICATION_ACK):
            process_notification_ack(conn, buffer, size);
            break;
        default:
            fprintf(stderr, "Invalid message type %u\n", msg_type);
            return -1;
        }

        return 0;
    }

    // Can be called from any thread
    void send_notifications_to_all(Notification&& notification) {
        send_notifications(std::move(notification), std::vector<std::string>());
    }

    // Can be called from any thread
    void send_notifications(Notification&& notification, std::vector<std::string>&& devices) {
        uv_thread_t curr_thread_id = uv_thread_self();
        if (uv_thread_equal(&curr_thread_id, &loop_thread_id_)) {
            send_notifications_sync(notification, devices);
        } else {
            auto cb = [](uv_async_t *handle) {
                PushNotificationAsync *pn_async = CONTAINER_OF(handle, PushNotificationAsync, handle);
                WanServer *server = static_cast<WanServer *>(handle->loop->data);

                server->send_notifications_sync(pn_async->notification, pn_async->devices);
                delete pn_async;
            };

            PushNotificationAsync *pn_async = new PushNotificationAsync(std::move(notification), std::move(devices));
            uv_async_init(loop(), &pn_async->handle, cb);
            uv_async_send(&pn_async->handle);
        }
    }

private:
    static inline size_t calc_notification_push_size(const Notification& notification) {
        return 3 + notification.estimate_size();
    }

    static char *write_notification_push_msg(char *out, const Notification& notification) {
        size_t length;
        char *msg_size = out;

        // skip message size field
        out += 2;

        // message type
        *(uint8_t *)out = static_cast<uint8_t>(MessageType::MSG_NOTIFICATION_PUSH);
        out++;

        // content
        out = notification.encode(out);

        // set message size
        *(uint16_t *)msg_size = htobe16(out - msg_size);

        return out;
    }

    static void write_cb(uv_write_t *req, int status)
    {
        delete[] ((char *)req->data);
        delete req;
    }

    static void process_login(conn_t *conn, char *buffer, uint16_t size) {
        // char *end = buffer + size;
        uint16_t length;

        // device_id
        uint8_t device_id_length = *(uint8_t *)buffer;
        buffer++;

        const char *device_id = buffer;
        buffer += length;

        // topic offset map
        std::unordered_map<std::string, CassUuid> topic_offset_map;

        uint16_t count = be16toh(*(uint16_t *)buffer);
        buffer += 2;

        for (uint16_t i = 0; i < count; ++i) {
            // topic
            std::size_t topic_len = be16toh(*(uint16_t *)buffer);
            buffer += 2;
            const char *topic = buffer;
            buffer += topic_len;

            // offset
            CassUuid offset;
            offset.time_and_version = be64toh(*(uint64_t *)buffer);
            buffer += 8;
            offset.clock_seq_and_node = be64toh(*(uint64_t *)buffer);
            buffer += 8;

            topic_offset_map.emplace(std::make_pair(std::string(topic, topic_len), offset));
        }

        ReadNotificationsWork *work = new ReadNotificationsWork(
                    conn, std::string(device_id, device_id_length),
                    std::move(topic_offset_map));
        uv_queue_work(conn->tcp.loop, &work->work_req, read_notifications_cb, after_read_notifications_cb);
        ++conn->ref_count;

        // put to device map
        WanServer *server = static_cast<WanServer *>(conn->tcp.loop->data);
        server->put_device(std::string(device_id, device_id_length), conn);
    }

    static void process_notification_ack(conn_t *conn, char *buffer, uint16_t size) {

    }

    static void read_notifications_cb(uv_work_t *req) {
        ReadNotificationsWork *work = CONTAINER_OF(req, ReadNotificationsWork, work_req);
        WanServer *server = static_cast<WanServer *>(work->conn->tcp.loop->data);
        CassClient& cass_client = server->cass_client_;

        Device device;
        CassError rc = cass_client.get_device(work->device_id.c_str(), device);
        if (rc != CASS_OK)
            return;

        if (device.id.empty()) {
            // TODO: insert new device?
            return;
        }

        CassUuid offset;
        for (const auto& topic: device.topics) {
            auto itr = work->topic_offset_map.find(topic);
            if (itr == work->topic_offset_map.end()) {
                cass_uuid_min_from_time(0, &offset);
            } else {
                offset = itr->second;
            }

            // get notifications for this topic
            rc = cass_client.get_notifications(topic.c_str(), offset, work->notifications);
        }
    }

    static void after_read_notifications_cb(uv_work_t *req, int status)
    {
        ReadNotificationsWork *work = CONTAINER_OF(req, ReadNotificationsWork, work_req);
        conn_t *conn = work->conn;

        if (status != UV__ECANCELED && !work->notifications.empty() &&
            !uv_is_closing((uv_handle_t *)&conn->tcp)) {
            // write out results
            uv_write_t *write_req = new uv_write_t();
            uv_buf_t write_buf;

            size_t total_size = 0;
            for (const auto& notification: work->notifications) {
                total_size += calc_notification_push_size(notification);
            }

            write_buf.base = new char[total_size];
            write_buf.len = total_size;
            char *out = write_buf.base;
            for (const auto& notification: work->notifications) {
                out = write_notification_push_msg(out, notification);
            }

            write_req->data = write_buf.base;
            uv_write(write_req, (uv_stream_t *)&conn->tcp, &write_buf, 1, write_cb);
        }

        delete work;

        if (--conn->ref_count == 0) {
            delete conn;
        }
    }

    void put_device(std::string&& device_id, conn_t *conn) {
        device_map_.emplace(device_id, conn);
    }

    void remove_device(const std::string& device_id, conn_t *conn) {
        auto itr = device_map_.find(device_id);
        if (itr != device_map_.end() && itr->second == conn) {
            device_map_.erase(itr);
        }
    }

    struct PushNotificationAsync {
        explicit PushNotificationAsync(Notification&& notification, std::vector<std::string>&& devices)
            : notification(std::move(notification)), devices(std::move(devices)) {
        }

        uv_async_t handle;
        Notification notification;
        std::vector<std::string> devices;   // empty device list indicate send to all devices
    };

    typedef struct {
        unsigned int ref_count;
        uv_buf_t buf;
    } shared_buffer_t;

    static shared_buffer_t *make_shared_buffer(size_t buf_size) {
        shared_buffer_t *shared_buffer = (shared_buffer_t *)malloc(sizeof(shared_buffer_t) + buf_size);
        shared_buffer->ref_count = 1;
        shared_buffer->buf.base = (char *)(shared_buffer + 1);
        shared_buffer->buf.len = buf_size;

        return shared_buffer;
    }

    static void acquire_shared_buffer(shared_buffer_t *shared_buffer) {
        ++shared_buffer->ref_count;
    }

    static void release_shared_buffer(shared_buffer_t *shared_buffer) {
        if (--shared_buffer->ref_count == 0) {
            free(shared_buffer);
        }
    }

    static void write_shared_message(uv_tcp_t *handle, shared_buffer_t *shared_buffer) {
        uv_write_t *req;
        auto cb = [](uv_write_t *req, int status) {
            shared_buffer_t *shared_buffer = static_cast<shared_buffer_t *>(req->data);
            release_shared_buffer(shared_buffer);
            delete req;
        };

        req = new uv_write_t();
        req->data = shared_buffer;
        uv_write(req, (uv_stream_t *)handle, &shared_buffer->buf, 1, cb);
        acquire_shared_buffer(shared_buffer);
    }

    void send_notifications_sync(const Notification& notification, const std::vector<std::string>& devices) {
        // construct push notification message
        size_t msg_size = calc_notification_push_size(notification);
        shared_buffer_t *shared_buffer = make_shared_buffer(msg_size);
        write_notification_push_msg(shared_buffer->buf.base, notification);

        if (devices.empty()) {
            // send to all devices
            for (const auto& kv: device_map_) {
                write_shared_message(&kv.second->tcp, shared_buffer);
            }
        } else {
            // send to selected devices
            for (const auto& device_id: devices) {
                auto itr = device_map_.find(device_id);
                if (itr != device_map_.end()) {
                    write_shared_message(&itr->second->tcp, shared_buffer);
                }
            }
        }

        release_shared_buffer(shared_buffer);
    }


private:
    CassClient cass_client_;
    std::unordered_map<std::string, conn_t*> device_map_;
};

#endif
