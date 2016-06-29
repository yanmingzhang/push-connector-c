#ifndef WAN_SERVER_H
#define WAN_SERVER_H

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <uv.h>
#include <unordered_map>
#include <mutex>
#include "tcp_server_base.h"
#include "cassclient.h"

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
    explicit WanServer(CassClient&& cass_client) : cass_client_(std::move(cass_client)) {

    }

    static conn_base_t *conn_new() {
        conn_t *conn = new conn_t();

        return static_cast<conn_base_t *>(conn);
    }

    static void conn_close(conn_base_t *conn_base) {
        conn_t *conn = static_cast<conn_t *>(conn_base);

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
        for (auto topic: device.topics) {
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
            for (const Notification& notification: work->notifications) {
                total_size += calc_notification_push_size(notification);
            }

            write_buf.base = new char[total_size];
            write_buf.len = total_size;
            char *out = write_buf.base;
            for (auto notification: work->notifications) {
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
        std::lock_guard<std::mutex> lock(device_map_mutex_);

        device_map_.emplace(device_id, conn);
    }

    void remove_device(const std::string& device_id, conn_t *conn) {
        std::lock_guard<std::mutex> lock(device_map_mutex_);

        auto itr = device_map_.find(device_id);
        if (itr != device_map_.end() && itr->second == conn) {
            device_map_.erase(itr);
        }
    }

private:
    CassClient cass_client_;
    std::unordered_map<std::string, conn_t*> device_map_;
    std::mutex device_map_mutex_;
};

#endif
