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
#include "serialization.h"
#include "tcp_server_base.h"
#include "cassclient.h"
#include "shared_buffer.h"

class WanServer : public TcpServerBase<WanServer, uint16_t>
{
public:
    static constexpr int STATIC_BUF_SIZE = 4096;
    static constexpr int IDLE_TIMEOUT = 30000000;     // milliseconds

    using ConnType = struct conn_t;

    enum class MessageType : uint8_t {
        // device to server messages
        MSG_LOGIN = 1,
        MSG_NOTIFICATION_ACK = 2,

        // server to device messages
        MSG_NOTIFICATION_PUSH = 129
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
    explicit WanServer(unsigned int id, CassClient *cass_client, int64_t net_id)
        : TcpServerBase(id), cass_client_(cass_client), net_id_(net_id) {
    }

    static conn_base_t *conn_new() {
        conn_t *conn = new conn_t();

        return static_cast<conn_base_t *>(conn);
    }

    static void conn_close(conn_base_t *conn_base) {
        conn_t *conn = static_cast<conn_t *>(conn_base);

        if (!conn->device_id.empty()) {
            WanServer *server = static_cast<WanServer *>(conn_base->tcp.data);

            server->remove_device(conn->device_id, conn);
            server->cass_client_->device_offline_async(conn->device_id.c_str(), server->net_id_);
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
/*
        uv_thread_t curr_thread_id = uv_thread_self();
        if (uv_thread_equal(&curr_thread_id, &loop_thread_id_)) {
            send_notifications_sync(notification, devices);
        } else {
*/
            auto cb = [](uv_async_t *handle) {
                PushNotificationAsync *pn_async = CONTAINER_OF(handle, PushNotificationAsync, handle);
                WanServer *server = static_cast<WanServer *>(handle->data);

                server->send_notifications_sync(pn_async->notification, pn_async->devices);
                delete pn_async;
            };

            PushNotificationAsync *pn_async = new PushNotificationAsync(std::move(notification), std::move(devices));
            uv_async_init(loop(), &pn_async->handle, cb);
            pn_async->handle.data = this;
            uv_async_send(&pn_async->handle);
/*
        }
*/
    }

private:
    static inline size_t calc_notification_push_size(const Notification& notification) {
        return 3 + notification.estimate_size();
    }

    template<typename Endian>
    static void write_notification_push_msg(Serializer<Endian>& serializer, const Notification& notification) {
        char *mark = serializer.buf();  // mark message size field

        // skip message size field
        serializer.skip(2);

        // message type
        serializer.serialize(static_cast<uint8_t>(MessageType::MSG_NOTIFICATION_PUSH));
        notification.serialize(serializer);

        // set message size
        Serializer<Endian> msg_size_serializer(mark);
        msg_size_serializer.serialize(static_cast<uint16_t>(serializer.buf() - mark));
    }

    static void on_write_complete(uv_write_t *req, int status) {
        if (req->data != NULL) {
            // shared buffer
            shared_buffer_t *buffer = (shared_buffer_t *)req->data;
            shared_buffer_release(buffer);
        }

        free(req);
    }

    static void process_login(conn_t *conn, char *buffer, uint16_t size) {
        Deserializer<BigEndian> deserializer(buffer, size);

        // device_id
        std::string device_id;
        deserializer.deserialize<uint8_t>(device_id);

        // topic offset map
        std::unordered_map<std::string, CassUuid> topic_offset_map;

        uint16_t num_topic_offset;
        deserializer.deserialize(num_topic_offset);

        for (uint16_t i = 0; i < num_topic_offset; ++i) {
            // topic
            std::string topic;
            deserializer.deserialize<uint16_t>(topic);

            // offset
            CassUuid offset;
            deserializer.deserialize(offset.time_and_version);
            deserializer.deserialize(offset.clock_seq_and_node);

            topic_offset_map.emplace(std::move(topic), offset);
        }

        if (!deserializer) {
            fprintf(stderr, "Parse login message failed");
            return;
        }

        ReadNotificationsWork *work = new ReadNotificationsWork(
                    conn, std::string(device_id),
                    std::move(topic_offset_map));
        uv_queue_work(conn->tcp.loop, &work->work_req, read_notifications_cb, after_read_notifications_cb);
        ++conn->ref_count;

        // put to device map
        conn->device_id = device_id;

        WanServer *server = static_cast<WanServer *>(conn->tcp.data);
        server->put_device(std::move(device_id), conn);
    }

    static void process_notification_ack(conn_t *conn, char *buffer, uint16_t size) {

    }

    static void read_notifications_cb(uv_work_t *req) {
        ReadNotificationsWork *work = CONTAINER_OF(req, ReadNotificationsWork, work_req);
        WanServer *server = static_cast<WanServer *>(work->conn->tcp.data);
        CassClient *cass_client = server->cass_client_;

        Device device;
        CassError rc = cass_client->get_device(work->device_id.c_str(), device);
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
            rc = cass_client->get_notifications(topic.c_str(), offset, work->notifications);
        }
    }

    static void after_read_notifications_cb(uv_work_t *req, int status)
    {
        ReadNotificationsWork *work = CONTAINER_OF(req, ReadNotificationsWork, work_req);
        conn_t *conn = work->conn;

        if (status != UV__ECANCELED && !work->notifications.empty() &&
            !uv_is_closing((uv_handle_t *)&conn->tcp)) {

            // write out results
            size_t total_size = 0;
            for (const auto& notification: work->notifications) {
                total_size += calc_notification_push_size(notification);
            }

            uv_write_t *write_req;

            write_req = (uv_write_t *)malloc(sizeof(uv_write_t) + total_size);
            write_req->data = NULL;

            uv_buf_t buf;
            buf.base = (char *)(write_req + 1);
            buf.len = total_size;

            Serializer<BigEndian> serializer(buf.base);
            for (const auto& notification: work->notifications) {
                write_notification_push_msg(serializer, notification);
            }

            uv_write(write_req, (uv_stream_t *)&conn->tcp, &buf, 1, on_write_complete);

            // put on cache list
            WanServer *server = static_cast<WanServer *>(work->conn->tcp.data);
            server->cass_client_->device_online(conn->device_id.c_str(), server->net_id_);
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

    static void write_shared_message(uv_tcp_t *handle, shared_buffer_t *shared_buffer) {
        uv_write_t *req;
        auto cb = [](uv_write_t *req, int status) {
            shared_buffer_t *shared_buffer = static_cast<shared_buffer_t *>(req->data);
            shared_buffer_release(shared_buffer);
            delete req;
        };

        req = new uv_write_t();
        req->data = shared_buffer;
        uv_write(req, (uv_stream_t *)handle, &shared_buffer->buf, 1, cb);
        shared_buffer_acquire(shared_buffer);
    }

    void send_notifications_sync(const Notification& notification, const std::vector<std::string>& devices) {
        // construct push notification message
        size_t msg_size = calc_notification_push_size(notification);
        shared_buffer_t *shared_buffer = shared_buffer_new(msg_size);
        Serializer<BigEndian> serializer(shared_buffer->buf.base);
        write_notification_push_msg(serializer, notification);

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

        shared_buffer_release(shared_buffer);
    }

private:
    CassClient *cass_client_;
    std::unordered_map<std::string, conn_t*> device_map_;
    int64_t net_id_;
};

#endif
