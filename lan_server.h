#ifndef LAN_SERVER_H
#define LAN_SERVER_H

#include <stdio.h>
#include <stdint.h>
#include <endian.h>
#include <string>
#include <vector>
#include "common.h"
#include "wan_server.h"
#include "serialization.h"

struct NotificationPushMsg
{
    template<typename Endian>
    void serialize(Serializer<Endian>& serializer) const {
        notification.serialize(serializer);
        serializer.serialize(server_id);

        serializer.serialize(static_cast<uint16_t>(devices.size()));
        for (const auto& device: devices) {
            serializer.serialize<uint8_t>(device);
        }
    }

    template<typename Endian>
    void deserialize(Deserializer<Endian>& deserializer) {
        notification.deserialize(deserializer);
        deserializer.deserialize(server_id);

        uint16_t num_devices;
        std::string device;
        deserializer.deserialize(num_devices);
        for (uint16_t i = 0; i < num_devices; ++i) {
            deserializer.deserialize<uint8_t>(device);
            devices.push_back(std::move(device));
        }
    }

    Notification notification;
    uint16_t server_id;
    std::vector<std::string> devices;
};

class LanServer : public TcpServerBase<LanServer, uint32_t>
{
public:
    static constexpr int STATIC_BUF_SIZE = 4096;
    static constexpr int IDLE_TIMEOUT = 30000;     // milliseconds

    enum class MessageType : uint16_t {
        // server to device messages
        MSG_NOTIFICATION_PUSH = 1
    };

    explicit LanServer(unsigned int id, WanServer **wan_servers, unsigned int num_wan_servers)
        : TcpServerBase(id), wan_servers_(wan_servers), num_wan_servers_(num_wan_servers) {

    }

    static int process_message(conn_base_t *conn, char *buffer, uint32_t size) {
        fprintf(stdout, "Processing message with size %u\n", size);

        if (size == 4) {
            // heartbeat message
            return 0;
        }

        buffer += 4;    // skip message size field
        size -= 4;

        uint8_t msg_type = be16toh(*(uint16_t *)buffer);
        buffer++;
        size--;

        switch (static_cast<int>(msg_type)) {
        case static_cast<int>(MessageType::MSG_NOTIFICATION_PUSH):
            process_notification_push(conn, buffer, size);
            break;
        default:
            fprintf(stderr, "Invalid message type %u\n", msg_type);
            return -1;
        }

        return 0;
    }

private:
    static void process_notification_push(conn_base_t *conn, char *buffer, uint32_t size) {
        LanServer *server = static_cast<LanServer *>(conn->tcp.data);

        Deserializer<BigEndian> deserializer(buffer, size);

        NotificationPushMsg message;

        message.deserialize(deserializer);

        if (message.server_id >= server->num_wan_servers_) {
            fprintf(stderr, "Invalid server id %u with server notification push", message.server_id);
            return;
        }

        server->wan_servers_[message.server_id]->send_notifications(
                    std::move(message.notification), std::move(message.devices));
    }

private:
    WanServer **wan_servers_;
    unsigned int num_wan_servers_;
};

#endif // LAN_SERVER_H
