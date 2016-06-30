#ifndef LAN_SERVER_H
#define LAN_SERVER_H

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <endian.h>
#include "cassclient.h"

struct NotificationPushMsg
{
    char *encode(char *out) const {
        out = notification.encode(out);

        *(uint16_t *)out = htobe16(server_id);
        out += 2;
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

    enum class MessageType : uint8_t {
        // server to device messages
        MSG_NOTIFICATION_PUSH = 1
    };

    static int process_message(conn_base_t *conn, char *buffer, uint32_t size) {
        fprintf(stdout, "Processing message with size %u\n", size);

        if (size == 4) {
            // heartbeat message
            return 0;
        }

        buffer += 4;    // skip message size field
        size -= 4;

        uint8_t msg_type = *(uint8_t *)buffer;
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

    }
};

#endif // LAN_SERVER_H
