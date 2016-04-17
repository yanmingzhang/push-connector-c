#ifndef DEFS_H
#define DEFS_H

#include <stdint.h>
#include <stdlib.h>
#include <uv.h>
#include <netinet/in.h>
#include <cassandra.h>
#include <arpa/inet.h>
#include <string>
#include <unordered_map>
#include <vector>
#include "cassclient.h"
#include "properties.h"

// MIN 3, MAX 65536
#define PREALLOC_BUFFER_SIZE    256

// device to server messages
#define MSG_LOGIN   1
#define MSG_NOTIFICATION_ACK    2

// server to device messages
#define MSG_NOTIFICATION_PUSH   128

class Config
{
public:
    explicit Config(Properties props) {
        const char *ip;
        const char *port;

        ip = props.get_property("lan.listen.ip");
        port = props.get_property("lan.listen.port");
        if (!to_addr(ip, port, lan_addr_)) {
            return;
        }

        ip = props.get_property("wan.listen.ip");
        port = props.get_property("wan.listen.port");
        if (!to_addr(ip, port, wan_addr_)) {
            return;
        }

        cassandra_addr_ = props.get_property("cassandra.address");
        if (cassandra_addr_.empty()) {
            return;
        }
    }

    struct sockaddr_in& lan_addr() { return lan_addr_; }
    struct sockaddr_in& wan_addr() { return wan_addr_; }
    const std::string& cassandra_addr() { return cassandra_addr_; }

    operator bool() {
        return !cassandra_addr_.empty();
    }

private:
    static bool to_addr(const char *ip, const char *str_port, struct sockaddr_in& addr) {
        if (ip == NULL || *ip == '\0' || str_port == NULL || *str_port == '\0') {
            return false;
        }

        addr.sin_family = AF_INET;
        if (!inet_aton(ip, &addr.sin_addr)) {
            return false;
        }

        int port = atoi(ip);
        if (port <= 0 || port >= 65536) {
            return false;
        }

        addr.sin_port = port;

        return true;
    }

private:
    struct sockaddr_in lan_addr_;
    struct sockaddr_in wan_addr_;

    std::string cassandra_addr_;

};

typedef struct {
    uv_tcp_t tcp;
    uv_timer_t timer;
    char prealloc_buf[PREALLOC_BUFFER_SIZE];
    // message parsing
    uv_buf_t buffer;
    uint16_t write_index;
    uint16_t msg_size;

    std::string device_id;
    
    int ref_count;
} conn_t;

typedef struct {
    uv_work_t work_req;
    std::string device_id;
    std::unordered_map<std::string, CassUuid> topic_offset_map;
    conn_t *conn;
    CassClient *cass_client;
    // result
    std::vector<Notification> notifications;
} read_notifications_work_t;

/* This macro looks complicated but it's not: it calculates the address
 * of the embedding struct through the address of the embedded struct.
 * In other words, if struct A embeds struct B, then we can obtain
 * the address of A by taking the address of B and subtracting the
 * field offset of B in A.
 */
#define CONTAINER_OF(ptr, type, field)                                        \
  ((type *) ((char *) (ptr) - ((char *) &((type *) 0)->field)))

#endif // DEFS_H
