#ifndef DEFS_H
#define DEFS_H

#include <stdint.h>
#include <uv.h>
#include <netinet/in.h>
#include <cassandra.h>
#include <string>
#include <unordered_map>
#include <vector>
#include "cassclient.h"

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
    struct sockaddr_in& lan_addr() { return lan_addr_; }
    struct sockaddr_in& wan_addr() { return wan_addr_; }
    const std::string& cassandra_addr() { return cassandra_addr_; }

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
