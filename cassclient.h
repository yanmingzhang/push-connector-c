#ifndef CASSCLIENT_H_
#define CASSCLIENT_H_

#include <stdint.h>
#include <string.h>
#include <endian.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <cassandra.h>
#include "common.h"


class CassClient
{
public:
    explicit CassClient(const char *contact_points);

    // not copyable
    CassClient(const CassClient&) = delete;
    CassClient& operator=(const CassClient&) = delete;

    // move
    CassClient(CassClient&&) = default;

    ~CassClient();

    bool connect();

    CassError get_device(const char *device_id, Device& device);
    CassError get_notifications(const char *topic, const CassUuid& offset,
                    std::vector<Notification>& notifications);
    bool device_online(const char *device_id, int64_t gate_svr_id);
    bool device_offline(const char *device_id, int64_t gate_svr_id);

    void device_offline_async(const char *device_id, int64_t gate_svr_id);

private:
    const CassPrepared *prepare(const char *cql);

private:
    CassCluster *cluster_;
    CassSession *session_;

    const CassPrepared *ps_get_device_;
    const CassPrepared *ps_get_notifications_;
    const CassPrepared *ps_device_online_;
    const CassPrepared *ps_device_offline_;
};

#endif
