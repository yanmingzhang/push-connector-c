#ifndef CASSCLIENT_H_
#define CASSCLIENT_H_

#include <string>
#include <vector>
#include <cassandra.h>

typedef struct {
    std::string  id;
    int last_active_time;
    std::string province;
    std::vector<std::string> topics;  
} device_t;

typedef struct {
    std::string topic;
    CassUuid create_time;
    std::string sender;
    std::string content;
} notification_t;

public class CassClient
{
public:
    CassClient() = delete;
    CassClient(const CassClient&) = delete;
    CassClient& operator=(const CassClient&) = delete;

    explicit CassClient(const char *contact_points);
    
    ~CassClient();
    
    bool connect();
    
    bool get_device(device_t& device);
    std::vector<notification_t> get_notifications(std::string& topic, CassUuid offset);
    bool device_online(std::string device_id, std::string ip, int port);
    bool device_offline(std::string device_id, std::string ip, int port); 

private:
    const CassPrepared *prepare(const char *cql)

private:
    CassCluster *cluster_;
    CassSession *session_;
    
    const CassPrepared *ps_get_device_;
    const CassPrepared *ps_get_notifications_;
    const CassPrepared *ps_device_online_;
    const CassPrepared *ps_device_offline_;
}

#endif
