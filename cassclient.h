#ifndef CASSCLIENT_H_
#define CASSCLIENT_H_

#include <string>
#include <vector>
#include <unordered_set>
#include <cassandra.h>

class Device
{
public:
    Device(const std::string& id, const cass_int64_t last_active_time,
           const std::string& province, const std::unordered_set<std::string>& topics)
        : id_(id), last_active_time_(last_active_time), province_(province), topics_(topics)
    {
    }

    ~Device() {}

    const std::string& id() const { return id_; }
    const cass_int64_t last_active_time() const { return last_active_time_; }
    const std::string& province() const { return province_; }
    const std::unordered_set<std::string>& topics() const { return topics_; }

private:
    const std::string id_;
    const cass_int64_t last_active_time_;
    const std::string province_;
    const std::unordered_set<std::string> topics_;
};

class Notification {

public:
    Notification(const std::string& topic, const CassUuid& create_time,
                 const std::string& sender, const std::string& content)
        : topic_(topic), create_time_(create_time), sender_(sender), content_(content)
    {
    }

    ~Notification() {}

    const std::string& topic() const { return topic_; }
    const CassUuid& create_time() const { return create_time_; }
    const std::string& sender() const { return sender_; }
    const std::string& content() const { return content_; }

private:
    const std::string topic_;
    const CassUuid create_time_;
    const std::string sender_;
    const std::string content_;
};

class CassClient
{
public:
    CassClient() = delete;
    CassClient(const CassClient&) = delete;
    CassClient& operator=(const CassClient&) = delete;

    explicit CassClient(const char *contact_points);
    
    ~CassClient();
    
    bool connect();
    
    CassError get_device(const char *device_id, Device **device);
    CassError get_notifications(const char *topic, const CassUuid& offset,
                                std::vector<Notification> notifications);
    CassError device_online(const char *device_id, const CassInet& ip, cass_int16_t port);
    CassError device_offline(const char *device_id, const CassInet& ip, cass_int16_t port);

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
