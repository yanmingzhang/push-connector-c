#ifndef CASSCLIENT_H_
#define CASSCLIENT_H_

#include <stdint.h>
#include <string.h>
#include <endian.h>
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
    
    size_t estimate_size() {
        return (2 + topic_.length()) + 16 +
               (1 + sender_.length()) +
               (2 + content_.length());
    }
    
    char *encode(char *out) {
        // topic
        *(uint16_t *)out = htobe16(topic_.length());
        out += 2;
        memcpy(out, topic_.data(), topic_.length());
        out += length;

        // create time
        *(uint64_t *)out = htobe64(create_time_.time_and_version);
        out += 8;
        *(uint64_t *)out = htobe64(create_time_.clock_seq_and_node);
        out += 8;

        // sender
        *(uint8_t *)out = sender_.length();
        out++;
        memcpy(out, sender_.data(), sender_.length());
        out += length;

        // content
        *(uint16_t *)out = htobe16(content_.length());
        out += 2;
        memcpy(out, content_.data(), content_.length());
        out += length;
        
        return out;
    }
    
    const char *decode(const char *in) {
        size_t length;
        
        // topic
        length = be16toh(*(uint16_t *)in);
        in += 2;
        topic_.assign(in, length);
        in += length;
        
        // create time
        create_time_.time_and_version = *(uint64_t *)in;
        in += 8;
        create_time_.clock_seq_and_node = *(uint64_t *)in;
        in += 8;
        
        // sender
        length = *(uint8_t *)in;
        in++;
        sender_.assign(in, length);
        in += length;
        
        // content
        length = be16toh(*(uint16_t *)in);
        in += 2;
        content_.assign(in, length);
        in += length;
        
        return in;
    }   

    const std::string& topic() const { return topic_; }
    const CassUuid& create_time() const { return create_time_; }
    const std::string& sender() const { return sender_; }
    const std::string& content() const { return content_; }

private:
    std::string topic_;
    CassUuid create_time_;
    std::string sender_;
    std::string content_;
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
