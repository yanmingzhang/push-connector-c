#ifndef CASSCLIENT_H_
#define CASSCLIENT_H_

#include <stdint.h>
#include <string.h>
#include <endian.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <cassandra.h>


struct Device
{
    Device() = default;

    explicit Device(std::string&& id, cass_int64_t last_active_time,
                    std::string&& province, std::unordered_set<std::string>&& topics)
        : id(std::move(id)), last_active_time(last_active_time),
        province(std::move(province)), topics(std::move(topics)) {
    }

    std::string id;
    cass_int64_t last_active_time;
    std::string province;
    std::unordered_set<std::string> topics;
};

struct Notification {
    Notification() {

    }

    explicit Notification(std::string&& topic, const CassUuid& create_time,
                          std::string&& sender, std::string&& content)
        : topic(std::move(topic)), create_time(create_time),
          sender(std::move(sender)), content(std::move(content)) {
    }

    size_t estimate_size() const {
        return (2 + topic.length()) + 16 +
               (1 + sender.length()) +
               (2 + content.length());
    }

    char *encode(char *out) const {
        std::string::size_type length;

        // topic
        length = topic.length();
        *(uint16_t *)out = htobe16(length);
        out += 2;
        memcpy(out, topic.data(), length);
        out += length;

        // create time
        *(uint64_t *)out = htobe64(create_time.time_and_version);
        out += 8;
        *(uint64_t *)out = htobe64(create_time.clock_seq_and_node);
        out += 8;

        // sender
        length = sender.length();
        *(uint8_t *)out = length;
        out++;
        memcpy(out, sender.data(), length);
        out += length;

        // content
        length = content.length();
        *(uint16_t *)out = htobe16(length);
        out += 2;
        memcpy(out, content.data(), length);
        out += length;

        return out;
    }

    const char *decode(const char *in) {
        size_t length;

        // topic
        length = be16toh(*(uint16_t *)in);
        in += 2;
        topic.assign(in, length);
        in += length;

        // create time
        create_time.time_and_version = *(uint64_t *)in;
        in += 8;
        create_time.clock_seq_and_node = *(uint64_t *)in;
        in += 8;

        // sender
        length = *(uint8_t *)in;
        in++;
        sender.assign(in, length);
        in += length;

        // content
        length = be16toh(*(uint16_t *)in);
        in += 2;
        content.assign(in, length);
        in += length;

        return in;
    }

    std::string topic;
    CassUuid create_time;
    std::string sender;
    std::string content;
};

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
    bool device_online(const char *device_id, const CassInet& ip, cass_int16_t port);
    bool device_offline(const char *device_id, const CassInet& ip, cass_int16_t port);

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
