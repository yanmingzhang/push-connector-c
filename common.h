#ifndef COMMON_H
#define COMMON_H

#include <cassandra.h>
#include "serialization.h"
#include "byte_order.h"

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

    Notification(Notification&&) = default;
    Notification& operator=(Notification&&) = default;

    Notification() = default;

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

    template<typename Endian>
    void serialize(Serializer<Endian>& serializer) const {
        std::string::size_type length;

        // topic
        serializer.serialize<uint16_t>(topic);

        // create time
        serializer.serialize(create_time.time_and_version);
        serializer.serialize(create_time.clock_seq_and_node);

        // sender
        serializer.serialize<uint8_t>(sender);

        // content
        serializer.serialize<uint16_t>(content);
    }

    template<typename Endian>
    void deserialize(Deserializer<Endian>& deserializer) {
        // topic
        deserializer.deserialize<uint16_t>(topic);

        // create time
        deserializer.deserialize(create_time.time_and_version);
        deserializer.deserialize(create_time.clock_seq_and_node);

        // sender
        deserializer.deserialize<uint8_t>(sender);

        // content
        deserializer.deserialize<uint16_t>(topic);
    }

    std::string topic;
    CassUuid create_time;
    std::string sender;
    std::string content;
};


#endif // COMMON_H
