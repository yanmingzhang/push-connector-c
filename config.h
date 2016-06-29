#ifndef CONFIG_H_
#define CONFIG_H_

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>

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

    const struct sockaddr_in& lan_addr() const { return lan_addr_; }
    const struct sockaddr_in& wan_addr() const { return wan_addr_; }
    const std::string& cassandra_addr() const { return cassandra_addr_; }

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

        int port = atoi(str_port);
        if (port <= 0 || port >= 65536) {
            return false;
        }

        addr.sin_port = htons(port);

        return true;
    }

private:
    struct sockaddr_in lan_addr_;
    struct sockaddr_in wan_addr_;
    std::string cassandra_addr_;
};

#endif // CONFIG_H
