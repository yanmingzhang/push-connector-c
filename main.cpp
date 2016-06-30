#include <iostream>
#include "cassclient.h"
#include "properties.h"
#include "config.h"
#include "wan_server.h"
#include "lan_server.h"

int main(int argc, char *argv[])
{
    if (argc != 2) {
        std::cout << "Usage: push-connector-c <config file>" << std::endl;
        return 1;
    }

    Properties props(argv[1]);
    if (!props) {
        std::cerr << "Parse config file failed" << std::endl;
        return 1;
    }

    Config config(props);
    if (!config) {
        std::cerr << "Load config file failed" << std::endl;
        return 1;
    }

    CassClient cass_client(config.cassandra_addr().c_str());
    if (!cass_client.connect()) {
        std::cerr << "Try to connect cassandra cluster failed" << std::endl;
        return 1;
    }

    WanServer wan_server(0, uv_default_loop(), std::move(cass_client));
    wan_server.run(config.wan_addr());

    return 0;
}
