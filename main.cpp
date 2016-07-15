#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <iostream>
#include "cassclient.h"
#include "properties.h"
#include "config.h"
#include "wan_server.h"
#include "lan_server.h"

static void *loop_run(void *arg)
{
    uv_loop_t *loop = static_cast<uv_loop_t *>(arg);

    uv_run(loop, UV_RUN_DEFAULT);

    return NULL;
}

int64_t make_net_id(const struct sockaddr_in& addr, uint16_t id)
{
    return ((int64_t)addr.sin_addr.s_addr) << 32 | ((int64_t)addr.sin_port) << 16 | (int64_t)id;
}

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

    unsigned int i;
    unsigned int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    WanServer *wan_servers[num_cores];
    uv_loop_t *loops[num_cores];

    for (i = 0; i < num_cores; ++i) {
        loops[i] = uv_loop_new();

        int64_t net_id = make_net_id(config.lan_addr(), i);
        WanServer *wan_server = new WanServer(i, &cass_client, net_id);
        wan_server->initialize(loops[i], config.wan_addr(), 512);

        wan_servers[i] = wan_server;
    }

    pthread_attr_t attr;
    cpu_set_t cpus;
    pthread_t threads[num_cores];

    pthread_attr_init(&attr);
    for (i = 0; i < num_cores; ++i) {
       CPU_ZERO(&cpus);
       CPU_SET(i, &cpus);
       pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
       assert(pthread_create(&threads[i], &attr, loop_run, loops[i]) == 0);
    }

    uv_loop_t *loop = uv_loop_new();
    LanServer *lan_server = new LanServer(0, wan_servers, num_cores);
    lan_server->initialize(loop, config.lan_addr(), 128);
    loop_run(loop);

    delete lan_server;

    for (i = 0; i < num_cores; ++i) {
        pthread_join(threads[i], NULL);
        delete wan_servers[i];
    }

    return 0;
}
