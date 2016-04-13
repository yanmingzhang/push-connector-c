#include <stdio.h>
#include <assert.h>
#include "cassclient.h"

static void print_error(const CassFuture *future)
{
    const char *message;
    size_t length;
        
    cass_future_error_message(future, &message, &length);
    fprintf(stderr, "Error: %.*s\n", (int)length, message);
}

const CassPrepared *CassClient::prepare(const char *cql)
{
    CassFuture *future;
    CassError rc;
    const CassPrepared *prepared;
    
    future = cass_session_prepare(session_, cql);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
        cass_future_free(future);
        return NULL;
    }
    
    prepared = cass_future_get_prepared(future);
    cass_future_free(future);
    
    return prepared;    
}

bool CassClient::connect()
{
    CassCluster *cluster;
    CassFuture *future;
    CassError rc;
    
    future = cass_session_connect_keyspace(session_, cluster_, "push");
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
        cass_future_free(future);
        return false;
    }
    
    cass_future_free(future);
    
    ps_get_device_ = prepare(
                "SELECT last_active_time, province, topics FROM devices WHERE device_id = ?")
    if (ps_get_device_ == NULL) {
        return false;
    }
    
    ps_get_notifications_ = prepare(
                "SELECT create_time, sender, content FROM messages "
                "WHERE topic = ? AND create_time > ? ORDER BY create_time ASC");
    if (ps_get_notifications_ == NULL) {
        return false;
    }
    
    ps_device_online_ = prepare(
                "INSERT INTO online_devices(device_id, gate_svr_ip, gate_svr_port) VALUES(?, ?, ?)");
    if (ps_device_online_ == NULL) {
        return false;
    }
    
    ps_device_offline_ = prepare(
                "DELETE FROM online_devices WHERE device_id = ? IF gate_svr_ip = ? AND gate_svr_port = ?");
    if (ps_device_offline_ == NULL) {
        return false;
    }
    
    return true;
)

CassClient::CassClient(const char *contact_points)
    : cluster_(cass_cluster_new()), session_(cass_session_new()),
      ps_get_device_(NULL), ps_get_notifications_(NULL),
      ps_device_online_(NULL), ps_device_offline_(NULL)
{
    cass_cluster_set_contact_points(cluster_, contact_points);
}

CassClient::~CassClient()
{
    if (ps_get_device_ != NULL) {
        cass_prepared_free(ps_get_device_);
    }
    
    if (ps_get_notifications_ != NULL) {
        cass_prepared_free(ps_get_notifications_);
    }
    
    if (ps_device_online_ != NULL) {
        cass_prepared_free(ps_device_online_);
    }
    
    if (ps_device_offline_ != NULL) {
        cass_prepared_free(ps_device_offline_);
    }
    
    cass_session_free(session_);
    cass_cluster_free(cluster_);
}

bool CassClient::get_device(device_t& device)
{
    CassStatement *stmt = cass_prepared_bind(ps_get_device_);
    cass_statement_bind_string(stmt, 0, device.id.c_str());
    
    CassFuture *future = cass_session_execute(session_, stmt);
    CassError rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
        const CassResult *result = cass_future_get_result(future);
        const CassRow *row = cass_result_first_row(result);
        cass_value_get_cass_row_get_column(row, 1);
        
        
        CassIterator *iterator = cass_iterator_from_result(result);
        
    } else {
        print_error(future);
    }
    
    
    cass_future_free(future);
    
    
    
    cass_statement_free(stmt);
}

std::vector<notification_t> get_notifications(std::string& topic, CassUuid offset);
bool device_online(std::string device_id, std::string ip, int port);
bool device_offline(std::string device_id, std::string ip, int port); 

