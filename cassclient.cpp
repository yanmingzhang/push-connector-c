#include <stdio.h>
#include <assert.h>
#include <unordered_set>
#include "cassclient.h"

static void print_error(CassFuture *future)
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
                "SELECT last_active_time, province, topics FROM devices WHERE device_id = ?");
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
}

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

CassError CassClient::get_device(const char *device_id, Device **device)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    *device = NULL;

    stmt = cass_prepared_bind(ps_get_device_);
    rc = cass_statement_bind_string(stmt, 0, device_id);
    if (rc != CASS_OK) {
        cass_statement_free(stmt);
        return rc;
    }
    
    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
        const CassResult *result = cass_future_get_result(future);
        const CassRow *row = cass_result_first_row(result);
        if (row != NULL) {
            cass_int64_t last_active_time;
            const char *province;
            size_t province_length;
            std::unordered_set<std::string> topics;

            cass_value_get_int64(cass_row_get_column(row, 0), &last_active_time);
            cass_value_get_string(cass_row_get_column(row, 1), &province, &province_length);

            CassIterator *itr = cass_iterator_from_collection(cass_row_get_column(row, 2));
            while (cass_iterator_next(itr)) {
                const char *topic;
                size_t topic_length;

                cass_value_get_string(cass_iterator_get_value(itr), &topic, &topic_length);
                topics.insert(std::string(topic, topic_length));
            }
            cass_iterator_free(itr);

            *device = new Device(device_id, last_active_time, std::string(province, province_length), topics);
        }
        cass_result_free(result);
    }
    
    if (future != NULL)
        cass_future_free(future);
    if (stmt != NULL)
        cass_statement_free(stmt);

    return rc;
}

CassError CassClient::get_notifications(const char *topic, const CassUuid& offset,
                            std::vector<Notification> notifications)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    stmt = cass_prepared_bind(ps_get_notifications_);
    cass_statement_bind_string(stmt, 0, topic);
    cass_statement_bind_uuid(stmt, 1, offset);

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
        const CassResult *result = cass_future_get_result(future);
        CassIterator *itr = cass_iterator_from_result(result);
        while (cass_iterator_next(itr)) {
            CassUuid create_time;
            const char *sender;
            size_t sender_length;
            const char *content;
            size_t content_length;

            const CassRow *row = cass_iterator_get_row(itr);

            cass_value_get_uuid(cass_row_get_column(row, 0), &create_time);
            cass_value_get_string(cass_row_get_column(row, 1), &sender, &sender_length);
            cass_value_get_string(cass_row_get_column(row, 2), &content, &content_length);

            Notification notification(topic, create_time, std::string(sender, sender_length),
                                      std::string(content, content_length));
            notifications.push_back(notification);
        }
        cass_iterator_free(itr);
        cass_result_free(result);
    }

    if (future != NULL)
        cass_future_free(future);
    if (stmt != NULL)
        cass_statement_free(stmt);

    return rc;
}

CassError CassClient::device_online(const char *device_id, const CassInet& ip, cass_int16_t port)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    stmt = cass_prepared_bind(ps_device_online_);
    cass_statement_bind_string(stmt, 0, device_id);
    cass_statement_bind_inet(stmt, 1, ip);
    cass_statement_bind_int16(stmt, 2, port);

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);

    return rc;
}

CassError CassClient::device_offline(const char *device_id, const CassInet& ip, cass_int16_t port)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    stmt = cass_prepared_bind(ps_device_offline_);
    cass_statement_bind_string(stmt, 0, device_id);
    cass_statement_bind_inet(stmt, 1, ip);
    cass_statement_bind_int16(stmt, 2, port);

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);

    return rc;
}
