#include <stdio.h>
#include <assert.h>
#include <stddef.h>
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
        return nullptr;
    }

    prepared = cass_future_get_prepared(future);
    cass_future_free(future);

    return prepared;
}

bool CassClient::connect()
{
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
    if (ps_get_device_ == nullptr) {
        return false;
    }

    ps_get_notifications_ = prepare(
                "SELECT create_time, sender, content FROM messages "
                "WHERE topic = ? AND create_time > ? ORDER BY create_time ASC");
    if (ps_get_notifications_ == nullptr) {
        return false;
    }

    ps_device_online_ = prepare(
                "INSERT INTO online_devices(device_id, gate_svr_id) VALUES(?, ?)");
    if (ps_device_online_ == nullptr) {
        return false;
    }

    ps_device_offline_ = prepare(
                "DELETE FROM online_devices WHERE device_id = ? IF gate_svr_id = ?");
    if (ps_device_offline_ == nullptr) {
        return false;
    }

    return true;
}

CassClient::CassClient(const char *contact_points)
    : cluster_(cass_cluster_new()), session_(cass_session_new()),
      ps_get_device_(nullptr), ps_get_notifications_(nullptr),
      ps_device_online_(nullptr), ps_device_offline_(nullptr)
{
    cass_cluster_set_contact_points(cluster_, contact_points);
}

CassClient::~CassClient()
{
    if (ps_get_device_ != nullptr) {
        cass_prepared_free(ps_get_device_);
    }

    if (ps_get_notifications_ != nullptr) {
        cass_prepared_free(ps_get_notifications_);
    }

    if (ps_device_online_ != nullptr) {
        cass_prepared_free(ps_device_online_);
    }

    if (ps_device_offline_ != nullptr) {
        cass_prepared_free(ps_device_offline_);
    }

    cass_session_free(session_);
    cass_cluster_free(cluster_);
}

CassError CassClient::get_device(const char *device_id, Device& device)
{
    CassStatement *stmt;
    CassFuture *future;
    CassError rc;

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
        if (row != nullptr) {
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
                topics.emplace(topic, topic_length);
            }
            cass_iterator_free(itr);

            device.id = device_id;
            device.last_active_time = last_active_time;
            device.province = std::string(province, province_length);
            device.topics = std::move(topics);
        } else {
            device.id.clear();  // empty id indicate no device found
        }
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return rc;
}

CassError CassClient::get_notifications(const char *topic, const CassUuid& offset,
                            std::vector<Notification>& notifications)
{
    CassStatement *stmt;
    CassFuture *future;
    CassError rc;

    stmt = cass_prepared_bind(ps_get_notifications_);

    rc = cass_statement_bind_string(stmt, 0, topic);
    if (rc != CASS_OK) {
        cass_statement_free(stmt);
        return rc;
    }

    rc = cass_statement_bind_uuid(stmt, 1, offset);
    if (rc != CASS_OK) {
        cass_statement_free(stmt);
        return rc;
    }

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
        const CassResult *result = cass_future_get_result(future);
        CassIterator *itr = cass_iterator_from_result(result);
        notifications.clear();
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

            notifications.emplace_back(std::string(topic), create_time,
                std::string(sender, sender_length), std::string(content, content_length));
        }
        cass_iterator_free(itr);
        cass_result_free(result);
    }

    cass_future_free(future);
    cass_statement_free(stmt);

    return rc;
}

bool CassClient::device_online(const char *device_id, int64_t gate_svr_id)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    stmt = cass_prepared_bind(ps_device_online_);
    cass_statement_bind_string(stmt, 0, device_id);
    cass_statement_bind_int64(stmt, 1, gate_svr_id);

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);

    return (rc == CASS_OK);
}

bool CassClient::device_offline(const char *device_id, int64_t gate_svr_id)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;
    CassError rc;

    stmt = cass_prepared_bind(ps_device_offline_);
    cass_statement_bind_string(stmt, 0, device_id);
    cass_statement_bind_int64(stmt, 1, gate_svr_id);

    future = cass_session_execute(session_, stmt);
    rc = cass_future_error_code(future);
    cass_future_free(future);
    cass_statement_free(stmt);

    return (rc == CASS_OK);
}

void CassClient::device_offline_async(const char *device_id, int64_t gate_svr_id)
{
    CassStatement *stmt = NULL;
    CassFuture *future = NULL;

    stmt = cass_prepared_bind(ps_device_offline_);
    cass_statement_bind_string(stmt, 0, device_id);
    cass_statement_bind_int64(stmt, 1, gate_svr_id);

    future = cass_session_execute(session_, stmt);
    cass_future_free(future);
    cass_statement_free(stmt);
}

