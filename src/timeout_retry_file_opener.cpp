#include "timeout_retry_file_opener.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/setting_info.hpp"
#include "httpfs_timeout_retry_settings.hpp"

namespace duckdb {

TimeoutRetryFileOpener::TimeoutRetryFileOpener(FileOpener &inner_opener_p, HttpfsOperationType operation_type_p)
    : inner_opener(inner_opener_p), operation_type(operation_type_p) {
}

SettingLookupResult TimeoutRetryFileOpener::TryGetCurrentSetting(const string &key, Value &result,
                                                                 FileOpenerInfo &info) {
	// Intercept http_timeout and http_retries to provide per-operation values
	if (key == "http_timeout") {
		// Try to get the per-operation timeout setting
		string op_timeout_key = GetTimeoutSettingName();
		if (FileOpener::TryGetCurrentSetting(&inner_opener, op_timeout_key, result, &info)) {
			// If the per-operation setting is NULL, fallback to http_timeout
			if (result.IsNull()) {
				return inner_opener.TryGetCurrentSetting(key, result, info);
			}
			// Convert from milliseconds to seconds for http_timeout
			uint64_t timeout_ms = result.GetValue<uint64_t>();
			uint64_t timeout_seconds = (timeout_ms > 0 && timeout_ms < 1000) ? 1 : (timeout_ms / 1000);
			result = Value::UBIGINT(timeout_seconds);
			// TODO(hjiang): double check the scope.
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_timeout if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result, info);
	}

	if (key == "http_retries") {
		// Try to get the per-operation retry setting
		string op_retry_key = GetRetrySettingName();
		if (FileOpener::TryGetCurrentSetting(&inner_opener, op_retry_key, result, &info)) {
			// If the per-operation setting is NULL, fallback to http_retries
			if (result.IsNull()) {
				return inner_opener.TryGetCurrentSetting(key, result, info);
			}
			// TODO(hjiang): double check the scope.
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_retries if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result, info);
	}

	// For all other settings, delegate to inner opener
	return inner_opener.TryGetCurrentSetting(key, result, info);
}

SettingLookupResult TimeoutRetryFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	FileOpenerInfo info;
	return TryGetCurrentSetting(key, result, info);
}

optional_ptr<ClientContext> TimeoutRetryFileOpener::TryGetClientContext() {
	return inner_opener.TryGetClientContext();
}

optional_ptr<DatabaseInstance> TimeoutRetryFileOpener::TryGetDatabase() {
	return inner_opener.TryGetDatabase();
}

shared_ptr<HTTPUtil> &TimeoutRetryFileOpener::GetHTTPUtil() {
	return inner_opener.GetHTTPUtil();
}

Logger &TimeoutRetryFileOpener::GetLogger() const {
	return inner_opener.GetLogger();
}

string TimeoutRetryFileOpener::GetTimeoutSettingName() const {
	switch (operation_type) {
	case HttpfsOperationType::OPEN:
		return HTTPFS_TIMEOUT_FILE_OPERATION_MS;
	case HttpfsOperationType::LIST:
		return HTTPFS_TIMEOUT_LIST_MS;
	case HttpfsOperationType::DELETE:
		return HTTPFS_TIMEOUT_DELETE_MS;
	case HttpfsOperationType::STAT:
		return HTTPFS_TIMEOUT_STAT_MS;
	case HttpfsOperationType::CREATE_DIR:
		return HTTPFS_TIMEOUT_CREATE_DIR_MS;
	default:
		throw InternalException("Unknown HttpfsOperationType in GetTimeoutSettingName: %d",
		                        static_cast<int>(operation_type));
	}
}

string TimeoutRetryFileOpener::GetRetrySettingName() const {
	switch (operation_type) {
	case HttpfsOperationType::OPEN:
		return HTTPFS_RETRIES_FILE_OPERATION;
	case HttpfsOperationType::LIST:
		return HTTPFS_RETRIES_LIST;
	case HttpfsOperationType::DELETE:
		return HTTPFS_RETRIES_DELETE;
	case HttpfsOperationType::STAT:
		return HTTPFS_RETRIES_STAT;
	case HttpfsOperationType::CREATE_DIR:
		return HTTPFS_RETRIES_CREATE_DIR;
	default:
		throw InternalException("Unknown HttpfsOperationType in GetRetrySettingName: %d",
		                        static_cast<int>(operation_type));
	}
}

} // namespace duckdb
