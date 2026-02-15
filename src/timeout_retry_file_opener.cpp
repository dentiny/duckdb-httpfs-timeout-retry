#include "timeout_retry_file_opener.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/setting_info.hpp"

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
			// Convert from milliseconds to seconds for http_timeout
			// httpfs expects timeout in seconds, so we divide by 1000
			// If timeout is less than 1000ms, we set it to 1 second minimum
			uint64_t timeout_ms = result.GetValue<uint64_t>();
			uint64_t timeout_seconds = (timeout_ms > 0 && timeout_ms < 1000) ? 1 : (timeout_ms / 1000);
			result = Value::UBIGINT(timeout_seconds);
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_timeout if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result, info);
	}

	if (key == "http_retries") {
		// Try to get the per-operation retry setting
		string op_retry_key = GetRetrySettingName();
		if (FileOpener::TryGetCurrentSetting(&inner_opener, op_retry_key, result, &info)) {
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_retries if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result, info);
	}

	// For all other settings, delegate to inner opener
	return inner_opener.TryGetCurrentSetting(key, result, info);
}

SettingLookupResult TimeoutRetryFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	// Intercept http_timeout and http_retries to provide per-operation values
	if (key == "http_timeout") {
		// Try to get the per-operation timeout setting
		string op_timeout_key = GetTimeoutSettingName();
		if (FileOpener::TryGetCurrentSetting(&inner_opener, op_timeout_key, result)) {
			// Convert from milliseconds to seconds for http_timeout
			// httpfs expects timeout in seconds, so we divide by 1000
			// If timeout is less than 1000ms, we set it to 1 second minimum
			uint64_t timeout_ms = result.GetValue<uint64_t>();
			uint64_t timeout_seconds = (timeout_ms > 0 && timeout_ms < 1000) ? 1 : (timeout_ms / 1000);
			result = Value::UBIGINT(timeout_seconds);
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_timeout if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result);
	}

	if (key == "http_retries") {
		// Try to get the per-operation retry setting
		string op_retry_key = GetRetrySettingName();
		if (FileOpener::TryGetCurrentSetting(&inner_opener, op_retry_key, result)) {
			return SettingLookupResult(SettingScope::GLOBAL);
		}
		// Fall back to original http_retries if per-operation setting not found
		return inner_opener.TryGetCurrentSetting(key, result);
	}

	// For all other settings, delegate to inner opener
	return inner_opener.TryGetCurrentSetting(key, result);
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
		return "httpfs_timeout_open_ms";
	case HttpfsOperationType::READ:
		return "httpfs_timeout_read_ms";
	case HttpfsOperationType::WRITE:
		return "httpfs_timeout_write_ms";
	case HttpfsOperationType::LIST:
		return "httpfs_timeout_list_ms";
	case HttpfsOperationType::DELETE:
		return "httpfs_timeout_delete_ms";
	case HttpfsOperationType::CONNECT:
		return "httpfs_timeout_connect_ms";
	default:
		return "httpfs_timeout_open_ms";
	}
}

string TimeoutRetryFileOpener::GetRetrySettingName() const {
	switch (operation_type) {
	case HttpfsOperationType::OPEN:
		return "httpfs_retries_open";
	case HttpfsOperationType::READ:
		return "httpfs_retries_read";
	case HttpfsOperationType::WRITE:
		return "httpfs_retries_write";
	case HttpfsOperationType::LIST:
		return "httpfs_retries_list";
	case HttpfsOperationType::DELETE:
		return "httpfs_retries_delete";
	case HttpfsOperationType::CONNECT:
		return "httpfs_retries_connect";
	default:
		return "httpfs_retries_open";
	}
}

} // namespace duckdb
