#include "record_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/setting_info.hpp"
#include "timeout_retry_file_opener.hpp"

namespace duckdb {

void RecordFileSystem::RecordParams(const string &path, optional_ptr<FileOpener> opener) {
	if (path.empty()) {
		throw InvalidInputException("Path cannot be empty");
	}
	if (!opener) {
		throw InternalException("FileOpener cannot be null");
	}

	Value value;
	RecordedParams params;

	// Check if opener is a TimeoutRetryFileOpener to get per-operation settings
	auto *timeout_retry_opener = dynamic_cast<TimeoutRetryFileOpener *>(opener.get());
	if (timeout_retry_opener == nullptr) {
		throw InternalException("File opener should be timeout retry opener");
	}

	// Get the operation type and query extension-specific per-operation settings
	HttpfsOperationType operation_type = timeout_retry_opener->GetOperationType();
	string timeout_setting_name;
	string retry_setting_name;

	// Determine the per-operation setting names based on operation type
	switch (operation_type) {
	case HttpfsOperationType::OPEN:
		timeout_setting_name = "httpfs_timeout_open_ms";
		retry_setting_name = "httpfs_retries_open";
		break;
	case HttpfsOperationType::READ:
		timeout_setting_name = "httpfs_timeout_read_ms";
		retry_setting_name = "httpfs_retries_read";
		break;
	case HttpfsOperationType::WRITE:
		timeout_setting_name = "httpfs_timeout_write_ms";
		retry_setting_name = "httpfs_retries_write";
		break;
	case HttpfsOperationType::LIST:
		timeout_setting_name = "httpfs_timeout_list_ms";
		retry_setting_name = "httpfs_retries_list";
		break;
	case HttpfsOperationType::DELETE:
		timeout_setting_name = "httpfs_timeout_delete_ms";
		retry_setting_name = "httpfs_retries_delete";
		break;
	case HttpfsOperationType::CONNECT:
		timeout_setting_name = "httpfs_timeout_connect_ms";
		retry_setting_name = "httpfs_retries_connect";
		break;
	default:
		timeout_setting_name = "httpfs_timeout_open_ms";
		retry_setting_name = "httpfs_retries_open";
		break;
	}

	// Get per-operation timeout (in milliseconds) and convert to seconds
	if (FileOpener::TryGetCurrentSetting(opener, timeout_setting_name, value)) {
		const uint64_t timeout_ms = value.GetValue<uint64_t>();
		params.timeout = (timeout_ms > 0 && timeout_ms < 1000) ? 1 : (timeout_ms / 1000);
	}

	// Get per-operation retries
	if (FileOpener::TryGetCurrentSetting(opener, retry_setting_name, value)) {
		params.retries = value.GetValue<uint64_t>();
	}

	// Try to get http_retry_wait_ms
	if (FileOpener::TryGetCurrentSetting(opener, "http_retry_wait_ms", value)) {
		params.retry_wait_ms = value.GetValue<uint64_t>();
	}

	// Try to get http_retry_backoff
	if (FileOpener::TryGetCurrentSetting(opener, "http_retry_backoff", value)) {
		params.retry_backoff = value.GetValue<double>();
	}

	const lock_guard<mutex> lock(params_lock);
	recorded_params[path] = params;
}

unique_ptr<FileHandle> RecordFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                  optional_ptr<FileOpener> opener) {
	RecordParams(path, opener);
	return LocalFileSystem::OpenFile(path, flags, opener);
}

unique_ptr<FileHandle> RecordFileSystem::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                          optional_ptr<FileOpener> opener) {
	RecordParams(path.path, opener);
	return LocalFileSystem::OpenFileExtended(path, flags, opener);
}

bool RecordFileSystem::ListFilesExtended(const string &directory,
                                         const std::function<void(OpenFileInfo &info)> &callback,
                                         optional_ptr<FileOpener> opener) {
	RecordParams(directory, opener);
	return LocalFileSystem::ListFilesExtended(directory, callback, opener);
}

void RecordFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	RecordParams(filename, opener);
	LocalFileSystem::RemoveFile(filename, opener);
}

RecordedParams RecordFileSystem::GetRecordedParams(const string &path) const {
	const lock_guard<mutex> lock(params_lock);
	auto it = recorded_params.find(path);
	if (it != recorded_params.end()) {
		return it->second;
	}
	return RecordedParams {};
}

} // namespace duckdb
