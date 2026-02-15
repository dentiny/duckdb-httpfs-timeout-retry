#include "record_file_system.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/setting_info.hpp"

namespace duckdb {

void RecordFileSystem::RecordParams(const string &path, optional_ptr<FileOpener> opener) {
	if (!opener) {
		return;
	}

	Value value;
	RecordedParams params;

	// Try to get http_timeout
	if (FileOpener::TryGetCurrentSetting(opener, "http_timeout", value)) {
		params.timeout = value.GetValue<uint64_t>();
	}

	// Try to get http_retries
	if (FileOpener::TryGetCurrentSetting(opener, "http_retries", value)) {
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

	lock_guard<mutex> lock(params_lock);
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
	lock_guard<mutex> lock(params_lock);
	auto it = recorded_params.find(path);
	if (it != recorded_params.end()) {
		return it->second;
	}
	return RecordedParams {};
}

} // namespace duckdb
