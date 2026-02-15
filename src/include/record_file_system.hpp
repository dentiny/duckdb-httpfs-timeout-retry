#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct RecordedParams {
	uint64_t timeout = 0;
	uint64_t retries = 0;
	uint64_t retry_wait_ms = 0;
	double retry_backoff = 0.0;
};

class RecordFileSystem : public LocalFileSystem {
public:
	RecordFileSystem() = default;

	// Override OpenFile to record parameters
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	// Override OpenFileExtended to record parameters
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

	// Override ListFilesExtended to record parameters
	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;

	// Override RemoveFile to record parameters
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	// Get recorded parameters for a path
	RecordedParams GetRecordedParams(const string &path) const;

	// Get all recorded parameters
	unordered_map<string, RecordedParams> GetAllRecordedParams() const {
		return recorded_params;
	}

	// Clear recorded parameters
	void ClearRecordedParams() {
		recorded_params.clear();
	}

	// Manually record parameters for testing
	void RecordParams(const string &path, optional_ptr<FileOpener> opener);

	std::string GetName() const override {
		return "RecordFileSystem";
	}

private:
	mutable unordered_map<string, RecordedParams> recorded_params;
	mutable mutex params_lock;
};

} // namespace duckdb
