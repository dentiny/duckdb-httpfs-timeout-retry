#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

enum class HttpfsOperationType { OPEN, READ, WRITE, LIST, DELETE, STAT, CREATE_DIR };

// FileOpener wrapper that provides per-operation timeout and retry settings
class TimeoutRetryFileOpener : public FileOpener {
public:
	TimeoutRetryFileOpener(FileOpener &inner_opener_p, HttpfsOperationType operation_type_p);

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info) override;
	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) override;

	optional_ptr<ClientContext> TryGetClientContext() override;
	optional_ptr<DatabaseInstance> TryGetDatabase() override;
	shared_ptr<HTTPUtil> &GetHTTPUtil() override;
	Logger &GetLogger() const override;

	HttpfsOperationType GetOperationType() const {
		return operation_type;
	}

private:
	FileOpener &inner_opener;
	HttpfsOperationType operation_type;

	// Util to get per-operation timeout setting name
	string GetTimeoutSettingName() const;
	// Util to get per-operation retry setting name
	string GetRetrySettingName() const;
};

} // namespace duckdb
