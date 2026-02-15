#define DUCKDB_EXTENSION_MAIN

#include "httpfs_timeout_retry_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

namespace {

// Default values matching httpfs extension for compatibility
constexpr uint64_t DEFAULT_TIMEOUT_MS = HTTPParams::DEFAULT_TIMEOUT_SECONDS * 1000;
constexpr uint64_t DEFAULT_RETRIES = HTTPParams::DEFAULT_RETRIES;
constexpr uint64_t DEFAULT_RETRY_WAIT_MS = HTTPParams::DEFAULT_RETRY_WAIT_MS;
constexpr float DEFAULT_RETRY_BACKOFF = HTTPParams::DEFAULT_RETRY_BACKOFF;

void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	// Timeout settings for different HTTP operations (in milliseconds)
	config.AddExtensionOption("httpfs_timeout_open", "Timeout for opening files (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	config.AddExtensionOption("httpfs_timeout_read", "Timeout for reading files (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	config.AddExtensionOption("httpfs_timeout_write", "Timeout for writing files (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	config.AddExtensionOption("httpfs_timeout_list", "Timeout for listing directories (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	config.AddExtensionOption("httpfs_timeout_delete", "Timeout for deleting files (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	config.AddExtensionOption("httpfs_timeout_connect", "Timeout for establishing connections (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));

	// Retry settings for different HTTP operations
	config.AddExtensionOption("httpfs_retries_open", "Maximum number of retries for opening files",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	config.AddExtensionOption("httpfs_retries_read", "Maximum number of retries for reading files",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	config.AddExtensionOption("httpfs_retries_write", "Maximum number of retries for writing files",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	config.AddExtensionOption("httpfs_retries_list", "Maximum number of retries for listing directories",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	config.AddExtensionOption("httpfs_retries_delete", "Maximum number of retries for deleting files",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	config.AddExtensionOption("httpfs_retries_connect", "Maximum number of retries for establishing connections",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));

	// Global retry behavior settings
	config.AddExtensionOption("httpfs_retry_wait_ms", "Initial wait time between retries (in milliseconds)",
	                          LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRY_WAIT_MS));
	config.AddExtensionOption("httpfs_retry_backoff", "Backoff factor for exponentially increasing retry wait time",
	                          LogicalType {LogicalTypeId::FLOAT}, Value::FLOAT(DEFAULT_RETRY_BACKOFF));
	config.AddExtensionOption("httpfs_retry_on_timeout", "Whether to retry on timeout errors",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value::BOOLEAN(true));
	config.AddExtensionOption("httpfs_retry_on_connection_error", "Whether to retry on connection errors",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value::BOOLEAN(true));
}

} // namespace

void HttpfsTimeoutRetryExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HttpfsTimeoutRetryExtension::Name() {
	return "httpfs_timeout_retry";
}

std::string HttpfsTimeoutRetryExtension::Version() const {
#ifdef EXT_VERSION_HTTPFS_TIMEOUT_RETRY
	return EXT_VERSION_HTTPFS_TIMEOUT_RETRY;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(httpfs_timeout_retry, loader) {
	duckdb::LoadInternal(loader);
}
}
