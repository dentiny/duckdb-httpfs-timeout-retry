#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "file_system_timeout_retry_wrapper.hpp"
#include "httpfs_timeout_retry_extension.hpp"
#include "httpfs_extension.hpp"

namespace duckdb {

namespace {

constexpr const char *HTTPFS_EXTENSION = "httpfs";

// Default values matching httpfs extension for compatibility
constexpr uint64_t DEFAULT_TIMEOUT_MS = HTTPParams::DEFAULT_TIMEOUT_SECONDS * 1000;
constexpr uint64_t DEFAULT_RETRIES = HTTPParams::DEFAULT_RETRIES;
constexpr uint64_t DEFAULT_RETRY_WAIT_MS = HTTPParams::DEFAULT_RETRY_WAIT_MS;
constexpr float DEFAULT_RETRY_BACKOFF = HTTPParams::DEFAULT_RETRY_BACKOFF;

// Whether `httpfs` extension has already been loaded.
bool IsHttpfsExtensionLoaded(DatabaseInstance &db_instance) {
	auto &extension_manager = db_instance.GetExtensionManager();
	const auto loaded_extensions = extension_manager.GetExtensions();
	return std::find(loaded_extensions.begin(), loaded_extensions.end(), HTTPFS_EXTENSION) != loaded_extensions.end();
}

// Ensure httpfs extension is loaded, loading it if necessary
void EnsureHttpfsExtensionLoaded(ExtensionLoader &loader, DatabaseInstance &instance) {
	const bool httpfs_extension_loaded = IsHttpfsExtensionLoaded(instance);
	if (!httpfs_extension_loaded) {
		auto httpfs_extension = make_uniq<HttpfsExtension>();
		httpfs_extension->Load(loader);

		// Register into extension manager to keep compatibility as httpfs.
		auto &extension_manager = ExtensionManager::Get(instance);
		auto extension_active_load = extension_manager.BeginLoad(HTTPFS_EXTENSION);
		// Manually fill in the extension install info to finalize extension load.
		ExtensionInstallInfo extension_install_info;
		extension_install_info.mode = ExtensionInstallMode::UNKNOWN;
		extension_active_load->FinishLoad(extension_install_info);
	}
}

void WrapHttpfsFileSystems(DatabaseInstance &instance) {
	auto &opener_filesystem = instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();

	// Wrap httpfs filesystems with timeout/retry wrapper
	auto http_fs = vfs.ExtractSubSystem("HTTPFileSystem");
	if (http_fs) {
		vfs.RegisterSubSystem(make_uniq<FileSystemTimeoutRetryWrapper>(std::move(http_fs), instance));
	}

	// Extract and wrap HuggingFaceFileSystem
	auto hf_fs = vfs.ExtractSubSystem("HuggingFaceFileSystem");
	if (hf_fs) {
		vfs.RegisterSubSystem(make_uniq<FileSystemTimeoutRetryWrapper>(std::move(hf_fs), instance));
	}

	// Extract and wrap S3FileSystem
	auto s3_fs = vfs.ExtractSubSystem("S3FileSystem");
	if (s3_fs) {
		vfs.RegisterSubSystem(make_uniq<FileSystemTimeoutRetryWrapper>(std::move(s3_fs), instance));
	}
}

void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	// Ensure httpfs extension is loaded to achieve 100% compatibility with httpfs extension
	EnsureHttpfsExtensionLoaded(loader, instance);

	// Wrap all httpfs filesystems with timeout/retry wrapper
	WrapHttpfsFileSystems(instance);

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
