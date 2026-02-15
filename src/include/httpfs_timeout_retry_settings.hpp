#pragma once

namespace duckdb {

// Timeout setting names (in milliseconds)
inline constexpr const char *HTTPFS_TIMEOUT_FILE_OPERATION_MS = "httpfs_timeout_file_operation_ms";
inline constexpr const char *HTTPFS_TIMEOUT_LIST_MS = "httpfs_timeout_list_ms";
inline constexpr const char *HTTPFS_TIMEOUT_DELETE_MS = "httpfs_timeout_delete_ms";
inline constexpr const char *HTTPFS_TIMEOUT_STAT_MS = "httpfs_timeout_stat_ms";
inline constexpr const char *HTTPFS_TIMEOUT_CREATE_DIR_MS = "httpfs_timeout_create_dir_ms";

// Retry setting names
inline constexpr const char *HTTPFS_RETRIES_FILE_OPERATION = "httpfs_retries_file_operation";
inline constexpr const char *HTTPFS_RETRIES_LIST = "httpfs_retries_list";
inline constexpr const char *HTTPFS_RETRIES_DELETE = "httpfs_retries_delete";
inline constexpr const char *HTTPFS_RETRIES_STAT = "httpfs_retries_stat";
inline constexpr const char *HTTPFS_RETRIES_CREATE_DIR = "httpfs_retries_create_dir";

} // namespace duckdb
