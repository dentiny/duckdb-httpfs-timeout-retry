#pragma once

namespace duckdb {

// Timeout setting names (in milliseconds)
inline constexpr const char *HTTPFS_TIMEOUT_OPEN_MS = "httpfs_timeout_open_ms";
inline constexpr const char *HTTPFS_TIMEOUT_READ_MS = "httpfs_timeout_read_ms";
inline constexpr const char *HTTPFS_TIMEOUT_WRITE_MS = "httpfs_timeout_write_ms";
inline constexpr const char *HTTPFS_TIMEOUT_LIST_MS = "httpfs_timeout_list_ms";
inline constexpr const char *HTTPFS_TIMEOUT_DELETE_MS = "httpfs_timeout_delete_ms";
inline constexpr const char *HTTPFS_TIMEOUT_STAT_MS = "httpfs_timeout_stat_ms";
inline constexpr const char *HTTPFS_TIMEOUT_CREATE_DIR_MS = "httpfs_timeout_create_dir_ms";

// Retry setting names
inline constexpr const char *HTTPFS_RETRIES_OPEN = "httpfs_retries_open";
inline constexpr const char *HTTPFS_RETRIES_READ = "httpfs_retries_read";
inline constexpr const char *HTTPFS_RETRIES_WRITE = "httpfs_retries_write";
inline constexpr const char *HTTPFS_RETRIES_LIST = "httpfs_retries_list";
inline constexpr const char *HTTPFS_RETRIES_DELETE = "httpfs_retries_delete";
inline constexpr const char *HTTPFS_RETRIES_STAT = "httpfs_retries_stat";
inline constexpr const char *HTTPFS_RETRIES_CREATE_DIR = "httpfs_retries_create_dir";

} // namespace duckdb
