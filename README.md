# DuckDB HTTPFS Timeout & Retry Extension

A DuckDB extension that provides per-operation timeout and retry configuration for HTTP file system operations (HTTP, S3, HuggingFace). This extension extends the standard `httpfs` extension with fine-grained control over timeout and retry behavior for different types of operations.

## Usage

```sql
FORCE INSTALL httpfs_timeout_retry FROM community;
LOAD httpfs_timeout_retry;
```

## Why This Extension?

The standard `httpfs` extension in DuckDB provides global timeout and retry settings (via `http_timeout` and `http_retries`) that apply uniformly to all HTTP operations. However, different operations have fundamentally different characteristics and requirements.

For example, list operation usually does range query, and should be expectedly taking more time than point query stat call.
Similarly, retry behavior should differ by operation type.
This extension solves the problem by allowing users to configure different settings for different operations.

This extension is **100% compatible** with the standard `httpfs` extension, so users don't need to change their code.
By default, all per-operation settings are `NULL` and automatically fallback to `http_timeout` and `http_retries` from the httpfs extension, ensuring seamless compatibility.

## Configuration

### Per-Operation Timeout Settings

Configure timeouts (in milliseconds) for each operation type. By default, all per-operation timeout settings are `NULL` and will fallback to the `http_timeout` setting from the httpfs extension.

```sql
-- File operations (open, read, write), which applies to all file operations.
SET httpfs_timeout_file_operation_ms = 30000;      -- 30 seconds

-- List operations
SET httpfs_timeout_list_ms = 60000;      -- 60 seconds

-- Delete operations
SET httpfs_timeout_delete_ms = 30000;    -- 30 seconds

-- Stat/metadata operations
SET httpfs_timeout_stat_ms = 10000;      -- 10 seconds

-- Directory creation operations
SET httpfs_timeout_create_dir_ms = 20000; -- 20 seconds

-- If a per-operation timeout is not set (NULL), it falls back to http_timeout
SET http_timeout = 30000;  -- This will be used for operations without per-operation settings
```

### Per-Operation Retry Settings

Configure maximum retries for each operation type. By default, all per-operation retry settings are `NULL` and will fallback to the `http_retries` setting from the httpfs extension.

```sql
-- Retry counts for each operation
SET httpfs_retries_file_operation = 3;  -- applies to open, read, and write operations
SET httpfs_retries_list = 3;
SET httpfs_retries_delete = 2;
SET httpfs_retries_stat = 2;
SET httpfs_retries_create_dir = 3;

-- If a per-operation retry is not set (NULL), it falls back to http_retries
SET http_retries = 3;  -- This will be used for operations without per-operation settings
```

### Fallback Behavior

When a per-operation setting is `NULL` (the default), the extension automatically falls back to the corresponding httpfs extension setting:
- `httpfs_timeout_*_ms` settings fallback to `http_timeout` (converted from seconds to milliseconds)
- `httpfs_retries_*` settings fallback to `http_retries`

This means you can:
- Set `http_timeout` and `http_retries` globally, and all operations will use these values
- Override specific operations by setting their per-operation settings
- Change `http_timeout`/`http_retries` and have all non-overridden operations automatically use the new values

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to this extension.

## License

See [LICENSE](LICENSE) for license information.
