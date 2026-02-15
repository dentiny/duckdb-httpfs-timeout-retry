# DuckDB HTTPFS Timeout & Retry Extension

A DuckDB extension that provides **per-operation timeout and retry configuration** for HTTP file system operations (HTTP, S3, HuggingFace). This extension extends the standard `httpfs` extension with fine-grained control over timeout and retry behavior for different types of operations.

## Usage

```sql
FORCE INSTALL httpfs_timeout_retry FROM community;
LOAD httpfs_timeout_retry;
```

## Why This Extension?

The standard `httpfs` extension in DuckDB provides global timeout and retry settings (via `http_timeout` and `http_retries`) that apply uniformly to **all** HTTP operations. However, different operations have fundamentally different characteristics and requirements.

For example,
- list operation usually does range query, and should be expectedly taking more time than point query stat call
- read request could potentially be served with cache and don't go through any data persistent layer, while it's non-avoidable for write operations

Similarly, retry behavior should differ by operation type.
This extension solves the problem by allowing users to configure different settings for different operations.

## Configuration

### Per-Operation Timeout Settings

Configure timeouts (in milliseconds) for each operation type:

```sql
-- File opening operations
SET httpfs_timeout_open_ms = 30000;      -- 30 seconds

-- Read operations
SET httpfs_timeout_read_ms = 45000;      -- 45 seconds

-- Write operations
SET httpfs_timeout_write_ms = 120000;    -- 120 seconds

-- List operations
SET httpfs_timeout_list_ms = 60000;      -- 60 seconds

-- Delete operations
SET httpfs_timeout_delete_ms = 30000;    -- 30 seconds
```

### Per-Operation Retry Settings

Configure maximum retries for each operation type:

```sql
-- Retry counts for each operation
SET httpfs_retries_open = 3;
SET httpfs_retries_read = 5; -- More retries for reads (idempotent, may hit cache)
SET httpfs_retries_write = 3;
SET httpfs_retries_list = 3;
SET httpfs_retries_delete = 2;
SET httpfs_retries_connect = 5;
```

## Compatibility

This extension is **100% compatible** with the standard `httpfs` extension. It:

- Automatically loads the `httpfs` extension if not already loaded
- Maintains backward compatibility with existing `http_timeout` and `http_retries` settings
- Falls back to global settings if per-operation settings are not configured

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to this extension.

## License

See [LICENSE](LICENSE) for license information.
