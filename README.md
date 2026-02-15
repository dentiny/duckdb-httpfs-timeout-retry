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

Similarly, retry behavior should differ by operation type.
This extension solves the problem by allowing users to configure different settings for different operations.

This extension is **100% compatible** with the standard `httpfs` extension, so users don't need to change their code.
The default configuration on timeout and retry also defaults to `httpfs` ones.

## Configuration

### Per-Operation Timeout Settings

Configure timeouts (in milliseconds) for each operation type:

```sql
-- File opening operations
SET httpfs_timeout_open_ms = 30000;      -- 30 seconds

-- List operations
SET httpfs_timeout_list_ms = 60000;      -- 60 seconds

-- Delete operations
SET httpfs_timeout_delete_ms = 30000;    -- 30 seconds

-- Stat/metadata operations
SET httpfs_timeout_stat_ms = 10000;      -- 10 seconds

-- Directory creation operations
SET httpfs_timeout_create_dir_ms = 20000; -- 20 seconds
```

### Per-Operation Retry Settings

Configure maximum retries for each operation type:

```sql
-- Retry counts for each operation
SET httpfs_retries_open = 3;
SET httpfs_retries_list = 3;
SET httpfs_retries_delete = 2;
SET httpfs_retries_stat = 2;
SET httpfs_retries_create_dir = 3;
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to this extension.

## License

See [LICENSE](LICENSE) for license information.
