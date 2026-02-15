#include "catch/catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "file_system_timeout_retry_wrapper.hpp"
#include "record_file_system.hpp"
#include "timeout_retry_file_opener.hpp"

#include <cstring>

using namespace duckdb;

namespace {
constexpr uint64_t DEFAULT_TIMEOUT_MS = 30000;
constexpr uint64_t DEFAULT_RETRIES = 3;
constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
constexpr double DEFAULT_RETRY_BACKOFF = 4.0;
} // namespace

TEST_CASE("Extension settings are passed correctly", "[extension_settings]") {
	DBConfig config;
	config.options.enable_external_access = true;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	// Register extension options (normally done by extension LoadInternal)
	db_config.AddExtensionOption("httpfs_timeout_open", "Timeout for opening files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_read", "Timeout for reading files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_retries_open", "Maximum number of retries for opening files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_read", "Maximum number of retries for reading files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));

	// Create a record filesystem to capture parameters
	auto record_fs = make_uniq<RecordFileSystem>();
	RecordFileSystem *record_fs_ptr = record_fs.get();

	// Wrap the record filesystem with FileSystemTimeoutRetryWrapper
	auto wrapped_fs = make_uniq<FileSystemTimeoutRetryWrapper>(std::move(record_fs), db_instance);

	// Test file path
	string test_file = "__test_file__.txt";

	// Create a test file
	{
		auto fs = FileSystem::CreateLocal();
		FileOpenFlags flags(FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		auto handle = fs->OpenFile(test_file, flags);
		const char *data = "test data";
		handle->Write(const_cast<char *>(data), strlen(data));
	}

	SECTION("Test default timeout and retry settings") {
		// Use default settings - wrapper will create DatabaseFileOpener internally
		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, nullptr);

			REQUIRE(handle != nullptr);
		}

		// Check that parameters were recorded (should use defaults)
		auto params = record_fs_ptr->GetRecordedParams(test_file);

		// Default timeout should be 30 seconds (30000 ms / 1000)
		REQUIRE(params.timeout == 30);
		// Default retries should be 3
		REQUIRE(params.retries == 3);
	}

	SECTION("Test custom timeout and retry settings") {
		// Set custom settings via database config
		// Note: http_retry_wait_ms is the standard httpfs setting name (not httpfs_retry_wait_ms)
		db_config.SetOptionByName("httpfs_timeout_open", Value::UBIGINT(60000));
		db_config.SetOptionByName("httpfs_retries_open", Value::UBIGINT(5));
		db_config.SetOptionByName("http_retry_wait_ms", Value::UBIGINT(200));
		db_config.SetOptionByName("http_retry_backoff", Value::FLOAT(2.0));

		record_fs_ptr->ClearRecordedParams();

		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, nullptr);

			REQUIRE(handle != nullptr);
		}

		// Check that custom parameters were recorded
		auto params = record_fs_ptr->GetRecordedParams(test_file);

		// Custom timeout should be 60 seconds (60000 ms / 1000)
		REQUIRE(params.timeout == 60);
		// Custom retries should be 5
		REQUIRE(params.retries == 5);
		// Custom retry_wait_ms should be 200
		REQUIRE(params.retry_wait_ms == 200);
		// Custom retry_backoff should be 2.0
		REQUIRE(params.retry_backoff == 2.0);
	}

	SECTION("Test per-operation timeout settings") {
		// Set different timeouts for different operations
		db_config.SetOptionByName("httpfs_timeout_open", Value::UBIGINT(10000));
		db_config.SetOptionByName("httpfs_timeout_read", Value::UBIGINT(20000));
		db_config.SetOptionByName("httpfs_retries_open", Value::UBIGINT(2));
		db_config.SetOptionByName("httpfs_retries_read", Value::UBIGINT(4));

		record_fs_ptr->ClearRecordedParams();

		// Test OPEN operation - OpenFile always uses OPEN operation type
		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, nullptr);

			REQUIRE(handle != nullptr);
		}

		auto open_params = record_fs_ptr->GetRecordedParams(test_file);

		// OPEN operation should use httpfs_timeout_open (10000 ms = 10 seconds)
		REQUIRE(open_params.timeout == 10);
		REQUIRE(open_params.retries == 2);

		record_fs_ptr->ClearRecordedParams();

		// Test READ operation - manually record with READ-type opener to verify it uses correct settings
		{
			DatabaseFileOpener opener(db_instance);
			TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::READ);

			// Record params by calling RecordParams directly with READ opener
			record_fs_ptr->RecordParams(test_file, &timeout_retry_opener);
		}

		auto read_params = record_fs_ptr->GetRecordedParams(test_file);

		// READ operation should use httpfs_timeout_read (20000 ms = 20 seconds)
		REQUIRE(read_params.timeout == 20);
		REQUIRE(read_params.retries == 4);
	}

	// Cleanup
	{
		auto fs = FileSystem::CreateLocal();
		fs->RemoveFile(test_file);
	}
}
