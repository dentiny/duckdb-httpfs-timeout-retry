#include "catch/catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "file_system_timeout_retry_wrapper.hpp"
#include "record_file_system.hpp"
#include "scoped_directory.hpp"
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
	db_config.AddExtensionOption("httpfs_timeout_write", "Timeout for writing files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_list", "Timeout for listing directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_delete", "Timeout for deleting files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_retries_open", "Maximum number of retries for opening files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_read", "Maximum number of retries for reading files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_write", "Maximum number of retries for writing files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_list", "Maximum number of retries for listing directories",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_delete", "Maximum number of retries for deleting files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));

	// Create a record filesystem to capture parameters
	auto record_fs = make_uniq<RecordFileSystem>();
	RecordFileSystem *record_fs_ptr = record_fs.get();

	// Wrap the record filesystem with FileSystemTimeoutRetryWrapper
	auto wrapped_fs = make_uniq<FileSystemTimeoutRetryWrapper>(std::move(record_fs), db_instance);

	// Test file paths
	string test_file = "__test_file__.txt";
	string test_file2 = "__test_file2__.txt";
	ScopedDirectory test_dir("/tmp/__test_dir__");

	// Create test files
	{
		auto fs = FileSystem::CreateLocal();
		FileOpenFlags flags(FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		auto handle = fs->OpenFile(test_file, flags);
		const char *data = "test data";
		handle->Write(const_cast<char *>(data), strlen(data));

		// Create second test file
		auto handle2 = fs->OpenFile(test_file2, flags);
		handle2->Write(const_cast<char *>(data), strlen(data));
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

	SECTION("Test LIST operation") {
		// Set custom settings for LIST operation
		db_config.SetOptionByName("httpfs_timeout_list", Value::UBIGINT(15000));
		db_config.SetOptionByName("httpfs_retries_list", Value::UBIGINT(6));

		record_fs_ptr->ClearRecordedParams();

		// Test LIST operation
		{
			wrapped_fs->ListFiles(test_dir.GetPath(), [](const string &, bool) {}, nullptr);
		}

		// Check that LIST parameters were recorded
		// Note: ListFiles doesn't record per-file, so we check if any params were recorded
		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());

		// Get params from any recorded entry (ListFiles may record directory path)
		auto list_params = all_params.begin()->second;

		// LIST operation should use httpfs_timeout_list (15000 ms = 15 seconds)
		REQUIRE(list_params.timeout == 15);
		REQUIRE(list_params.retries == 6);
	}

	SECTION("Test DELETE operation") {
		// Set custom settings for DELETE operation
		db_config.SetOptionByName("httpfs_timeout_delete", Value::UBIGINT(25000));
		db_config.SetOptionByName("httpfs_retries_delete", Value::UBIGINT(7));

		record_fs_ptr->ClearRecordedParams();

		// Test DELETE operation
		{
			wrapped_fs->RemoveFile(test_file2, nullptr);
		}

		// Check that DELETE parameters were recorded
		auto delete_params = record_fs_ptr->GetRecordedParams(test_file2);

		// DELETE operation should use httpfs_timeout_delete (25000 ms = 25 seconds)
		REQUIRE(delete_params.timeout == 25);
		REQUIRE(delete_params.retries == 7);
	}

	SECTION("Test WRITE operation") {
		// Set custom settings for WRITE operation
		db_config.SetOptionByName("httpfs_timeout_write", Value::UBIGINT(35000));
		db_config.SetOptionByName("httpfs_retries_write", Value::UBIGINT(8));

		record_fs_ptr->ClearRecordedParams();

		// Test WRITE operation - manually record with WRITE-type opener
		{
			DatabaseFileOpener opener(db_instance);
			TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::WRITE);

			// Record params by calling RecordParams directly with WRITE opener
			record_fs_ptr->RecordParams(test_file, &timeout_retry_opener);
		}

		auto write_params = record_fs_ptr->GetRecordedParams(test_file);

		// WRITE operation should use httpfs_timeout_write (35000 ms = 35 seconds)
		REQUIRE(write_params.timeout == 35);
		REQUIRE(write_params.retries == 8);
	}

	// Cleanup test files (directory is cleaned up automatically by ScopedDirectory)
	{
		auto fs = FileSystem::CreateLocal();
		if (fs->FileExists(test_file)) {
			fs->RemoveFile(test_file);
		}
		if (fs->FileExists(test_file2)) {
			fs->RemoveFile(test_file2);
		}
	}
}
