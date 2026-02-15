#include "catch/catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
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

TEST_CASE("Extension settings via filesystem operations", "[extension_settings_filesystem]") {
	DBConfig config;
	config.options.enable_external_access = true;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	db_config.AddExtensionOption("httpfs_timeout_open_ms", "Timeout for opening files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_read_ms", "Timeout for reading files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_write_ms", "Timeout for writing files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_list_ms", "Timeout for listing directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_delete_ms", "Timeout for deleting files (in milliseconds)",
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

	auto record_fs = make_uniq<RecordFileSystem>();
	RecordFileSystem *record_fs_ptr = record_fs.get();
	auto wrapped_fs = make_uniq<FileSystemTimeoutRetryWrapper>(std::move(record_fs), db_instance);

	string test_file = "__test_file__.txt";
	string test_file2 = "__test_file2__.txt";
	ScopedDirectory test_dir("/tmp/__test_dir__");

	{
		auto fs = FileSystem::CreateLocal();
		FileOpenFlags flags(FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		auto handle1 = fs->OpenFile(test_file, flags);
		const char *data = "test data";
		handle1->Write(const_cast<char *>(data), strlen(data));
		handle1->Sync();

		auto handle2 = fs->OpenFile(test_file2, flags);
		handle2->Write(const_cast<char *>(data), strlen(data));
		handle2->Sync();
	}

	SECTION("Test default timeout and retry settings") {
		record_fs_ptr->ClearRecordedParams();
		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, /*opener=*/nullptr);
			REQUIRE(handle != nullptr);
		}

		auto params = record_fs_ptr->GetRecordedParams(test_file);
		REQUIRE(params.timeout == 30);
		REQUIRE(params.retries == 3);
	}

	SECTION("Test custom timeout and retry settings") {
		db_config.SetOptionByName("httpfs_timeout_open_ms", Value::UBIGINT(60000));
		db_config.SetOptionByName("httpfs_retries_open", Value::UBIGINT(5));
		db_config.SetOptionByName("http_retry_wait_ms", Value::UBIGINT(200));
		db_config.SetOptionByName("http_retry_backoff", Value::FLOAT(2.0));
		record_fs_ptr->ClearRecordedParams();

		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, nullptr);
			REQUIRE(handle != nullptr);
		}

		auto params = record_fs_ptr->GetRecordedParams(test_file);
		REQUIRE(params.timeout == 60);
		REQUIRE(params.retries == 5);
		REQUIRE(params.retry_wait_ms == 200);
		REQUIRE(params.retry_backoff == 2.0);
	}

	SECTION("Test OPEN operation") {
		db_config.SetOptionByName("httpfs_timeout_open_ms", Value::UBIGINT(10000));
		db_config.SetOptionByName("httpfs_retries_open", Value::UBIGINT(2));
		record_fs_ptr->ClearRecordedParams();

		{
			FileOpenFlags flags(FileFlags::FILE_FLAGS_READ);
			auto handle = wrapped_fs->OpenFile(test_file, flags, nullptr);
			REQUIRE(handle != nullptr);
		}

		auto open_params = record_fs_ptr->GetRecordedParams(test_file);
		REQUIRE(open_params.timeout == 10);
		REQUIRE(open_params.retries == 2);
	}

	SECTION("Test LIST operation") {
		db_config.SetOptionByName("httpfs_timeout_list_ms", Value::UBIGINT(15000));
		db_config.SetOptionByName("httpfs_retries_list", Value::UBIGINT(6));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->ListFiles(test_dir.GetPath(), [](const string &, bool) {}, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());

		auto list_params = all_params.begin()->second;
		REQUIRE(list_params.timeout == 15);
		REQUIRE(list_params.retries == 6);
	}

	SECTION("Test DELETE operation") {
		db_config.SetOptionByName("httpfs_timeout_delete_ms", Value::UBIGINT(25000));
		db_config.SetOptionByName("httpfs_retries_delete", Value::UBIGINT(7));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->RemoveFile(test_file2, nullptr);

		auto delete_params = record_fs_ptr->GetRecordedParams(test_file2);
		REQUIRE(delete_params.timeout == 25);
		REQUIRE(delete_params.retries == 7);
	}
}
