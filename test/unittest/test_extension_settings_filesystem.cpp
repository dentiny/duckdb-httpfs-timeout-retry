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
	db_config.AddExtensionOption("httpfs_timeout_stat_ms", "Timeout for stat/metadata operations (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_create_dir_ms", "Timeout for creating directories (in milliseconds)",
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
	db_config.AddExtensionOption("httpfs_retries_stat", "Maximum number of retries for stat/metadata operations",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_create_dir", "Maximum number of retries for creating directories",
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

	SECTION("Test DirectoryExists operation (STAT)") {
		db_config.SetOptionByName("httpfs_timeout_stat_ms", Value::UBIGINT(12000));
		db_config.SetOptionByName("httpfs_retries_stat", Value::UBIGINT(3));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->DirectoryExists(test_dir.GetPath(), nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 12);
		REQUIRE(params.retries == 3);
	}

	SECTION("Test CreateDirectory operation (CREATE_DIR)") {
		string new_dir = "/tmp/__test_new_dir__";
		db_config.SetOptionByName("httpfs_timeout_create_dir_ms", Value::UBIGINT(18000));
		db_config.SetOptionByName("httpfs_retries_create_dir", Value::UBIGINT(4));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->CreateDirectory(new_dir, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 18);
		REQUIRE(params.retries == 4);

		// Cleanup
		wrapped_fs->RemoveDirectory(new_dir, nullptr);
	}

	SECTION("Test CreateDirectoriesRecursive operation (CREATE_DIR)") {
		string new_dir = "/tmp/__test_nested_dir__/level1/level2";
		db_config.SetOptionByName("httpfs_timeout_create_dir_ms", Value::UBIGINT(22000));
		db_config.SetOptionByName("httpfs_retries_create_dir", Value::UBIGINT(5));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->CreateDirectoriesRecursive(new_dir, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 22);
		REQUIRE(params.retries == 5);

		// Cleanup
		wrapped_fs->RemoveDirectory("/tmp/__test_nested_dir__", nullptr);
	}

	SECTION("Test RemoveDirectory operation (DELETE)") {
		string new_dir = "/tmp/__test_remove_dir__";
		wrapped_fs->CreateDirectory(new_dir, nullptr);
		db_config.SetOptionByName("httpfs_timeout_delete_ms", Value::UBIGINT(28000));
		db_config.SetOptionByName("httpfs_retries_delete", Value::UBIGINT(6));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->RemoveDirectory(new_dir, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 28);
		REQUIRE(params.retries == 6);
	}

	SECTION("Test MoveFile operation (WRITE)") {
		string source_file = "__test_move_source__.txt";
		string target_file = "__test_move_target__.txt";
		{
			auto fs = FileSystem::CreateLocal();
			FileOpenFlags flags(FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
			auto handle = fs->OpenFile(source_file, flags);
			const char *data = "move test data";
			handle->Write(const_cast<char *>(data), strlen(data));
			handle->Sync();
		}

		db_config.SetOptionByName("httpfs_timeout_write_ms", Value::UBIGINT(35000));
		db_config.SetOptionByName("httpfs_retries_write", Value::UBIGINT(8));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->MoveFile(source_file, target_file, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 35);
		REQUIRE(params.retries == 8);

		// Cleanup
		wrapped_fs->RemoveFile(target_file, nullptr);
	}

	SECTION("Test FileExists operation (STAT)") {
		db_config.SetOptionByName("httpfs_timeout_stat_ms", Value::UBIGINT(14000));
		db_config.SetOptionByName("httpfs_retries_stat", Value::UBIGINT(2));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->FileExists(test_file, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 14);
		REQUIRE(params.retries == 2);
	}

	SECTION("Test IsPipe operation (STAT)") {
		db_config.SetOptionByName("httpfs_timeout_stat_ms", Value::UBIGINT(16000));
		db_config.SetOptionByName("httpfs_retries_stat", Value::UBIGINT(3));
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->IsPipe(test_file, nullptr);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 16);
		REQUIRE(params.retries == 3);
	}

	SECTION("Test Glob operation (LIST)") {
		db_config.SetOptionByName("httpfs_timeout_list_ms", Value::UBIGINT(20000));
		db_config.SetOptionByName("httpfs_retries_list", Value::UBIGINT(4));
		DatabaseFileOpener database_opener(db_instance);
		record_fs_ptr->ClearRecordedParams();
		wrapped_fs->Glob(test_dir.GetPath() + "/*", &database_opener);

		auto all_params = record_fs_ptr->GetAllRecordedParams();
		REQUIRE(!all_params.empty());
		auto params = all_params.begin()->second;
		REQUIRE(params.timeout == 20);
		REQUIRE(params.retries == 4);
	}
}
