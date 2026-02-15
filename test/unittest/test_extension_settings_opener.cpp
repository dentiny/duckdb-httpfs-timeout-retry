#include "catch/catch.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "timeout_retry_file_opener.hpp"

using namespace duckdb;

namespace {
constexpr uint64_t DEFAULT_TIMEOUT_MS = 30000;
constexpr uint64_t DEFAULT_RETRIES = 3;
} // namespace

TEST_CASE("Extension settings via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	config.options.enable_external_access = true;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	db_config.AddExtensionOption("httpfs_timeout_file_operation_ms",
	                             "Timeout for file operations (open/read/write) (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_list_ms", "Timeout for listing directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_delete_ms", "Timeout for deleting files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_stat_ms", "Timeout for stat/metadata operations (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_timeout_create_dir_ms", "Timeout for creating directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_TIMEOUT_MS));
	db_config.AddExtensionOption("httpfs_retries_file_operation",
	                             "Maximum number of retries for file operations (open/read/write)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_list", "Maximum number of retries for listing directories",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_delete", "Maximum number of retries for deleting files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_stat", "Maximum number of retries for stat/metadata operations",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));
	db_config.AddExtensionOption("httpfs_retries_create_dir", "Maximum number of retries for creating directories",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));

	SECTION("Test OPEN operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_file_operation_ms", Value::UBIGINT(10000));
		db_config.SetOptionByName("httpfs_retries_file_operation", Value::UBIGINT(2));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::OPEN);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 10);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 2);
	}

	SECTION("Test LIST operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_list_ms", Value::UBIGINT(15000));
		db_config.SetOptionByName("httpfs_retries_list", Value::UBIGINT(6));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::LIST);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 15);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 6);
	}

	SECTION("Test DELETE operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_delete_ms", Value::UBIGINT(25000));
		db_config.SetOptionByName("httpfs_retries_delete", Value::UBIGINT(7));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::DELETE);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 25);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 7);
	}

	SECTION("Test STAT operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_stat_ms", Value::UBIGINT(30000));
		db_config.SetOptionByName("httpfs_retries_stat", Value::UBIGINT(5));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::STAT);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 30);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 5);
	}

	SECTION("Test CREATE_DIR operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_create_dir_ms", Value::UBIGINT(40000));
		db_config.SetOptionByName("httpfs_retries_create_dir", Value::UBIGINT(6));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::CREATE_DIR);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 40);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 6);
	}
}
