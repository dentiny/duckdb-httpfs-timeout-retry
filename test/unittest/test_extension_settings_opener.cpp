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
	db_config.AddExtensionOption("httpfs_timeout_connect_ms", "Timeout for establishing connections (in milliseconds)",
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
	db_config.AddExtensionOption("httpfs_retries_connect", "Maximum number of retries for establishing connections",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value::UBIGINT(DEFAULT_RETRIES));

	SECTION("Test OPEN operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_open_ms", Value::UBIGINT(10000));
		db_config.SetOptionByName("httpfs_retries_open", Value::UBIGINT(2));

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

	SECTION("Test READ operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_read_ms", Value::UBIGINT(20000));
		db_config.SetOptionByName("httpfs_retries_read", Value::UBIGINT(4));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::READ);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 20);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 4);
	}

	SECTION("Test WRITE operation via direct opener") {
		db_config.SetOptionByName("httpfs_timeout_write_ms", Value::UBIGINT(35000));
		db_config.SetOptionByName("httpfs_retries_write", Value::UBIGINT(8));

		DatabaseFileOpener opener(db_instance);
		TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::WRITE);

		Value timeout_value;
		auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
		REQUIRE(static_cast<bool>(timeout_result));
		REQUIRE(timeout_value.GetValue<uint64_t>() == 35);

		Value retries_value;
		auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
		REQUIRE(static_cast<bool>(retries_result));
		REQUIRE(retries_value.GetValue<uint64_t>() == 8);
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
}
