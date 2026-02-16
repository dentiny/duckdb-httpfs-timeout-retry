#include "catch/catch.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "timeout_retry_file_opener.hpp"

using namespace duckdb;

namespace {
constexpr uint64_t DEFAULT_TIMEOUT_MS = 30000;
constexpr uint64_t DEFAULT_RETRIES = 3;

void RegisterExtensionOptions(DBConfig &db_config) {
	db_config.AddExtensionOption("httpfs_timeout_file_operation_ms",
	                             "Timeout for file operations (open/read/write) (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_timeout_list_ms", "Timeout for listing directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_timeout_delete_ms", "Timeout for deleting files (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_timeout_stat_ms", "Timeout for stat/metadata operations (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_timeout_create_dir_ms", "Timeout for creating directories (in milliseconds)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_retries_file_operation",
	                             "Maximum number of retries for file operations (open/read/write)",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_retries_list", "Maximum number of retries for listing directories",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_retries_delete", "Maximum number of retries for deleting files",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_retries_stat", "Maximum number of retries for stat/metadata operations",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
	db_config.AddExtensionOption("httpfs_retries_create_dir", "Maximum number of retries for creating directories",
	                             LogicalType {LogicalTypeId::UBIGINT}, Value());
}
} // namespace

TEST_CASE("Test OPEN operation via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
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

TEST_CASE("Test LIST operation via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
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

TEST_CASE("Test DELETE operation via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
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

TEST_CASE("Test STAT operation via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
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

TEST_CASE("Test CREATE_DIR operation via direct opener", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
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

TEST_CASE("Test fallback to http_timeout/http_retries when per-operation setting is NULL - OPEN",
          "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(45));
	db_config.SetOptionByName("http_retries", Value::UBIGINT(7));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::OPEN);

	Value timeout_value;
	auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 45);

	Value retries_value;
	auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 7);
}

TEST_CASE("Test fallback to http_timeout/http_retries when per-operation setting is NULL - LIST",
          "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(60));
	db_config.SetOptionByName("http_retries", Value::UBIGINT(4));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::LIST);

	Value timeout_value;
	auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 60);

	Value retries_value;
	auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 4);
}

TEST_CASE("Test fallback to http_timeout/http_retries when per-operation setting is NULL - DELETE",
          "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(35));
	db_config.SetOptionByName("http_retries", Value::UBIGINT(2));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::DELETE);

	Value timeout_value;
	auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 35);

	Value retries_value;
	auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 2);
}

TEST_CASE("Test fallback to http_timeout/http_retries when per-operation setting is NULL - STAT",
          "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(25));
	db_config.SetOptionByName("http_retries", Value::UBIGINT(5));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::STAT);

	Value timeout_value;
	auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 25);

	Value retries_value;
	auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 5);
}

TEST_CASE("Test fallback to http_timeout/http_retries when per-operation setting is NULL - CREATE_DIR",
          "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(40));
	db_config.SetOptionByName("http_retries", Value::UBIGINT(6));

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

TEST_CASE("Test fallback updates when http_timeout changes", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_timeout", Value::UBIGINT(20));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::LIST);

	Value timeout_value;
	auto timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 20);

	db_config.SetOptionByName("http_timeout", Value::UBIGINT(50));

	timeout_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_timeout", timeout_value);
	REQUIRE(static_cast<bool>(timeout_result));
	REQUIRE(timeout_value.GetValue<uint64_t>() == 50);
}

TEST_CASE("Test fallback updates when http_retries changes", "[extension_settings_opener]") {
	DBConfig config;
	DuckDB db(nullptr, &config);
	DatabaseInstance &db_instance = *db.instance;
	auto &db_config = DBConfig::GetConfig(db_instance);

	RegisterExtensionOptions(db_config);
	db_config.SetOptionByName("http_retries", Value::UBIGINT(1));

	DatabaseFileOpener opener(db_instance);
	TimeoutRetryFileOpener timeout_retry_opener(opener, HttpfsOperationType::DELETE);

	Value retries_value;
	auto retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 1);

	db_config.SetOptionByName("http_retries", Value::UBIGINT(8));

	retries_result = FileOpener::TryGetCurrentSetting(&timeout_retry_opener, "http_retries", retries_value);
	REQUIRE(static_cast<bool>(retries_result));
	REQUIRE(retries_value.GetValue<uint64_t>() == 8);
}
