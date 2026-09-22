#include "httpfs_timeout_retry_functions.hpp"

#include <utility>

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "httpfs_client.hpp"

namespace duckdb {

namespace {

void ClearHttpfsConnectionCache(ClientContext &context, TableFunctionInput &, DataChunk &) {
	auto &config = DBConfig::GetConfig(context);
	auto &httpfs_util = static_cast<HTTPFSUtil &>(config.GetHTTPUtil());
	httpfs_util.ClearCachedConnections();
}

unique_ptr<FunctionData> BindClearHttpfsConnectionCache(ClientContext &, TableFunctionBindInput &,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return nullptr;
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                           string description, vector<string> examples, vector<string> categories) {
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	FunctionDescription function_description;
	function_description.parameter_names = std::move(parameter_names);
	function_description.description = std::move(description);
	function_description.examples = std::move(examples);
	function_description.categories = std::move(categories);
	info.descriptions.push_back(std::move(function_description));

	loader.RegisterFunction(std::move(info));
}

} // namespace

void RegisterHttpfsTimeoutRetryFunctions(ExtensionLoader &loader) {
	TableFunction clear_connection_cache("clear_httpfs_connection_cache", {}, ClearHttpfsConnectionCache,
	                                     BindClearHttpfsConnectionCache);
	RegisterTableFunction(loader, std::move(clear_connection_cache),
	                      /*parameter_names=*/ {},
	                      /*description=*/"Clears cached HTTP connections used by the httpfs file systems.",
	                      /*examples=*/ {"SELECT * FROM clear_httpfs_connection_cache();"},
	                      /*categories=*/ {"http", "connection"});
}

} // namespace duckdb
