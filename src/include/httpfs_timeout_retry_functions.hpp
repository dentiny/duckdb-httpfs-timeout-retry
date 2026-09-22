#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterHttpfsTimeoutRetryFunctions(ExtensionLoader &loader);

} // namespace duckdb
