#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class HttpfsTimeoutRetryExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	string Name() override;
	string Version() const override;
};

} // namespace duckdb
