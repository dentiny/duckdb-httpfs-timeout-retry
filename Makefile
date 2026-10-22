PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=httpfs_timeout_retry
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all: format-fix
	cmake-format -i CMakeLists.txt extension_config.cmake
	cmake-format -i test/unittest/CMakeLists.txt

.PHONY: format-all

.PHONY: test_release_unit test_debug_unit test_reldebug_unit test_unit
test_release_unit:
	./build/release/extension/httpfs_timeout_retry/test/unittest/unittest_httpfs_with_retry_timeout

test_debug_unit:
	./build/debug/extension/httpfs_timeout_retry/test/unittest/unittest_httpfs_with_retry_timeout

test_reldebug_unit:
	./build/reldebug/extension/httpfs_timeout_retry/test/unittest/unittest_httpfs_with_retry_timeout

test_unit: test_release_unit
