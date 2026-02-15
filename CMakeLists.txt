cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME httpfs_timeout_retry)

set(CMAKE_CXX_STANDARD 14)

if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-write-strings -Wno-c++17-extensions -Wno-c++20-extensions -Wunused-variable -Wunused-function -Wunused-but-set-variable"
  )
endif()

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duckdb-httpfs/src/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/file_system_timeout_retry_wrapper.cpp
    src/httpfs_timeout_retry_extension.cpp
    src/record_file_system.cpp
    src/scoped_directory.cpp
    src/timeout_retry_file_opener.cpp
    duckdb-httpfs/src/create_secret_functions.cpp
    duckdb-httpfs/src/crypto.cpp
    duckdb-httpfs/src/hash_functions.cpp
    duckdb-httpfs/src/hffs.cpp
    duckdb-httpfs/src/http_state.cpp
    duckdb-httpfs/src/httpfs.cpp
    duckdb-httpfs/src/httpfs_curl_client.cpp
    duckdb-httpfs/src/httpfs_extension.cpp
    duckdb-httpfs/src/httpfs_httplib_client.cpp
    duckdb-httpfs/src/s3fs.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
