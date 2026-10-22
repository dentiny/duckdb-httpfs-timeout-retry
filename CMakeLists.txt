cmake_minimum_required(VERSION 3.15)

set(TARGET_NAME httpfs_timeout_retry)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wunused-variable -Wunused-function -Wunused-but-set-variable"
  )
endif()

# Override DuckDB encryption library, mimics httpfs extension behavior.
if(NOT EMSCRIPTEN)
  add_definitions(-DOVERRIDE_ENCRYPTION_UTILS=1)
else()
  set(DUCKDB_EXTENSION_HTTPFS_LINKED_LIBS
      "../../third_party/mbedtls/libduckdb_mbedtls.a")
endif()

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duckdb-httpfs/src/include)
include_directories(duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/file_system_timeout_retry_wrapper.cpp
    src/httpfs_timeout_retry_functions.cpp
    src/httpfs_timeout_retry_extension.cpp
    src/timeout_retry_file_opener.cpp
    duckdb-httpfs/src/create_secret_functions.cpp
    duckdb-httpfs/src/hffs.cpp
    duckdb-httpfs/src/httpfs_extension.cpp
    duckdb-httpfs/src/http/httpfs.cpp
    duckdb-httpfs/src/http/httpfs_transport.cpp
    duckdb-httpfs/src/http/httpfs_certificates.cpp
    duckdb-httpfs/src/http/http_request.cpp
    duckdb-httpfs/src/http/http_settings.cpp
    duckdb-httpfs/src/http/http_request_session.cpp
    duckdb-httpfs/src/http/http_state.cpp
    duckdb-httpfs/src/s3/s3_auth.cpp
    duckdb-httpfs/src/s3/s3_delete.cpp
    duckdb-httpfs/src/s3/s3_endpoint.cpp
    duckdb-httpfs/src/s3/s3_list.cpp
    duckdb-httpfs/src/s3/s3_provider.cpp
    duckdb-httpfs/src/s3/s3_request.cpp
    duckdb-httpfs/src/s3/s3_settings.cpp
    duckdb-httpfs/src/s3/s3_upload_session.cpp
    duckdb-httpfs/src/s3/s3_url.cpp
    duckdb-httpfs/src/s3/s3_xml_response.cpp
    duckdb-httpfs/src/s3/s3fs.cpp)

if(NOT EMSCRIPTEN)
  list(APPEND EXTENSION_SOURCES duckdb-httpfs/src/crypto.cpp
       duckdb-httpfs/src/http/httpfs_curl_client.cpp
       duckdb-httpfs/src/http/httpfs_httplib_client.cpp)
else()
  list(APPEND EXTENSION_SOURCES duckdb-httpfs/src/http/httpfs_client_wasm.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto
                      CURL::libcurl)

if(APPLE)
  find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
  find_library(SECURITY_FRAMEWORK Security)
  target_link_libraries(${EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK}
                        ${SECURITY_FRAMEWORK})
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK}
                        ${SECURITY_FRAMEWORK})
endif()

if(BUILD_UNITTESTS)
  add_subdirectory(test/unittest)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
