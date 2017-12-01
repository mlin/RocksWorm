cmake_minimum_required(VERSION 2.8)
include(ExternalProject)

# Options. Turn on with 'cmake -Dmyvarname=ON'.
option(test "Build all tests." OFF) # Makes boolean 'test' available.

IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
ENDIF(NOT CMAKE_BUILD_TYPE)

# Make PROJECT_SOURCE_DIR, PROJECT_BINARY_DIR, and PROJECT_NAME available.
project(rocksdb-on-cloud)

ExternalProject_Add(upstream_rocksdb
    URL https://github.com/mlin/rocksdb/archive/2.8.fb-spaces.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -j8 OPT="-DNDEBUG -O2" static_lib
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
  )

ExternalProject_Get_Property(upstream_rocksdb source_dir)
set(ROCKSDB_INCLUDE_DIR ${source_dir}/include)
include_directories(${ROCKSDB_INCLUDE_DIR})
ExternalProject_Get_Property(upstream_rocksdb binary_dir)
add_library(rocksdb STATIC IMPORTED)  
set(LIBROCKSDB_A ${binary_dir}/librocksdb.a)
set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${LIBROCKSDB_A})

find_library(BZ2_LIBRARY_PATH NAMES bz2)

find_path(CURL_INCLUDE_DIR curl/curl.h HINTS "/usr/include")
find_library(CURL_LIBRARY_PATH NAMES curl libcurl HINTS "/usr/lib")
include_directories(BEFORE ${CURL_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

################################
# Normal Libraries & Executables
################################
set(COMMON_INCLUDES ${PROJECT_SOURCE_DIR}/include)
include_directories(${COMMON_INCLUDES} src)

add_library(rocksdb-on-cloud
            include/rocksdb-on-cloud/HTTP.h src/HTTP.cc
            include/rocksdb-on-cloud/BaseHTTPEnv.h src/BaseHTTPEnv.cc
            include/rocksdb-on-cloud/RocHTTPEnv.h src/RocHTTPEnv.cc
            include/rocksdb-on-cloud/GivenManifestHTTPEnv.h)
add_dependencies(rocksdb-on-cloud upstream_rocksdb)
add_executable(roc src/roc.cc)
target_link_libraries(roc -pthread jemalloc rocksdb z snappy bz2 rt rocksdb-on-cloud)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include DESTINATION . FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${ROCKSDB_INCLUDE_DIR} DESTINATION . FILES_MATCHING PATTERN "*.h")
install(TARGETS rocksdb-on-cloud DESTINATION lib)
install(FILES ${LIBROCKSDB_A} DESTINATION lib)
install(TARGETS roc DESTINATION bin)

################################
# Testing
################################
if (test)

  # http://mirkokiefer.com/blog/2013/03/cmake-by-example/
  # http://stackoverflow.com/a/9695234
  ExternalProject_Add(googletest
    URL https://github.com/google/googletest/archive/release-1.7.0.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/googletest
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
  )

  ExternalProject_Get_Property(googletest source_dir)
  include_directories(${source_dir}/include)

  ExternalProject_Get_Property(googletest binary_dir)
  add_library(gtest STATIC IMPORTED)  
  set_property(TARGET gtest PROPERTY IMPORTED_LOCATION ${binary_dir}/libgtest.a)
  add_library(gtest_main STATIC IMPORTED)
  set_property(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${binary_dir}/libgtest_main.a)

  ##############
  # Unit Tests
  ##############
  add_executable(unit_tests test/unit/HTTP_test.cc include/rocksdb-on-cloud/GivenManifestHTTPEnv.h test/unit/GivenManifestHTTPEnv_test.cc test/unit/RocHTTPEnv_test.cc)

  # Standard linking to gtest stuff.
  target_link_libraries(unit_tests gtest gtest_main -pthread)

  # Extra linking for the project.
  target_link_libraries(unit_tests jemalloc rocksdb z snappy bz2 rt rocksdb-on-cloud ${CURL_LIBRARY_PATH})

  add_test(unit_tests unit_tests)
endif()
