cmake_minimum_required(VERSION 3.17)
project(prototype)

if(POLICY CMP0077)
    cmake_policy(SET CMP0077 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)

set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "build shared libraries")
set(WITH_TOOLS OFF CACHE BOOL "disable tools")
set(WITH_CORE_TOOLS OFF CACHE BOOL "disable core tools")
set(WITH_TESTS OFF CACHE BOOL "disable tests")
set(WITH_GFLAGS 0 CACHE BOOL "")
add_subdirectory(rocksdb)

set(BuildVelocyPackExamples OFF CACHE BOOL "")
set(Maintainer OFF CACHE BOOL "")
add_subdirectory(velocypack)

set(CMAKE_REQUIRED_INCLUDES "${CMAKE_SOURCE_DIR}/asio/asio/include")
set(USE_STANDALONE_ASIO ON CACHE BOOL "")
add_subdirectory(simple-web-server)
target_include_directories(simple-web-server INTERFACE "asio/asio/include")

add_subdirectory(function2)

set(immer_BUILD_TESTS OFF CACHE BOOL "")
set(immer_BUILD_EXAMPLES OFF CACHE BOOL "")
set(immer_BUILD_DOCS OFF CACHE BOOL "")
set(immer_BUILD_EXTRAS OFF CACHE BOOL "")
add_subdirectory(immer EXCLUDE_FROM_ALL)

add_executable(prototype main.cpp futures.h replicated-log.cpp priority-queue.h)
target_link_libraries(prototype velocypack)

target_include_directories(prototype PRIVATE rocksdb/include)
target_link_libraries(prototype rocksdb)

target_link_libraries(prototype simple-web-server)

target_link_libraries(prototype function2)
target_link_libraries(prototype immer)
