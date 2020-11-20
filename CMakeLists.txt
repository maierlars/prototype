cmake_minimum_required(VERSION 3.17)
project(prototype)

if(POLICY CMP0077)
    cmake_policy(SET CMP0077 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)

set(WITH_GFLAGS 0)
add_subdirectory(rocksdb)

set(BuildVelocyPackExamples OFF)
set(Maintainer OFF)
add_subdirectory(velocypack)

set (CMAKE_REQUIRED_INCLUDES "asio/asio/include")
set(USE_STANDALONE_ASIO ON)
add_subdirectory(simple-web-server)
target_include_directories(simple-web-server INTERFACE "asio/asio/include")

add_executable(prototype main.cpp)
target_link_libraries(prototype velocypack)

target_include_directories(prototype PRIVATE rocksdb/include)
target_link_libraries(prototype rocksdb)

target_link_libraries(prototype simple-web-server)
