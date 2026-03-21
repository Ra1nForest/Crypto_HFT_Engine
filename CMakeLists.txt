cmake_minimum_required(VERSION 3.16)
project(Crypto_HFT_Engine)
 
# C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
 
# Complier optimization
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
 
# Find dependencies
find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
 
# Generate executable files
add_executable(crypto_hft_engine
    src/main.cpp
    third_party/simdjson.cpp
)
 
# Head
target_include_directories(crypto_hft_engine PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party
    ${Boost_INCLUDE_DIRS}
)
 
# Link libraries
target_link_libraries(crypto_hft_engine PRIVATE
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)
 