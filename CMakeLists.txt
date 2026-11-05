cmake_minimum_required(VERSION 3.16)
project(Crypto_HFT_Engine CXX)

# C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 编译选项：所有构建类型都使用 -O2（Debug 也是优化构建）
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

option(HFT_BUILD_TESTS "Build unit and integration tests" ON)
option(HFT_BUILD_BENCHMARKS "Build benchmark programs into the build directory" ON)
set(HFT_SANITIZE "" CACHE STRING "Sanitizers to enable, e.g. address,undefined or thread")

# Sanitizer 构建：-O1 -g 放在 -O2 之后，覆盖优化级别
if(HFT_SANITIZE)
    add_compile_options(-fsanitize=${HFT_SANITIZE} -fno-omit-frame-pointer -g -O1)
    add_link_options(-fsanitize=${HFT_SANITIZE})
    # GCC 对 Boost.Asio 内部的 atomic_thread_fence 给出 -Wtsan 警告（并非本项目代码），TSan 构建时关闭
    if(HFT_SANITIZE MATCHES "thread" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        add_compile_options(-Wno-tsan)
    endif()
endif()

# 依赖
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/third_party/simdjson.cpp)
    message(FATAL_ERROR "third_party/simdjson.cpp not found: run `bash Setup.sh` first")
endif()

# simdjson 只编译一次，主程序与测试共享
add_library(simdjson STATIC third_party/simdjson.cpp)
target_include_directories(simdjson PUBLIC ${CMAKE_SOURCE_DIR}/third_party)

# 纯头文件的 HFT 基础库与撮合引擎（不依赖 Boost/simdjson）
add_library(hft_core INTERFACE)
target_include_directories(hft_core INTERFACE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(hft_core INTERFACE Threads::Threads)

# 行情模块：WebSocket/TLS + JSON 解析
add_library(market_data INTERFACE)
target_include_directories(market_data INTERFACE ${Boost_INCLUDE_DIRS})
target_link_libraries(market_data INTERFACE
    hft_core
    simdjson
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
)

# 主程序：跨交易所套利监控
add_executable(crypto_hft_engine src/main.cpp)
target_link_libraries(crypto_hft_engine PRIVATE market_data)

# 撮合引擎模拟交易所（源文件存在时才构建）
if(EXISTS ${CMAKE_SOURCE_DIR}/src/exchange_sim.cpp)
    add_executable(exchange_sim src/exchange_sim.cpp)
    target_link_libraries(exchange_sim PRIVATE hft_core)
endif()

# 离线模拟交易所（源文件存在时才构建）：三家交易所协议的 TLS WebSocket + REST 服务器，供监控程序端到端演示
if(EXISTS ${CMAKE_SOURCE_DIR}/tools/mock_exchange.cpp)
    add_executable(mock_exchange tools/mock_exchange.cpp)
    target_include_directories(mock_exchange PRIVATE ${CMAKE_SOURCE_DIR}/tools)
    target_link_libraries(mock_exchange PRIVATE market_data)
endif()

# 测试：tests/<name>.cpp 存在时注册为 CTest 用例，工作目录为仓库根目录
if(HFT_BUILD_TESTS)
    enable_testing()
    function(hft_add_test name dep)
        if(EXISTS ${CMAKE_SOURCE_DIR}/tests/${name}.cpp)
            add_executable(${name} tests/${name}.cpp)
            # tests/：测试框架与参考模型；tools/：模拟交易所（mock_exchange_server.h 等，测试与 mock_exchange 共用）
            target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/tests ${CMAKE_SOURCE_DIR}/tools)
            target_link_libraries(${name} PRIVATE ${dep})
            add_test(NAME ${name} COMMAND ${name} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
        endif()
    endfunction()
    hft_add_test(test_hft hft_core)
    hft_add_test(test_matching hft_core)
    hft_add_test(test_engine_runner hft_core)
    hft_add_test(test_market_data market_data)
    hft_add_test(test_feed_integration market_data)
    hft_add_test(test_mock_exchange market_data)
    hft_add_test(test_app market_data)
    # test_app 的信号处理测试运行同一构建目录中的主程序：先构建它，测试不会用到旧的二进制
    if(TARGET test_app)
        add_dependencies(test_app crypto_hft_engine)
    endif()
endif()

# 基准测试：输出到构建目录，不会弄脏 benchmark/
if(HFT_BUILD_BENCHMARKS)
    function(hft_add_bench name dep)
        if(EXISTS ${CMAKE_SOURCE_DIR}/benchmark/${name}.cpp)
            add_executable(${name} benchmark/${name}.cpp)
            target_link_libraries(${name} PRIVATE ${dep})
        endif()
    endfunction()
    hft_add_bench(orderbook_benchmark Threads::Threads)
    hft_add_bench(parse_benchmark Threads::Threads)
    hft_add_bench(hft_benchmark hft_core)
    hft_add_bench(matching_engine_benchmark hft_core)
    hft_add_bench(market_data_benchmark market_data)
    # 行情基准用模拟交易所的市场仿真与发布状态机（tools/mock_market.h、tools/mock_publishers.h）生成真实形状的行情流
    # 撮合基准以 tests/ 中的参考模型作对照
    if(TARGET matching_engine_benchmark)
        target_include_directories(matching_engine_benchmark PRIVATE ${CMAKE_SOURCE_DIR}/tests)
    endif()
    if(TARGET market_data_benchmark)
        target_include_directories(market_data_benchmark PRIVATE ${CMAKE_SOURCE_DIR}/tools)
    endif()
endif()
