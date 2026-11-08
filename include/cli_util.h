//
// cli_util.h
// 命令行工具函数：src/exchange_sim.cpp 与 tools/mock_exchange.cpp 共用的参数解析。
// Command-line helpers: argument parsing shared by src/exchange_sim.cpp and tools/mock_exchange.cpp.
//

#pragma once

#include <cerrno>
#include <cstdint>
#include <cstdlib>

namespace cli_util {

// 十进制无符号整数（strtoull）：拒绝空串、以 '+' / '-' 开头、带尾随字符或超出 uint64 范围的输入
// Decimal unsigned integer (strtoull): rejects input that is empty, starts with '+' / '-', has trailing
// characters or is out of the uint64 range
inline bool parseUnsigned(const char* text, std::uint64_t& out) {
    if (text == nullptr || *text == '\0' || *text == '-' || *text == '+') return false;
    errno = 0;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') return false;
    out = static_cast<std::uint64_t>(v);
    return true;
}

}  // namespace cli_util
