//
// crc32.h
// CRC-32/ISO-HDLC（与 zlib 的 crc32 相同，反射多项式 0xEDB88320）：编译期生成查找表，slicing-by-8 实现。
// CRC-32/ISO-HDLC (same as zlib's crc32, reflected polynomial 0xEDB88320): lookup tables generated at
// compile time, slicing-by-8 implementation.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hft {

namespace detail {

// slicing-by-8 查找表：t[0] 为标准逐字节表；t[k][i] 等于"字节 i 之后再跟 k 个 0 字节"的 CRC 贡献
// slicing-by-8 lookup tables: t[0] is the standard byte-at-a-time table; t[k][i] equals the CRC
// contribution of "byte i followed by k zero bytes"
struct Crc32Tables {
    std::uint32_t t[8][256];
};

constexpr Crc32Tables makeCrc32Tables() noexcept {
    Crc32Tables tables{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        tables.t[0][i] = c;
    }
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = tables.t[0][i];
        for (int s = 1; s < 8; ++s) {
            c = tables.t[0][c & 0xFFu] ^ (c >> 8);
            tables.t[s][i] = c;
        }
    }
    return tables;
}

// 8 KB 只读表，编译期计算（C++17 inline 变量，全程序唯一一份）
// 8 KB read-only tables computed at compile time (a C++17 inline variable, one copy in the whole program)
inline constexpr Crc32Tables kCrc32Tables = makeCrc32Tables();

// 以小端序读取 4 字节（逐字节组装，编译器在小端机器上会合并为一次加载；对齐无要求）
// Reads 4 bytes in little-endian order (assembled byte by byte, which the compiler merges into one load on
// little-endian machines; no alignment requirement)
inline std::uint32_t loadLe32(const unsigned char* p) noexcept {
    return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
           (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}

}  // namespace detail

// 计算 data[0, len) 的 CRC-32。seed 为之前数据的 CRC（zlib 语义），因此
// crc32(b, nb, crc32(a, na)) == crc32(a 与 b 拼接)。crc32("123456789", 9) == 0xCBF43926。
// Computes the CRC-32 of data[0, len). seed is the CRC of the preceding data (zlib semantics), so
// crc32(b, nb, crc32(a, na)) == crc32(a concatenated with b). crc32("123456789", 9) == 0xCBF43926.
inline std::uint32_t crc32(const void* data, std::size_t len, std::uint32_t seed = 0) noexcept {
    const auto& t = detail::kCrc32Tables.t;
    const auto* p = static_cast<const unsigned char*>(data);
    std::uint32_t c = ~seed;
    while (len >= 8) {
        const std::uint32_t one = detail::loadLe32(p) ^ c;
        const std::uint32_t two = detail::loadLe32(p + 4);
        c = t[7][one & 0xFFu] ^ t[6][(one >> 8) & 0xFFu] ^ t[5][(one >> 16) & 0xFFu] ^ t[4][one >> 24] ^
            t[3][two & 0xFFu] ^ t[2][(two >> 8) & 0xFFu] ^ t[1][(two >> 16) & 0xFFu] ^ t[0][two >> 24];
        p += 8;
        len -= 8;
    }
    while (len-- > 0) c = t[0][(c ^ *p++) & 0xFFu] ^ (c >> 8);
    return ~c;
}

// 字符串重载
// String overload
inline std::uint32_t crc32(std::string_view s, std::uint32_t seed = 0) noexcept {
    return crc32(s.data(), s.size(), seed);
}

// 32 位无符号 CRC 按二进制补码解释为有符号数（OKX 订单簿 checksum 的表示方式）。
// 超出 int32 范围的值按模 2^32 回绕：GCC/Clang 的实现定义行为，C++20 起写入标准。
// Interprets the 32-bit unsigned CRC as a signed two's-complement number (the representation of the OKX
// order book checksum).
// Values beyond the int32 range wrap modulo 2^32: implementation-defined behavior of GCC/Clang, part of the
// standard since C++20.
constexpr std::int32_t toSignedCrc(std::uint32_t v) noexcept { return static_cast<std::int32_t>(v); }

// 有符号 CRC-32：crc32Signed("3366.1:7:3366.8:9:3366:6:3368:8") == -1881014294（即 2413953002u）
// Signed CRC-32: crc32Signed("3366.1:7:3366.8:9:3366:6:3368:8") == -1881014294 (i.e. 2413953002u)
inline std::int32_t crc32Signed(std::string_view s) noexcept { return toSignedCrc(crc32(s)); }

}  // namespace hft
