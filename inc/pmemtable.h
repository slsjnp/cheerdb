//
// Created by sj on 22-7-24.
//

#include <cstdint>
#include <utility>
#include <string.h>

#ifndef INTERFACE_PMEMTABLE_H
#define INTERFACE_PMEMTABLE_H

#endif //INTERFACE_PMEMTABLE_H

// id int64, user_id char(128), name char(128), salary int64
// pk : id 			  //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef std::pair<uint64_t, uint64_t> uint128;

inline uint64 Uint128Low64(const uint128& x) {return x.first;}
inline uint64 Uint128High64(const uint128& x) {return x.second;}

inline uint64 Hash128to64(const uint128& x) {
    // Murmur-inspired hashing.
    const uint64 kMul = 0x9ddfea08eb382d69ULL;
    uint64 a = (Uint128Low64(x) ^ Uint128High64(x)) * kMul;
    a ^= (a >> 47);
    uint64 b = (Uint128High64(x) ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

static uint64 UNALIGNED_LOAD64(const char *p) {
    uint64 result;
    memcpy(&result, p, sizeof(result));
    return result;
}

static uint32 UNALIGNED_LOAD32(const char *p) {
    uint32 result;
    memcpy(&result, p, sizeof(result));
    return result;
}

struct PmemTable{
    uint64_t id;

    uint64_t salary;
};
