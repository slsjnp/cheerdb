//
// Created by sj on 22-7-23.
//

#ifndef INTERFACE_WAL_H
#define INTERFACE_WAL_H

#endif //INTERFACE_WAL_H

#pragma once

#include <libpmem.h>
#include "slot.h"

#include <map>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>

typedef uint32_t baddr_t;
typedef uint16_t digest_t;

#ifdef DEBUG
static constexpr int kNumOfThreads = 4;
static constexpr size_t kMemSize = 0x80000000;
static constexpr size_t kPMemSize = 0xe0000000;
static constexpr size_t kNumOfFreeFrames = 0x80000;
#else
static constexpr int kNumOfThreads = 16;
static constexpr size_t kMemSize = 0x180000000;
static constexpr size_t kPMemSize = 0x1000000000;
static constexpr size_t kNumOfFreeFrames = 0x80000;
#endif

static constexpr int kKeySize = 8;
static constexpr int kMaxPayloadSize = 272;
static constexpr int kBlockSize = 64;
static constexpr int kCacheLineSize = 64;
static constexpr int kFreeListSize = 18;
static constexpr int kFreeFrameSize = 2048;
static constexpr int kEntriesPerFreeFrame = (kFreeFrameSize - sizeof(void *) - sizeof(uint32_t)) / sizeof(baddr_t);
static constexpr size_t kNumOfBlocks = kPMemSize / kBlockSize;
static constexpr size_t kBlocksPerPartition = kNumOfBlocks / kNumOfThreads;
static constexpr size_t kReservedBlocksPerPartition = kBlocksPerPartition >> 3;
static constexpr size_t kFreeFramesPerPartition = kNumOfFreeFrames / kNumOfThreads;
static constexpr int kMaxHops = 1024;
// #include "logging/event_logger.h"
// namespace CHEERDB_NAMESPACE {
using WalNumber = uint64_t;

class Slice;

class Status;

// todo env
class Env;

typedef union {
    struct __attribute__((packed)) {
        uint16_t union_size; // | 5bit: block size | 11bit payload size |
        uint16_t crc;
        uint32_t serial;
    };
    uint64_t value;
} record_header_t;

typedef union {
    // todo varient
    struct {
        int64_t id;
        int64_t salary;
        char user_id[128];
        char name[128];
    };
    char raw[kKeySize];
} payload_t;

// 一个BLock 64字节 对齐cacheline
union alignas(kBlockSize) Block {
    struct {
        record_header_t ctrl;
        payload_t payload;
    };
    char padding[kBlockSize];
};

struct balloc_t {
    baddr_t block;
    uint32_t block_size;
};

// 碎片回收
struct alignas(kCacheLineSize) FreeFrame {
    uint32_t top;
    baddr_t blocks[kEntriesPerFreeFrame];
    FreeFrame *next;
};

// Local 本地线程信息
struct alignas(kCacheLineSize) Local {
    Block *next_block;
    Block *end_block;
    size_t serial;
    balloc_t short_alloc;
    FreeFrame *next_free_frame;
    FreeFrame *end_free_frame;
    FreeFrame *free_list[kFreeListSize];
};

class NvmEngine {
private:
    struct alignas(kCacheLineSize) {
        FILE *log_file = nullptr;
        Block *blocks = nullptr;
        char padding[kCacheLineSize - sizeof(log_file) - sizeof(blocks)];
    };

    alignas(kCacheLineSize) std::atomic<int> thread_counter = {0};
    alignas(kCacheLineSize) Bucket buckets[kNumOfBuckets];
    alignas(kCacheLineSize) FreeFrame free_frames[kNumOfFreeFrames];
    alignas(kCacheLineSize) Local locals[kNumOfThreads];

    void log(const char *s);

    bool check_or_set_header();

    void recovery();

    balloc_t alloc(const balloc_t block);

    void free(const balloc_t block);

    void persist_free(const balloc_t block);

    int match(const digest_t digest, const digest_t *target);

    int matchEmpty(const digest_t *target);

    Local *get_local();

    int assign_local();
};

class Accessor {
    Bucket* const bucket;
    const int offset;
public:
    Accessor();
    bool empty();
    baddr_t get();
};


// class WalMetadata {
// public:
//     WalMetadata() = default;
//
//     explicit WalMetadata(uint64_t synced_size_bytes) : synced_size_bytes_(synced_size_bytes) {}
//
//     bool HasSyncedSize() const { return synced_size_bytes_ != kUnknownWalSize; }
//
//     void SetSyncedSizeInBytes(uint64_t bytes) { synced_size_bytes_ = bytes; }
//
//     uint64_t GetSyncedSizeInBytes() const { return synced_size_bytes_; }
//
//
// private:
//     // 极限值代替传统预处理常数
//     constexpr static uint64_t kUnknownWalSize = std::numeric_limits<uint64_t>::max();
//
//     uint64_t synced_size_bytes_ = kUnknownWalSize;
// };
//
// enum class WalAdditionTag : uint32_t {
//     kTerminate = 1,
//     kSyncedSize = 2,
// };
//
// class WalAddition {
// public:
//     WalAddition() : number_(0), metadata_(0) {}
//
//     explicit WalAddition(WalNumber number) : number_(number), metadata_() {}
//
//     WalAddition(WalNumber number, WalMetadata meta) : number_(number), metadata_(std::move(meta)) {}
//
//     WalNumber GetLogNumber() const { return number_; }
//
//     const WalMetadata &GetMetadata() const { return metadata_; }
//
//     void EncodeTo(std::string *dst) const;
//
//     Status DecodeForm(Slice *src);
//
//     std::string DebugString() const;
//
//
// private:
//     WalNumber number_;
//     WalMetadata metadata_;
//
//
// };
//
//
// std::ostream &operator<<(std::ostream &os, const WalAddition &wal);
//
// using WalAdditions = std::vector<WalAddition>;
//
// class WalDeletion {
// public:
//     WalDeletion() : number_(kEmpty) {}
//
//     explicit WalDeletion(WalNumber number) : number_(number) {}
//
//     WalNumber GetLogNumber() const { return number_; }
//
//     void EncodeTo(std::string *dst) const;
//
//     Status DecodeFrom(Slice *src);
//
//     std::string DebugString() const;
//
//     bool IsEmpty() const { return number_ == kEmpty; }
//
//     void Reset() { number_ = kEmpty; }
//
// private:
//     static constexpr WalNumber kEmpty = 0;
//
//     WalNumber number_;
// };
//
// std::ostream &operator<<(std::ostream &os, const WalDeletion &wal);
//
// class WalSet {
// public:
//     // Add WAL(s).
//     // If the WAL is closed,
//     // then there must be an existing unclosed WAL,
//     // otherwise, return Status::Corruption.
//     // Can happen when applying a VersionEdit or recovering from MANIFEST.
//     Status AddWal(const WalAddition &wal);
//
//     Status AddWals(const WalAdditions &wals);
//
//     // Delete WALs with log number smaller than the specified wal number.
//     // Can happen when applying a VersionEdit or recovering from MANIFEST.
//     Status DeleteWalsBefore(WalNumber wal);
//
//     // Resets the internal state.
//     void Reset();
//
//     // WALs with number less than MinWalNumberToKeep should not exist in WalSet.
//     WalNumber GetMinWalNumberToKeep() const { return min_wal_number_to_keep_; }
//
//     const std::map<WalNumber, WalMetadata> &GetWals() const { return wals_; }
//
//     // Checks whether there are missing or corrupted WALs.
//     // Returns Status::OK if there is no missing nor corrupted WAL,
//     // otherwise returns Status::Corruption.
//     // logs_on_disk is a map from log number to the log filename.
//     // Note that logs_on_disk may contain logs that is obsolete but
//     // haven't been deleted from disk.
//     Status CheckWals(
//             Env *env,
//             const std::unordered_map<WalNumber, std::string> &logs_on_disk) const;
//
// private:
//     std::map<WalNumber, WalMetadata> wals_;
//     // WAL number < min_wal_number_to_keep_ should not exist in wals_.
//     // It's monotonically increasing, in-memory only, not written to MANIFEST.
//     WalNumber min_wal_number_to_keep_ = 0;
// };

// }  // namespace ROCKSDB_NAMESPACE
