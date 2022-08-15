//
// Created by sj on 22-7-23.
//

#ifndef INTERFACE_WAL_H
#define INTERFACE_WAL_H

#endif //INTERFACE_WAL_H

#pragma once

#include <map>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

// #include "logging/event_logger.h"
namespace CHEERDB_NAMESPACE{
using WalNumber = uint64_t;

class Slice;
class Status;
// todo env
class Env;

class WalMetadata {
public:
    WalMetadata() = default;

    explicit WalMetadata(uint64_t synced_size_bytes) : synced_size_bytes_(synced_size_bytes) {}

    bool HasSyncedSize() const { return synced_size_bytes_ != kUnknownWalSize; }

    void SetSyncedSizeInBytes(uint64_t bytes) { synced_size_bytes_ = bytes; }

    uint64_t GetSyncedSizeInBytes() const { return synced_size_bytes_; }


private:
    // 极限值代替传统预处理常数
    constexpr static uint64_t kUnknownWalSize = std::numeric_limits<uint64_t>::max();

    uint64_t synced_size_bytes_ = kUnknownWalSize;
};

enum class WalAdditionTag : uint32_t {
    kTerminate = 1,
    kSyncedSize = 2,
};

class WalAddition {
public:
    WalAddition() : number_(0), metadata_(0) {}

    explicit WalAddition(WalNumber number) : number_(number), metadata_() {}

    WalAddition(WalNumber number, WalMetadata meta) : number_(number), metadata_(std::move(meta)) {}

    WalNumber GetLogNumber() const { return number_; }

    const WalMetadata &GetMetadata() const { return metadata_; }

    void EncodeTo(std::string *dst) const;

    Status DecodeForm(Slice *src);

    std::string DebugString() const;


private:
    WalNumber number_;
    WalMetadata metadata_;


};


std::ostream& operator<<(std::ostream& os, const WalAddition& wal);

using WalAdditions = std::vector<WalAddition>;

class WalDeletion {
public:
    WalDeletion() : number_(kEmpty) {}

    explicit WalDeletion(WalNumber number) : number_(number) {}

    WalNumber GetLogNumber() const { return number_; }

    void EncodeTo(std::string* dst) const;

    Status DecodeFrom(Slice* src);

    std::string DebugString() const;

    bool IsEmpty() const { return number_ == kEmpty; }

    void Reset() { number_ = kEmpty; }

private:
    static constexpr WalNumber kEmpty = 0;

    WalNumber number_;
};

std::ostream& operator<<(std::ostream& os, const WalDeletion& wal);

class WalSet {
public:
    // Add WAL(s).
    // If the WAL is closed,
    // then there must be an existing unclosed WAL,
    // otherwise, return Status::Corruption.
    // Can happen when applying a VersionEdit or recovering from MANIFEST.
    Status AddWal(const WalAddition& wal);
    Status AddWals(const WalAdditions& wals);

    // Delete WALs with log number smaller than the specified wal number.
    // Can happen when applying a VersionEdit or recovering from MANIFEST.
    Status DeleteWalsBefore(WalNumber wal);

    // Resets the internal state.
    void Reset();

    // WALs with number less than MinWalNumberToKeep should not exist in WalSet.
    WalNumber GetMinWalNumberToKeep() const { return min_wal_number_to_keep_; }

    const std::map<WalNumber, WalMetadata>& GetWals() const { return wals_; }

    // Checks whether there are missing or corrupted WALs.
    // Returns Status::OK if there is no missing nor corrupted WAL,
    // otherwise returns Status::Corruption.
    // logs_on_disk is a map from log number to the log filename.
    // Note that logs_on_disk may contain logs that is obsolete but
    // haven't been deleted from disk.
    Status CheckWals(
            Env* env,
            const std::unordered_map<WalNumber, std::string>& logs_on_disk) const;

private:
    std::map<WalNumber, WalMetadata> wals_;
    // WAL number < min_wal_number_to_keep_ should not exist in wals_.
    // It's monotonically increasing, in-memory only, not written to MANIFEST.
    WalNumber min_wal_number_to_keep_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
