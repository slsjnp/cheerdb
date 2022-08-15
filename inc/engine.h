#pragma once

#include "slot.h"
#include "hashtable.h"
#include "record.h"

#include <string_view>
#include <valarray>
#include <optional>

class Engine {
public:
    enum Column {
        Id = 0,
        UserId,
        Name,
        Salary,
    };

    void init(
            std::string_view host_info,
            std::valarray<std::string_view> peer_host_infos,
            std::string_view aep_dir,
            std::string_view disk_dir
    );

    std::optional<void> write(void *ctx, const User &data);

    std::optional<size_t> read(
            void *ctx,
            Column select_column,
            Column where_column,
            const void *column_key,
            void *res
    );

    void deinit(void *ctx);

    static Engine &get();

private:
    std::optional<void> write_single_slot(
            size_t slot,
            void *ctx,
            const User &data
    );

    Block *block = nullptr;

    void insert_to_memtable(const User &user, uint64_t offset);

    static Engine instance_;

    Engine();

    ~Engine();

    Engine(const Engine &) = delete;

    Engine &operator=(const Engine &) = delete;

    std::string_view host_info_;
    std::valarray<std::string_view> peer_host_infos_;
    std::string_view aep_dir_;
    std::string_view disk_dir_;

    std::array<Slot, SlotSize> slots_;

    // memtable
    HashTable<int64_t, uint64_t, 12> id_to_offset_;
    HashTable<char[128], int64_t, 12> userid_to_id_;
    HashTable<int64_t, int64_t, 12> salary_to_id_;

    struct alignas(kCacheLineSize) {
        FILE *log_file = nullptr;
        Block *blocks = nullptr;
        char padding[kCacheLineSize - sizeof(log_file) - sizeof(blocks)];
    };

    alignas(kCacheLineSize) std::atomic<int> thread_counter = {0};
    // alignas(kCacheLineSize) Bucket buckets[kNumOfBuckets];
    alignas(kCacheLineSize) FreeFrame free_frames[kNumOfFreeFrames];
    alignas(kCacheLineSize) Local locals[kNumOfThreads];

    void log(const char *s);

    bool check_or_set_header();

    void recovery();

    balloc_t alloc(const uint32_t block);

    void free(const balloc_t block);

    void persist_free(const balloc_t block);

    int match(const digest_t digest, const digest_t *target);

    int matchEmpty(const digest_t *target);

    Local *get_local();

    int assign_local();
};