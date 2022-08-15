#include "../inc/engine.h"
#include "../inc/slot.h"
#include "../inc/fmtlog.h"
#include "../inc/slice.h"
#include "../inc/log_writer.h"
#include "../inc/env.h"
#include "../inc/record.h"
#include <emmintrin.h>
#include <nmmintrin.h>
#include <iostream>
#include <sstream>

#define CRC_SIZE(psize) (((psize >> 3) + ((psize & 0x07) > 0)) << 3)
#define CTRL_BLOCK_SIZE(usize) static_cast<uint32_t>((usize>>11) & 0x1F)
#define CTRL_PAYLOAD_SIZE(usize) static_cast<uint32_t>(usize & 0x07FF)
#define CTRL_UNION_SIZE(bsize, psize) static_cast<uint16_t>((bsize << 11) | (psize & 0x07FF))

inline void init_free_frames(FreeFrame *start, FreeFrame *const end) {
    do {
        start->next = start + 1;
    } while (++start < end);
    (end - 1)->next = nullptr;
}

inline uint16_t crc32(uint32_t crc, char *input, size_t len) {
    uint32_t *ptr = reinterpret_cast<uint32_t *>(input);
    while (len > 0) {
        crc = _mm_crc32_u32(crc, *ptr);
        len -= sizeof(uint32_t);
        ++ptr;
    }
    return crc ^ (crc >> 16);
}

void Engine::init(
        std::string_view host_info,
        std::valarray<std::string_view> peer_host_infos,
        std::string_view aep_dir,
        std::string_view disk_dir
) {
    fmtlog::setLogLevel(fmtlog::DBG);
    fmtlog::setLogFile("cheerdb.log", truncate);

    logi("Hello cheerdb engine");

    host_info_ = host_info;
    peer_host_infos_ = peer_host_infos;
    aep_dir_ = aep_dir;
    disk_dir_ = disk_dir;

    // TODO: open or create file, reconstruct memtable
    std::string name;
    if ((blocks = (Block *) pmem_map_file(name.c_str(),
                                          kNumOfBlocks * sizeof(Block),
                                          PMEM_FILE_CREATE, 0666, nullptr,
                                          nullptr)) == NULL) {
        perror("Pmem map file failed");
        exit(1);
    }

    for (int i = 0; i < kNumOfThreads; ++i) {
        locals[i].next_block = block + i * kBlocksPerPartition;
        locals[i].end_block = locals[i].next_block + kBlocksPerPartition;
        locals[i].serial = 0;
        locals[i].short_alloc = {0, 0};
        locals[i].next_free_frame = free_frames + i * kFreeFramesPerPartition;
        locals[i].end_free_frame = locals[i].next_free_frame + kFreeFramesPerPartition;
    }
    ++locals[0].next_block;

    std::thread threads[kNumOfThreads];

    if (check_or_set_header()) {
        // database recovery
        logi("Cheerdb recovery");
        for (int i = 0; i < kNumOfThreads; ++i) {
            threads[i] = std::thread([&](int t) {
                recovery();
            }, i);
        }
        for (int i = 0; i < kNumOfThreads; ++i) {
            threads[i].join();
        }
    } else {
        // init database;
        logi("Cheerdb initialization");
        for (int i = 0; i < kNumOfThreads; ++i) {
            threads[i] = std::thread([&](int t) {
                recovery();
                init_free_frames(locals[t].next_free_frame, locals[t].end_free_frame);
            }, i);
        }
        for (int i = 0; i < kNumOfThreads; ++i) {
            threads[i].join();
        }
    }

    logi("engine init done");
}

std::optional<void> Engine::write(
        void *ctx,
        const User &data
) {
    auto slot = data.id % SlotSize;
    return this->write_single_slot(slot, ctx, data);
}

std::optional<size_t> Engine::read(
        void *ctx,
        Column select_column,
        Column where_column,
        const void *column_key,
        void *res
) {
    auto offsets = std::vector<uint64_t>();
    switch (select_column) {
        case Id:
            auto offset = this->id_to_offset_.get(*reinterpret_cast<const int64_t *>(column_key));
            if (offset.has_value()) offsets.emplace_back(offset);
        case Salary: {
            auto ids = this->salary_to_id_.get_all(*reinterpret_cast<const int64_t *>(column_key));
            for (auto id: ids) {
                auto offset = this->id_to_offset_.get(id);
                if (offset.has_value()) offsets.emplace_back(offset);
            }
        }
        case UserId:
            auto id = this->userid_to_id_.get(*reinterpret_cast<const char (*)[128]>(column_key));
            if (id.has_value()) {
                auto offset = this->id_to_offset_.get(id.value());
                if (offset.has_value()) offsets.emplace_back(offset);
            }
        default:
            unreachable();
    }

    // TODO: extract column by offset
    std::vector<payload_t > result_t;
    for (auto offset: offsets){
        auto payload_size = CTRL_PAYLOAD_SIZE(blocks[offset].ctrl.union_size);
        int id_, salary;
        std::string *value, *name;
        value->assign(blocks[offset].payload.user_id);
        id_ = blocks[offset].payload.id;
        salary = blocks[offset].payload.salary;
        name->assign(blocks[offset].payload.name);
    }
}

std::optional<void> Engine::write_single_slot(
        size_t slot_id,
        void *ctx,
        const User &data
) {
    auto &&slot = slots_[slot_id];

    Slot::Writer writer;
    slot.join_batch_group(&writer);

    if (writer.state_ == Slot::State::GroupLeader) {
        Slot::WriteGroup wal_write_group;

        auto last_batch_group_size_ =
                slot.enter_as_batch_group_leader(&writer, &wal_write_group);

        // TODO:
        {
            // Unlock here.
            mutex_.Unlock()
            auto status = writer.log_->AddRecord(Slice(writer.batch_->rep));
            bool sync_error = false;
            if (status.ok()) {
                // Sync
                status = writer.log_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }
            if (status.ok()) {
                // Write to mem;
            }
        }

        slot.exit_as_batch_group_leader(wal_write_group);
    }

    // NOTE: the memtable_write_group is declared before the following
    // `if` statement because its lifetime needs to be longer
    // that the inner context  of the `if` as a reference to it
    // may be used further below within the outer _slot
    Slot::WriteGroup memtable_write_group;

    if (writer.state_ == Slot::State::MemtableWriteLeader) {
        slot.enter_as_memtable_writer(&writer, &memtable_write_group);
        if (memtable_write_group.size_ > 1) {
            slot.launch_parallel_memtable_writers(&memtable_write_group);
        } else {
            Engine::insert_to_memtable(data, writer.offset_);

            slot.exit_as_memtable_writer(&writer, memtable_write_group);
        }
    }

    if (writer.state_ == Slot::State::ParallelMemtableWriter) {
        Engine::insert_to_memtable(data, writer.offset_);

        if (slot.complete_parallel_memtable_writer(&writer)) {
            slot.exit_as_memtable_writer(&writer, *writer.group_);
        }
    }

    assume(writer.state_ == Slot::State::Completed);
}

void Engine::deinit(void *ctx) {
    // TODO: nothing
}

Engine &Engine::get() {
    return instance_;
}

void Engine::insert_to_memtable(const User &user, uint64_t offset) {
    this->salary_to_id_.set(user.salary, user.id);
    this->userid_to_id_.set(user.user_id, user.id);

    this->id_to_offset_.set(user.id, offset);
}

bool Engine::check_or_set_header() {
    if (blocks->ctrl.union_size == CTRL_UNION_SIZE(1, 10) && blocks->ctrl.crc == 0x1018 &&
    blocks->ctrl.serial == 0 && memcpy(blocks->payload.raw, "DB STORE", 10) == 0){
        return true;
    } else{ // don't have header;
        memset(blocks, 0, sizeof(Block));
        blocks->ctrl.union_size = CTRL_UNION_SIZE(1, 10);
        blocks->ctrl.crc = 0x1018;
        blocks->ctrl.serial = 0;
        memcpy(blocks->payload.raw, "DB STORE", 10);
        pmem_persist(blocks, sizeof(Block));
        return false;
    }
}

void Engine::recovery() {
    auto *const local = get_local();
    init_free_frames(local->next_free_frame, local->end_free_frame);

    while (local->next_block < local->end_block) {
        auto const block = local->next_block;
        auto const bsize = CTRL_BLOCK_SIZE(block->ctrl.union_size);
        auto const payload_size = CTRL_PAYLOAD_SIZE(block->ctrl.union_size);
        if (bsize >= 2 && bsize < kFreeListSize) {
            if (payload_size > 0 && payload_size <= kMaxPayloadSize &&
                block->ctrl.crc == crc32(0, block->payload.raw, CRC_SIZE(payload_size))) {
                // try to find another one
                void *res;
                auto res_size = read(nullptr, Id, Id, &(block->payload.id), res);
                if (res_size != 0 && res->ctrl.serial > block->ctrl.serial) {
                    // free loser block;
                    free({static_case<uint32_t>(block - blocks), bsize});
                    local->next_block += bsize;
                    continue;
                }
            } else {
                // invalid or free block, reset the ctrl
                block->ctrl.union_size = CTRL_UNION_SIZE(bsize, 0);
                block->ctrl.crc = 0;
                block->ctrl.serial = 0;
                pmem_persist(block, sizeof(record_header_t));
                free({static_cast<uint32_t>(block - blocks), bsize});
            }
        } else {
            break;
        }
        local->next_block += bsize;
    }
}

inline Local *NvmEngine::get_local() {
    static thread_local Local *const local = &locals[assign_local()];
    return local;
}

inline int NvmEngine::assign_local() {
    const auto tid = (thread_counter++) % kNumOfThreads;
    logi("start thread");
    return tid;
}

inline void Engine::persist_free(const balloc_t balloc) {
    alignas(kCacheLineSize) Block free_block;
    free_block.ctrl.union_size = CTRL_UNION_SIZE(balloc.block_size, 0);
    pmem_memcpy(blocks + balloc.block, &free_block, sizeof(free_block), PMEM_F_MEM_NONTEMPORAL | PMEM_F_MEM_WC);
}

inline balloc_t Engine::alloc(const uint32_t block_size) {
    auto const local = get_local();

    if (local->short_alloc.block_size >= block_size ) {
        if (local->short_alloc.block_size - block_size >= 2) {
            balloc_t alloc = {local->short_alloc.block, block_size};
            local->short_alloc = {alloc.block + block_size, local->short_alloc.block_size - block_size};
            persist_free(local->short_alloc);
            return alloc;
        } else {
            balloc_t alloc = local->short_alloc;
            local->short_alloc = {0, 0};
            return alloc;
        }
    }

    if (local->next_block + kReservedBlocksPerPartition < local->end_block) {
        balloc_t alloc = {static_cast<baddr_t>(local->next_block - blocks), block_size};
        local->next_block += block_size;
        return alloc;
    }

    for (uint32_t i = block_size; i < kFreeListSize; ++i) {
        if (local->free_list[i] != nullptr) {
            auto frame = local->free_list[i];
            balloc_t alloc = {frame->blocks[--frame->top], i};
            if (i - block_size >= 2) {
                alloc.block_size = block_size;
                if (local->short_alloc.block_size >= 2) free(local->short_alloc);
                local->short_alloc = {alloc.block + block_size, i - block_size};
            }
            if (frame->top == 0) {
                local->free_list[i] = frame->next;
                frame->next = local->next_free_frame;
                local->next_free_frame = frame;
            }
            return alloc;
        }
    }

    if (local->next_block + block_size < local->end_block) {
        balloc_t alloc = {static_cast<baddr_t>(local->next_block - blocks), block_size};
        local->next_block += block_size;
        return alloc;
    }

    std::stringstream stream;
    stream<<"block_size: "<<block_size<<"|"<<local->next_block - blocks<<"|serial:"<<local->serial<<"||";
    for (int i = 0; i < kFreeListSize; ++i) {
        stream<<(local->free_list[i] != nullptr);
    }
    log(stream.str().c_str());

    ERROR("Out of memory!");
}

inline void Engine::free(const balloc_t balloc) {
    auto const local = get_local();

    auto frame = local->free_list[balloc.block_size];
    if (frame == nullptr || frame->top >= kEntriesPerFreeFrame) {
        if (local->next_free_frame) {
            local->free_list[balloc.block_size] = local->next_free_frame;
            local->next_free_frame = local->next_free_frame->next;
            local->free_list[balloc.block_size]->next = frame;
            frame = local->free_list[balloc.block_size];
            frame->top = 0;
        } else {
            ERROR("Out of free frame!");
            std::abort();
        }
    }

    frame->blocks[frame->top++] = balloc.block;
}


baddr_t Accessor::get() {
    return bucket->blocks[offset];
}