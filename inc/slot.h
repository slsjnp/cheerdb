#pragma once

#include "util.h"
#include "log_writer.h"
#include "env.h"

#include <atomic>
#include <mutex>
#include <condition_variable>

class Slot {
public:
  enum State : uint8_t {
    // The initial state of a writer.  This is a Writer that is
    // waiting in JoinBatchGroup.  This state can be left when another
    // thread informs the waiter that it has become a group leader
    // (-> STATE_GROUP_LEADER), when a leader that has chosen to be
    // non-parallel informs a follower that its writes have been committed
    // (-> STATE_COMPLETED), or when a leader that has chosen to perform
    // updates in parallel and needs this Writer to apply its batch (->
    // STATE_PARALLEL_MEMTABLE_WRITER).
    Init = 1,

    // The state used to inform a waiting Writer that it has become the
    // leader, and it should now build a write batch group.  Tricky:
    // this state is not used if newest_writer_ is empty when a writer
    // enqueues itself, because there is no need to wait (or even to
    // create the mutex and condvar used to wait) in that case.  This is
    // a terminal state unless the leader chooses to make this a parallel
    // batch, in which case the last parallel worker to finish will move
    // the leader to STATE_COMPLETED.
    GroupLeader = 2,

    // The state used to inform a waiting writer that it has become the
    // leader of memtable writer group. The leader will either write
    // memtable for the whole group, or launch a parallel group write
    // to memtable by calling LaunchParallelMemTableWrite.
    MemtableWriteLeader = 4,

    // The state used to inform a waiting writer that it has become a
    // parallel memtable writer. It can be the group leader who launch the
    // parallel writer group, or one of the followers. The writer should then
    // apply its batch to the memtable concurrently and call
    // CompleteParallelMemTableWriter.
    ParallelMemtableWriter = 8,

    // A follower whose writes have been applied, or a parallel leader
    // whose followers have all finished their work.  This is a terminal
    // state.
    Completed = 16,

    // A state indicating that the thread may be waiting using StateMutex()
    // and StateCondVar()
    LockedWaiting = 32,
  };

  struct WriteBatch {
    static size_t byte_size(const WriteBatch* batch) {
      return batch->rep_.size();
    }

    std::string rep_;
  };

  struct Writer;

  struct WriteGroup {
    Writer* leader_ = nullptr;
    Writer* last_writer_ = nullptr;
    SequenceNumber last_sequence_;
    // before running goes to zero, status needs leader->StateMutex()
    std::atomic<size_t> running_;
    size_t size_ = 0;

    struct Iterator {
      Writer* writer_;
      Writer* last_writer_;

      explicit Iterator(Writer* w, Writer* last)
          : writer_(w), last_writer_(last) {}

      Writer* operator*() const { return writer_; }

      Iterator& operator++() {
        assume(writer_ != nullptr);
        if (writer_ == last_writer_) {
          writer_ = nullptr;
        } else {
          writer_ = writer_->link_newer_;
        }
        return *this;
      }

      bool operator!=(const Iterator& other) const {
        return writer_ != other.writer_;
      }
    };

    Iterator begin() const { return Iterator(leader_, last_writer_); }
    Iterator end() const { return Iterator(nullptr, nullptr); }
  };

  struct Writer {
    Writer() {}

    ~Writer() {
      if (made_waitable_) {
        state_mutex().~mutex();
        state_cv().~condition_variable();
      }
    }

    std::atomic<uint8_t> state_;

    uint64_t offset_;

    bool made_waitable_;          // records lazy construction of mutex and cv
    std::aligned_storage<sizeof(std::mutex)>::type state_mutex_bytes_;
    std::aligned_storage<sizeof(std::condition_variable)>::type state_cv_bytes_;

    Writer* link_older_;  // read/write only before linking, or as leader
    Writer* link_newer_;  // lazy, read/write only before linking, or as leader

    CHEERDB_NAMESPACE::log::Writer* log_;
    WriteBatch* batch_;
    WriteGroup* group_;

    // No other mutexes may be acquired while holding StateMutex(), it is
    // always last in the order
    std::mutex& state_mutex() {
      assume(made_waitable_);
      return *static_cast<std::mutex*>(static_cast<void*>(&state_mutex_bytes_));
    }

    std::condition_variable& state_cv() {
      assume(made_waitable_);
      return *static_cast<std::condition_variable*>(
          static_cast<void*>(&state_cv_bytes_));
    }

    void create_mutex() {
      if (!made_waitable_) {
        // Note that made_waitable is tracked separately from state
        // transitions, because we can't atomically create the mutex and
        // link into the list.
        made_waitable_ = true;
        new (&state_mutex_bytes_) std::mutex;
        new (&state_cv_bytes_) std::condition_variable;
      }
    }
  };



  void join_batch_group(Writer* writer);

  size_t enter_as_batch_group_leader(Writer* leader, WriteGroup* write_group);

  void exit_as_batch_group_leader(WriteGroup& write_group);

  void enter_as_memtable_writer(Writer* leader, WriteGroup* write_group);

  void launch_parallel_memtable_writers(WriteGroup* write_group);

  void exit_as_memtable_writer(Writer* writer, WriteGroup& write_group);

  bool Slot::complete_parallel_memtable_writer(Writer* w);

private:
  bool link(Writer* w, std::atomic<Writer*>* newest_writer);

  void set_state(Writer* w, uint8_t new_state);

  Writer* find_next_leader(Writer* from, Writer* boundary);

  void create_missing_newer_links(Writer* head);

  bool link_group(WriteGroup& write_group, std::atomic<Writer*>* newest_writer);

  // Blocks until w->state & goal_mask, returning the state value
  // that satisfied the predicate.  Uses ctx to adaptively use
  // std::this_thread::yield() to avoid mutex overheads.  ctx should be
  // a context-dependent static.
  uint8_t await_state(Writer* w, uint8_t goal_mask, std::atomic<int32_t>* yield_ctx);

  uint8_t blocking_await_state(Writer* w, uint8_t goal_mask);

  // Points to the newest pending writer. Only leader can remove
  // elements, adding can be done lock-free by anybody.
  std::atomic<Writer*> newest_writer_;

  // Points to the newest pending memtable writer. Used only when pipelined
  // write is enabled.
  std::atomic<Writer*> newest_memtable_writer_;

  // A dummy writer to indicate a write stall condition. This will be inserted
  // at the tail of the writer queue by the leader, so newer writers can just
  // check for this and bail
  Writer write_stall_dummy_;

  // Mutex and condvar for writers to block on a write stall. During a write
  // stall, writers with no_slowdown set to false will wait on this rather
  // on the writer queue
  std::mutex stall_mu_;
  std::condition_variable stall_cv_;

  std::atomic<int32_t> join_batch_group_yield_ctx_;
  std::atomic<int32_t> exit_as_batch_group_leader_ctx_;
  std::atomic<int32_t> complete_parallel_memtable_writer_ctx_;
  
  // See AwaitState.
  const uint64_t max_yield_usec_;
  const uint64_t slow_yield_usec_;

  const size_t max_write_batch_group_size_bytes_;
};