#include "../inc/slot.h"
#include "../inc/util.h"
#include "../inc/random.h"

#include <mutex>
#include <thread>


void Slot::join_batch_group(Writer* writer) {
  bool linked_as_leader = link(writer, &this->newest_writer_);
  if (linked_as_leader) {
    this->set_state(writer, State::GroupLeader);
  } else {
    /**
     * Wait util:
     * 1) An existing leader pick us as the new leader when it finishes
     * 2) An existing leader pick us as its follewer and
     * 2.1) finishes the memtable writes on our behalf
     * 2.2) Or tell us to finish the memtable writes in pralallel
     * 3) (pipelined write) An existing leader pick us as its follower and
     *    finish book-keeping and WAL write for us, enqueue us as pending
     *    memtable writer, and
     * 3.1) we become memtable writer group leader, or
     * 3.2) an existing memtable writer group leader tell us to finish memtable
     *      writes in parallel.
     */
    this->await_state(
      writer, 
      State::GroupLeader 
      | State::MemtableWriteLeader 
      | State::ParallelMemtableWriter 
      | State::Completed,
      &join_batch_group_yield_ctx_
    );
  }
}

bool Slot::link(Writer* w, std::atomic<Writer*>* newest_writer) {
  assume(newest_writer != nullptr);
  assume(w->state_ == State::Init);

  Writer* writers = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    if (writers == &write_stall_dummy_) {
      // Since no_slowdown is false, wait here to be notified of the write
      // stall clearing
      {
        std::unique_lock<std::mutex> lock(stall_mu_);
        writers = newest_writer->load(std::memory_order_relaxed);
        if (writers == &write_stall_dummy_) {
          stall_cv_.wait(lock);
          // Load newest_writers_ again since it may have changed
          writers = newest_writer->load(std::memory_order_relaxed);
          continue;
        }
      }
    }
    w->link_older_ = writers;
    if (newest_writer->compare_exchange_weak(writers, w)) {
      return (writers == nullptr);
    }
  }
}

void Slot::set_state(Writer* w, uint8_t new_state) {
  assume(w != nullptr);

  auto state = w->state_.load(std::memory_order_acquire);
  if (state == State::LockedWaiting ||
      !w->state_.compare_exchange_strong(state, new_state)) {
    assume(state == State::LockedWaiting);
    std::lock_guard<std::mutex> guard(w->state_mutex());
    assume(w->state_.load(std::memory_order_relaxed) != new_state);
    w->state_.store(new_state, std::memory_order_relaxed);
    w->state_cv().notify_one();
  }
}

uint8_t Slot::await_state(Writer* w, uint8_t goal_mask, std::atomic<int32_t>* yield_ctx) {
  uint8_t state = 0;

  // 1. Busy loop using "pause" for 1 micro sec
  // 2. Else SOMETIMES busy loop using "yield" for 100 micro sec (default)
  // 3. Else blocking wait

  // On a modern Xeon each loop takes about 7 nanoseconds (most of which
  // is the effect of the pause instruction), so 200 iterations is a bit
  // more than a microsecond.  This is long enough that waits longer than
  // this can amortize the cost of accessing the clock and yielding.
  for (uint32_t tries = 0; tries < 200; ++tries) {
    state = w->state_.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) {
      return state;
    }

    // TODO: port
    asm volatile("pause");
  }

  // This is below the fast path, so that the stat is zero when all writes are
  // from the same thread.
  // PERF_TIMER_GUARD(slot_wait_nanos);

  // If we're only going to end up waiting a short period of time,
  // it can be a lot more efficient to call std::this_thread::yield()
  // in a loop than to block in StateMutex().  For reference, on my 4.0
  // SELinux test server with support for syscall auditing enabled, the
  // minimum latency between FUTEX_WAKE to returning from FUTEX_WAIT is
  // 2.7 usec, and the average is more like 10 usec.  That can be a big
  // drag on RockDB's single-writer design.  Of course, spinning is a
  // bad idea if other threads are waiting to run or if we're going to
  // wait for a long time.  How do we decide?
  //
  // We break waiting into 3 categories: short-uncontended,
  // short-contended, and long.  If we had an oracle, then we would always
  // spin for short-uncontended, always block for long, and our choice for
  // short-contended might depend on whether we were trying to optimize
  // RocksDB throughput or avoid being greedy with system resources.
  //
  // Bucketing into short or long is easy by measuring elapsed time.
  // Differentiating short-uncontended from short-contended is a bit
  // trickier, but not too bad.  We could look for involuntary context
  // switches using getrusage(RUSAGE_THREAD, ..), but it's less work
  // (portability code and CPU) to just look for yield calls that take
  // longer than we expect.  sched_yield() doesn't actually result in any
  // context switch overhead if there are no other runnable processes
  // on the current core, in which case it usually takes less than
  // a microsecond.
  //
  // There are two primary tunables here: the threshold between "short"
  // and "long" waits, and the threshold at which we suspect that a yield
  // is slow enough to indicate we should probably block.  If these
  // thresholds are chosen well then CPU-bound workloads that don't
  // have more threads than cores will experience few context switches
  // (voluntary or involuntary), and the total number of context switches
  // (voluntary and involuntary) will not be dramatically larger (maybe
  // 2x) than the number of voluntary context switches that occur when
  // --max_yield_wait_micros=0.
  //
  // There's another constant, which is the number of slow yields we will
  // tolerate before reversing our previous decision.  Solitary slow
  // yields are pretty common (low-priority small jobs ready to run),
  // so this should be at least 2.  We set this conservatively to 3 so
  // that we can also immediately schedule a ctx adaptation, rather than
  // waiting for the next update_ctx.

  const size_t kMaxSlowYieldsWhileSpinning = 3;

  // Whether the yield approach has any credit in this context. The credit is
  // added by yield being succesfull before timing out, and decreased otherwise.
  auto& yield_credit = *yield_ctx;
  // Update the yield_credit based on sample runs or right after a hard failure
  bool update_ctx = false;
  // Should we reinforce the yield credit
  bool would_spin_again = false;
  // The samling base for updating the yeild credit. The sampling rate would be
  // 1/sampling_base.
  const int sampling_base = 256;

  if (max_yield_usec_ > 0) {
    update_ctx = Random::GetTLSInstance()->OneIn(sampling_base);

    if (update_ctx || yield_credit.load(std::memory_order_relaxed) >= 0) {
      // we're updating the adaptation statistics, or spinning has >
      // 50% chance of being shorter than max_yield_usec_ and causing no
      // involuntary context switches
      auto spin_begin = std::chrono::steady_clock::now();

      // this variable doesn't include the final yield (if any) that
      // causes the goal to be met
      size_t slow_yield_count = 0;

      auto iter_begin = spin_begin;
      while ((iter_begin - spin_begin) <=
             std::chrono::microseconds(max_yield_usec_)) {
        std::this_thread::yield();

        state = w->state_.load(std::memory_order_acquire);
        if ((state & goal_mask) != 0) {
          // success
          would_spin_again = true;
          break;
        }

        auto now = std::chrono::steady_clock::now();
        if (now == iter_begin ||
            now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
          // conservatively count it as a slow yield if our clock isn't
          // accurate enough to measure the yield duration
          ++slow_yield_count;
          if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
            // Not just one ivcsw, but several.  Immediately update yield_credit
            // and fall back to blocking
            update_ctx = true;
            break;
          }
        }
        iter_begin = now;
      }
    }
  }

  if ((state & goal_mask) == 0) {
    // TEST_SYNC_POINT_CALLBACK("Slot::AwaitState:BlockingWaiting", w);
    state = blocking_await_state(w, goal_mask);
  }

  if (update_ctx) {
    // Since our update is sample based, it is ok if a thread overwrites the
    // updates by other threads. Thus the update does not have to be atomic.
    auto v = yield_credit.load(std::memory_order_relaxed);
    // fixed point exponential decay with decay constant 1/1024, with +1
    // and -1 scaled to avoid overflow for int32_t
    //
    // On each update the positive credit is decayed by a facor of 1/1024 (i.e.,
    // 0.1%). If the sampled yield was successful, the credit is also increased
    // by X. Setting X=2^17 ensures that the credit never exceeds
    // 2^17*2^10=2^27, which is lower than 2^31 the upperbound of int32_t. Same
    // logic applies to negative credits.
    v = v - (v / 1024) + (would_spin_again ? 1 : -1) * 131072;
    yield_credit.store(v, std::memory_order_relaxed);
  }

  assert((state & goal_mask) != 0);
  return state;
}

uint8_t Slot::blocking_await_state(Writer* w, uint8_t goal_mask) {
  // We're going to block.  Lazily create the mutex.  We guarantee
  // propagation of this construction to the waker via the
  // STATE_LOCKED_WAITING state.  The waker won't try to touch the mutex
  // or the condvar unless they CAS away the STATE_LOCKED_WAITING that
  // we install below.
  w->create_mutex();

  auto state = w->state_.load(std::memory_order_acquire);
  assume(state != State::LockedWaiting);
  if ((state & goal_mask) == 0 &&
    w->state_.compare_exchange_strong(state, State::LockedWaiting)) {
    // we have permission (and an obligation) to use StateMutex
    std::unique_lock<std::mutex> guard(w->state_mutex());
    w->state_cv().wait(guard, [w] {
      return w->state_.load(std::memory_order_relaxed) != State::LockedWaiting;
    });
    state = w->state_.load(std::memory_order_relaxed);
  }
  // else tricky.  Goal is met or CAS failed.  In the latter case the waker
  // must have changed the state, and compare_exchange_strong has updated
  // our local variable with the new one.  At the moment Slot never
  // waits for a transition across intermediate states, so we know that
  // since a state change has occurred the goal must have been met.
  assume((state & goal_mask) != 0);
  return state;
}

size_t Slot::enter_as_batch_group_leader(Writer* leader, WriteGroup* write_group) {
  assume(leader->link_older_ == nullptr);
  assume(leader->batch_ != nullptr);
  assume(write_group != nullptr);

  size_t size = WriteBatch::byte_size(leader->batch_);

  Writer* newest_writer = newest_writer_.load(std::memory_order_acquire);
  
  // This is safe regardless of any db mutex status of the caller. Previous
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to
  // explicitly wake us up (the list was non-empty when we added ourself,
  // so we have already received our MarkJoined).
  create_missing_newer_links(newest_writer);

  // Tricky. Iteration start (leader) is exclusive and finish
  // (newest_writer) is inclusive. Iteration goes from old to new.
  Writer* w = leader;
  while (w != newest_writer) {
    assume(w->link_newer_);
    w = w->link_newer_;

    if (w->batch_ == nullptr) {
      // Do not include those writes with nullptr batch. Those are not writes,
      // those are something else. They want to be alone
      break;
    }

    auto batch_size = WriteBatch::byte_size(w->batch_);
    if (size + batch_size > max_write_batch_group_size_bytes_) {
      // Do not make batch too big
      break;
    }

    w->group_ = write_group;
    size += batch_size;
    write_group->last_writer_ = w;
    write_group->size_++;
  }

  return size;
}

void Slot::exit_as_batch_group_leader(WriteGroup& write_group) {
  Writer* leader = write_group.leader_;
  Writer* last_writer = write_group.last_writer_;
  assume(leader->link_older_ == nullptr);

  Writer* next_leader = nullptr;

  // Look for next leader before we call LinkGroup. If there isn't
  // pending writers, place a dummy writer at the tail of the queue
  // so we know the boundary of the current write group.
  Writer dummy;
  Writer* expected = last_writer;
  bool has_dummy = newest_writer_.compare_exchange_strong(expected, &dummy);
  if (!has_dummy) {
    // We find at least one pending writer when we insert dummy. We search
    // for next leader from there.
    next_leader = find_next_leader(expected, last_writer);
    assume(next_leader != nullptr && next_leader != last_writer);
  }

  // Link the ramaining of the group to memtable writer list.
  //
  // We have to link our group to memtable writer queue before wake up the
  // next leader or set newest_writer_ to null, otherwise the next leader
  // can run ahead of us and link to memtable writer queue before we do.
  if (write_group.size_ > 0) {
    if (link_group(write_group, &newest_memtable_writer_)) {
      // The leader can now be different from current writer.
      set_state(write_group.leader_, State::MemtableWriteLeader);
    }
  }

  // If we have inserted dummy in the queue, remove it now and check if there
  // are pending writer join the queue since we insert the dummy. If so,
  // look for next leader again.
  if (has_dummy) {
    assume(next_leader == nullptr);
    expected = &dummy;
    bool has_pending_writer =
        !newest_writer_.compare_exchange_strong(expected, nullptr);
    if (has_pending_writer) {
      next_leader = find_next_leader(expected, &dummy);
      assert(next_leader != nullptr && next_leader != &dummy);
    }
  }

  if (next_leader != nullptr) {
    next_leader->link_older_ = nullptr;
    set_state(next_leader, State::GroupLeader);
  }
  await_state(
    leader, 
    State::MemtableWriteLeader 
    | State::ParallelMemtableWriter 
    | State::Completed,
    &exit_as_batch_group_leader_ctx_
  );
}

void Slot::enter_as_memtable_writer(Writer* leader, WriteGroup* write_group) {
  assume(leader != nullptr);
  assume(leader->link_older_ == nullptr);
  assume(leader->batch_ != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatch::byte_size(leader->batch_);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = max_write_batch_group_size_bytes_;
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes_ / 8;
  if (size <= min_batch_size_bytes) {
    max_size = size + min_batch_size_bytes;
  }

  leader->group_ = write_group;
  write_group->leader_ = leader;
  write_group->size_ = 1;
  Writer* last_writer = leader;

  // !leader->batch_->HasMerge()
  if (true) {
    Writer* newest_writer = newest_memtable_writer_.load();
    create_missing_newer_links(newest_writer);

    Writer* w = leader;
    while (w != newest_writer) {
      assume(w->link_newer_);
      w = w->link_newer_;

      if (w->batch_ == nullptr) {
        break;
      }

      // w->batch_->HasMerge()
      if (false) {
        break;
      }

      w->group_ = write_group;
      last_writer = w;
      write_group->size_++;
    }
  }

  write_group->last_writer_ = last_writer;
  // write_group->last_sequence_ =
  //     last_writer->sequence_ + WriteBatch::Count(last_writer->batch_) - 1;
}

void Slot::create_missing_newer_links(Writer* head) {
  while (true) {
    Writer* next = head->link_older_;
    if (next == nullptr || next->link_newer_ != nullptr) {
      assume(next == nullptr || next->link_newer_ == head);
      break;
    }
    next->link_newer_ = head;
    head = next;
  }
}

Slot::Writer* Slot::find_next_leader(Writer* from, Writer* boundary) {
  assume(from != nullptr && from != boundary);
  Writer* current = from;
  while (current->link_older_ != boundary) {
    current = current->link_older_;
    assume(current != nullptr);
  }
  return current;
}

bool Slot::link_group(WriteGroup& write_group, std::atomic<Writer*>* newest_writer) {
  assume(newest_writer != nullptr);
  Writer* leader = write_group.leader_;
  Writer* last_writer = write_group.last_writer_;
  Writer* w = last_writer;
  while (true) {
    // Unset link_newer pointers to make sure when we call
    // CreateMissingNewerLinks later it create all missing links.
    w->link_newer_ = nullptr;
    w->group_ = nullptr;
    if (w == leader) {
      break;
    }
    w = w->link_older_;
  }
  Writer* newest = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    leader->link_older_ = newest;
    if (newest_writer->compare_exchange_weak(newest, last_writer)) {
      return (newest == nullptr);
    }
  }
}

void Slot::launch_parallel_memtable_writers(WriteGroup* write_group) {
  assume(write_group != nullptr);
  write_group->running_.store(write_group->size_);
  for (auto w : *write_group) {
    set_state(w, State::ParallelMemtableWriter);
  }
}

void Slot::exit_as_memtable_writer(Writer* writer, WriteGroup& write_group) {
  Writer* leader = write_group.leader_;
  Writer* last_writer = write_group.last_writer_;

  Writer* newest_writer = last_writer;
  if (!newest_memtable_writer_.compare_exchange_strong(newest_writer,
                                                       nullptr)) {
    create_missing_newer_links(newest_writer);
    Writer* next_leader = last_writer->link_newer_;
    assert(next_leader != nullptr);
    next_leader->link_older_ = nullptr;
    set_state(next_leader, State::MemtableWriteLeader);
  }
  Writer* w = leader;
  while (true) {
    Writer* next = w->link_newer_;
    if (w != leader) {
      set_state(w, State::Completed);
    }
    if (w == last_writer) {
      break;
    }
    assert(next);
    w = next;
  }
  // Note that leader has to exit last, since it owns the write group.
  set_state(leader, State::Completed);
}

// This method is called by both the leader and parallel followers
bool Slot::complete_parallel_memtable_writer(Writer* w) {
  auto* write_group = w->group_;
    std::lock_guard<std::mutex> guard(write_group->leader_->state_mutex());

  if (write_group->running_-- > 1) {
    // we're not the last one
    await_state(w, State::Completed, &complete_parallel_memtable_writer_ctx_);
    return false;
  }

  return true;
}