#pragma once

#include <assert.h>
#include <stdint.h>

#ifndef NDEBUG
  #define assume(x) assert(!!(x))
#else
  #define assume(x) if (!(x)) __builtin_unreachable()
#endif

#define unreachable() __builtin_unreachable()

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

<<<<<<< HEAD
using SequenceNumber = uint64_t;

constexpr size_t SlotSizeDepth = 1;

constexpr size_t SlotSize = 1 << SlotSizeDepth;

struct User {
  int64_t id;
  char user_id[128];
  char name[128];
  int64_t salary;

  static uint64_t hash(int64_t id) {
    // TODO:
    return id;
  }
};

=======
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <cstdio>
#include <string>
#include <chrono>
#include <sys/time.h>
#include <pthread.h>
#include <atomic>
#include <unistd.h>
>>>>>>> wal
