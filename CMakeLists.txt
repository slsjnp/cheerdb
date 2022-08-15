cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 17)

# generate .so file
include_directories(inc)
add_library(interface SHARED
        src/interface.cpp
        src/arena.cpp src/crc32c.cc src/hashtable.cpp src/random.cpp src/status.cpp src/wal.cpp src/coding.cpp src/disk_benchmark.cpp
        src/interface.cpp src/slice.cpp src/test_main.cpp src/config.cpp src/engine.cpp src/log_writer.cc src/slot.cpp src/utils.cpp
        inc/arena.h inc/config.h inc/fmtlog.h inc/list.h inc/random.h inc/status.h inc/wal.h
        inc/city_hash.h inc/crc32c.h inc/fmtlog-inl.h inc/log_writer.h inc/slice.h inc/util.h
        inc/cleanable.h inc/engine.h inc/hashtable.h inc/model.h inc/slice_transform.h inc/record.h
        inc/coding.h inc/env.h inc/interface.h inc/pmemtable.h inc/slot.h inc/wal_format.h
        src/wal_cp.cpp)