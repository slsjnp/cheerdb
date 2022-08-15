//
// Created by sj on 22-8-12.
//

#ifndef INTERFACE_WAL_FORMAT_H
#define INTERFACE_WAL_FORMAT_H




namespace CHEERDB_NAMESPACE {
    namespace log {

        enum RecordType {
            // Zero is reserved for preallocated files
            kZeroType = 0,

            kFullType = 1,

            // For fragments
            kFirstType = 2,
            kMiddleType = 3,
            kLastType = 4
        };
        static const int kMaxRecordType = kLastType;

        static const int kBlockSize = 4096;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
        static const int kHeaderSize = 4 + 2 + 1;

    }  // namespace log
}  // namespace cheerdb

#endif //INTERFACE_WAL_FORMAT_H
