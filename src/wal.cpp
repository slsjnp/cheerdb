//
// Created by sj on 22-7-23.
//

#include "wal.h"
#include "slice.h"
#include "status.h"

namespace CHEERDB_NAMESPACE {
    void WalAddition::EncodeTo(std::string* dst) const{
        PutVarint64(dst, number_);

        if (metadata_.HasSyncedSize()){

        }

    }
}
