#include "../inc/interface.h"
#include "../inc/util.h"
#include "../inc/engine.h"
#include "../inc/fmtlog.h"
#include <iostream>
#include <vector>
#include <string.h>



enum Column{Id=0,Userid,Name,Salary};

std::vector<User> users;

void engine_write( void *ctx, const void *data, size_t len) {
    auto ok = Engine::get().write(ctx, *reinterpret_cast<const User*>(data));
    if (!ok) {
        loge("engine_write");
    }
}

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    auto n = Engine::get().read(
        ctx,
        Engine::Column(select_column),
        Engine::Column(where_column),
        column_key,
        res
    );
    if (!n) {
        loge("engine_read");
        return 0;
    }
    return n.value();
}

void* engine_init(
    const char* host_info, 
    const char* const* peer_host_info_, 
    size_t peer_host_info_num,
    const char* aep_dir, 
    const char* disk_dir
) {
    auto peer_host_info = std::valarray<std::string_view>(peer_host_info_num);
    for (int i = 0; i < peer_host_info_num; ++i) peer_host_info[i] = peer_host_info[i];

    Engine::get().init(
        host_info,
        peer_host_info,
        aep_dir,
        disk_dir
    );
}

void engine_deinit(void *ctx) {
    Engine::get().deinit(ctx);
}
