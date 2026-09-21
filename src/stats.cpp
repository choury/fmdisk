#include "stats.h"

#include <atomic>
#include <cstdio>
#include <cstring>

static std::atomic<long long> g_stats[FM_STAT_MAX];

static const char* g_stat_names[FM_STAT_MAX] = {
    "read_bytes",
    "read_bytes_miss",
    "write_bytes",
    "pull_total",
    "pull_seed_empty",
    "pull_download_fail",
    "pull_local_fail",
    "push_total",
    "push_zero",
    "push_ok",
    "push_local_fail",
    "push_upload_fail",
    "push_discard_race",
    "meta_push_total",
    "meta_push_fail",
    "meta_push_stale",
    "entry_hit",
    "entry_miss",
    "meta_hit",
    "meta_miss",
};

void fm_stat_add(enum fm_stat_id id, long long val) {
    if(id < 0 || id >= FM_STAT_MAX) {
        return;
    }
    g_stats[id].fetch_add(val, std::memory_order_relaxed);
}

long long fm_stat_get(enum fm_stat_id id) {
    if(id < 0 || id >= FM_STAT_MAX) {
        return 0;
    }
    return g_stats[id].load(std::memory_order_relaxed);
}

void fm_stats_reset(void) {
    for(int i = 0; i < FM_STAT_MAX; i++) {
        g_stats[i].store(0, std::memory_order_relaxed);
    }
}

const char* fm_stat_name(enum fm_stat_id id) {
    if(id < 0 || id >= FM_STAT_MAX) {
        return "";
    }
    return g_stat_names[id];
}

int fm_stat_find(const char* name) {
    if(name == nullptr) {
        return -1;
    }
    for(int i = 0; i < FM_STAT_MAX; i++) {
        if(strcmp(name, g_stat_names[i]) == 0) {
            return i;
        }
    }
    return -1;
}

static std::string percent(long long part, long long total) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.2f%%", total > 0 ? 100.0 * part / total : 0.0);
    return buf;
}

std::string fm_stats_dump(void) {
    long long v[FM_STAT_MAX];
    for(int i = 0; i < FM_STAT_MAX; i++) {
        v[i] = g_stats[i].load(std::memory_order_relaxed);
    }
    long long read_total = v[FM_STAT_READ_BYTES];
    long long read_miss = v[FM_STAT_READ_BYTES_MISS];
    long long entry_total = v[FM_STAT_ENTRY_HIT] + v[FM_STAT_ENTRY_MISS];
    long long meta_ok = v[FM_STAT_META_PUSH_TOTAL] - v[FM_STAT_META_PUSH_FAIL]
                      - v[FM_STAT_META_PUSH_STALE];
    if(meta_ok < 0) {
        meta_ok = 0;
    }
    std::string s;
    s.reserve(512);
    auto line = [&s](const char* key, const std::string& val) {
        s += key;
        s += "=";
        s += val;
        s += "\n";
    };
    line("read_bytes", std::to_string(read_total));
    line("read_bytes_miss", std::to_string(read_miss));
    line("read_bytes_hit", std::to_string(std::max(0LL, read_total - read_miss)));
    line("read_miss_rate", percent(read_miss, read_total));
    line("write_bytes", std::to_string(v[FM_STAT_WRITE_BYTES]));
    line("pull_total", std::to_string(v[FM_STAT_PULL_TOTAL]));
    line("pull_seed_empty", std::to_string(v[FM_STAT_PULL_SEED_EMPTY]));
    line("pull_download_fail", std::to_string(v[FM_STAT_PULL_DOWNLOAD_FAIL]));
    line("pull_local_fail", std::to_string(v[FM_STAT_PULL_LOCAL_FAIL]));
    line("push_total", std::to_string(v[FM_STAT_PUSH_TOTAL]));
    line("push_zero", std::to_string(v[FM_STAT_PUSH_ZERO]));
    line("push_ok", std::to_string(v[FM_STAT_PUSH_OK]));
    line("push_local_fail", std::to_string(v[FM_STAT_PUSH_LOCAL_FAIL]));
    line("push_upload_fail", std::to_string(v[FM_STAT_PUSH_UPLOAD_FAIL]));
    line("push_discard_race", std::to_string(v[FM_STAT_PUSH_DISCARD_RACE]));
    line("meta_push_total", std::to_string(v[FM_STAT_META_PUSH_TOTAL]));
    line("meta_push_ok", std::to_string(meta_ok));
    line("meta_push_fail", std::to_string(v[FM_STAT_META_PUSH_FAIL]));
    line("meta_push_stale", std::to_string(v[FM_STAT_META_PUSH_STALE]));
    line("entry_total", std::to_string(entry_total));
    line("entry_hit", std::to_string(v[FM_STAT_ENTRY_HIT]));
    line("entry_miss", std::to_string(v[FM_STAT_ENTRY_MISS]));
    line("entry_hit_rate", percent(v[FM_STAT_ENTRY_HIT], entry_total));
    long long meta_cache_total = v[FM_STAT_META_HIT] + v[FM_STAT_META_MISS];
    line("meta_hit", std::to_string(v[FM_STAT_META_HIT]));
    line("meta_miss", std::to_string(v[FM_STAT_META_MISS]));
    line("meta_hit_rate", percent(v[FM_STAT_META_HIT], meta_cache_total));
    return s;
}
