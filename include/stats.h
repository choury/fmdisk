#ifndef STATS_H__
#define STATS_H__

#include <string>

// 运行统计计数器(进程内原子量, 生命周期与挂载一致, 不落盘)
enum fm_stat_id {
    // 需求读(prefetch wait=true)的块请求, STALE 早退与预读不计入;
    // truncate 收缩路径的补块预取同样走 wait=true, 也计入
    FM_STAT_READ_BLOCK_HIT,      // 命中本地缓存
    FM_STAT_READ_BLOCK_MISS,     // 未命中, 触发整块拉取
    // 块拉取(block_t::pull 实际处理的入口, 含 STALE 早退与后台预读)
    FM_STAT_PULL_TOTAL,
    FM_STAT_PULL_SEED_EMPTY,     // 远端缺块(ENOENT), 本地补零
    // 块推送(block_t::push 实际处理的入口)
    FM_STAT_PUSH_TOTAL,
    FM_STAT_PUSH_OK,             // 上传成功且生效
    FM_STAT_PUSH_ZERO,           // 全零块免上传(本地打洞), 推送仍生效故亦计入 push_ok
    FM_STAT_PUSH_LOCAL_FAIL,     // 本地故障: 读缓存/存库(存库失败会回滚已传数据)
    FM_STAT_PUSH_UPLOAD_FAIL,    // 远端上传失败(fm_upload)
    FM_STAT_PUSH_DISCARD_RACE,   // 上传后因并发写版本变化被丢弃(无效上传)
    // meta 推送(upload_meta)
    FM_STAT_META_PUSH_TOTAL,
    FM_STAT_META_PUSH_FAIL,
    FM_STAT_META_PUSH_STALE,     // 上传成功但版本已过期, 需重推
    // 目录条目缓存(load_entry_from_db)
    FM_STAT_ENTRY_HIT,
    FM_STAT_ENTRY_MISS,
    FM_STAT_MAX
};

void fm_stat_add(enum fm_stat_id id, long long val = 1);
long long fm_stat_get(enum fm_stat_id id);
void fm_stats_reset(void);
const char* fm_stat_name(enum fm_stat_id id);

// 按 dump 输出中的 key 名查找计数器, 未找到返回 -1
int fm_stat_find(const char* name);

// key=value 多行文本, 含派生行(read_block_total/read_miss_rate/entry_hit_rate等)
std::string fm_stats_dump(void);

#endif // STATS_H__
