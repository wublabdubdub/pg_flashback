# ADR-0040：禁用依赖 lookahead spool 反扫的 3/9 eager prune precompute

## 状态

已采纳（2026-04-12）

## 背景

在 14pg `alldb` live 现场继续复核 `random_flashback_2` 后，确认当前
`locator stream + replay metadata frontload` 主线里还有一个实现 bug：

- `scenario_oa_50t_50000r.documents @ '2026-04-11 07:50:40.916510+00'`
  - `3/9 payload ~= 91.805s`
  - 随后 preflight 因 `memory_limit=1GB` 失败
- `scenario_oa_50t_50000r.meetings @ '2026-04-11 07:50:40.916510+00'`
  - `3/9 payload ~= 31.213s`

`gdb` 直取 backend 栈已确认真实热点为：

- `fb_replay_precompute_index_metadata()`
- `fb_replay_build_prune_lookahead()`
- `fb_wal_record_cursor_read_lookahead()`
- `fb_spool_cursor_read() -> FileRead/pread64`

也就是说，当前实现虽然把 locator stub / stats sidecar 主热点移掉了，
却又在 `3/9` 结尾对 worker 产出的 `lookahead_log` 做了一次完整反扫，
把原本计划中的 prune lookahead 预计算重新变成 payload 主热点。

这违反了 ADR-0039 的预算约束：

- `3/9` 允许顺手产出 replay metadata
- 但不允许通过额外的整条 cursor / spool 二次扫描把 payload 再次拉爆

## 决策

在真正的 dense future-block delta 落地前，当前 `3/9` 只保留：

- block lifecycle metadata 前移
- replay block retire 所需 metadata 前移

不再允许：

- 依赖 `lookahead_log`
- 在 `fb_replay_precompute_index_metadata()`
- 对 locator-stream 场景再做一遍 eager prune lookahead 反扫

具体约束：

- 只要 `FbWalRecordIndex` 上仍存在 `lookahead_log`
  这一“需要二次读取 spool 才能构建 prune future”的形态，
  `fb_replay_precompute_index_metadata()` 就必须直接跳过
- `6/9 final` 继续保留现有 lazy fallback：
  - 若 `replay_prune_lookahead_ready = false`
  - 则在 `fb_replay_prepare_final_control()` 内按需构建 prune lookahead

## 为什么这样做

- 这不是放弃 prune metadata 主线，而是停止一个已被证据证明错误的实现形态
- 当前 bug 的本质不是“缺少元数据”，而是“为了元数据又补出一遍完整反扫”
- 先切掉这个 eager 反扫，才能让 `3/9 payload` 重新反映真正的索引/locator 成本
- dense future-block delta 仍是后续目标，但必须在不引入二次全量读的前提下落地

## 结果

预期立即收益：

- `documents` / `meetings` 这类 locator-stream live case 的 `3/9 payload`
  不再被 `fb_replay_build_prune_lookahead()` 主导
- preflight 失败的 case 不再先白白花几十秒做 eager prune precompute

接受的代价：

- 部分开销会临时回到 `6/9 final` 的 lazy fallback
- 这只是过渡形态，不是 dense future-block delta 的最终方案

## 后续

- 补 contract regression：
  - `lookahead_log` 存在时，eager prune precompute 必须保持关闭
- 继续推进真正的 dense future-block delta
  - 落地后再重新评估是否恢复 `3/9` eager metadata
