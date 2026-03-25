# 2026-03-24 主表并行开关基准阻塞记录

## 任务目标

- 单表场景
- 基线数据量 `10000000` 行
- `target_ts -> now` WAL 窗口约 `5GB`
- 对 `pg_flashback.parallel_segment_scan = off/on` 连续各测 `3` 次
- 关注真实主表 flashback 入口，而不是只测 `fb_scan_wal_debug()`

## 本次确认的前提

- `parallel_segment_scan` 当前已在 `scan/debug` 路径生效
- 但 `fb_wal_build_record_index()` 仍是保守全扫
- 因此主表真实 flashback 入口的端到端速度，不应直接套用此前 `fb_scan_wal_debug()` 的加速结论

## 本次执行结果

本轮阻塞分成两段：

1. 早期阻塞是本机 `/isoTest` 容量不足，无法同时稳定容纳：

- `10000000` 行主表基线
- `5GB` 级别目标 WAL 窗口
- flashback 查询期间的数据文件增长 / 中间工作集
- 为防止 `pg_wal` recycle 而准备的 archive fixture

2. 在用户将归档目录迁到 `/walstorage/18waldata` 后，容量 blocker 已解除，并成功得到：
   - 单表 `bench_main`
   - 基线行数 `10000000`
   - `target_ts = 2026-03-24 13:09:29.198271+08`
   - `start_lsn = 58/2B000000`
   - `pg_wal_lsn_diff(current, start_lsn) = 8264334160`
   - 即约 `7.70GB`

但在直接执行真实 flashback 入口时，第一轮即失败：

- `select count(*) from fb_internal_flashback('bench_main'::regclass, '2026-03-24 13:09:29.198271+08'::timestamptz)`
- 错误：`fb does not support WAL windows containing storage_change operations`

进一步用调试入口确认：

- `select fb_scan_wal_debug('bench_main'::regclass, '2026-03-24 13:09:29.198271+08'::timestamptz);`
- 返回：`parallel=off prefilter=off hit_segments=0/0 visited_segments=1475/1475 complete=true anchor=true unsafe=true reason=storage_change`

这说明当前这批 `10M + 7.7GB WAL` 单表夹具，虽然容量条件已满足，但窗口本身已被判为 `unsafe=storage_change`，不能用于真实 flashback 三轮测速。

后续又额外做了两轮更保守的真实 flashback 验证，结果依旧被同一 blocker 拦住：

1. `fb_parallel_flashback_update_only`
   - 目标表 `bench_main` 在 `target_ts` 后只做普通 `UPDATE`
   - 无 `VACUUM`、无 `TRUNCATE`、无后续辅助表 DDL
   - `WAL diff = 826463568`，约 `788MB`
   - 直接执行 `fb_internal_flashback()` 仍报：
     - `fb does not support WAL windows containing storage_change operations`
2. `fb_parallel_flashback_noise_only`
   - 目标表 `bench_target` 在 `target_ts` 后完全不再变更
   - 仅由独立噪声表 `bench_noise` 生成 `WAL diff = 409213208`，约 `390MB`
   - 直接执行 `fb_internal_flashback('bench_target', target_ts)` 仍报同样错误

这说明当前 blocker 已经不只是“某个 workload 含显式危险操作”，而是“多 WAL 的真实 flashback 场景在当前实现下会稳定触发 `storage_change` 判定”，至少到本轮验证为止仍无法给出真实入口的 `off/on` 对比样本。

## 2026-03-24 内核级定位结论

基于 `fb_parallel_flashback_noise_only` 场景，已做一轮 live backend `gdb` 调试，结论如下：

1. 真实命中的不是 `RM_SMGR_ID` 的“窗口内后续噪声表记录”，而是 `RM_STANDBY_ID` 路径：
   - 断点位置：`src/fb_wal.c:2452`
   - 栈：`pg_flashback -> fb_wal_build_record_index() -> fb_wal_visit_records() -> fb_index_record_visitor()`
   - 现场值：
     - `lock_xid = 24370`
     - `info->relid = 3450949`
     - `info->locator.dbOid = 3450922`
2. 用 `pg_waldump` 还原该 LSN 附近记录：
   - `lsn = 5F/0D989960`
     - `rmgr: Standby ... desc: LOCK xid 24370 db 3450922 rel 3450949`
   - `lsn = 5F/0D989990`
     - `rmgr: Storage ... desc: CREATE base/3450922/3450949`
3. `3450949` 正是目标表 `bench_target` 的 `oid/relfilenode`。
4. 但这两条记录都发生在当前测试窗口 `start_lsn = 5F/1BA70910` 之前：
   - SQL 已确认：
     - `'5F/0D989960'::pg_lsn < '5F/1BA70910'::pg_lsn`
     - `'5F/0D989990'::pg_lsn < '5F/1BA70910'::pg_lsn`

因此当前长期卡住的真正原因已经明确：

- `fb_wal_build_record_index()` 当前会从“`target_ts` 前最近 checkpoint anchor”开始整段扫描。
- 在这段“为找 anchor 而回看的 pre-target 区间”里，目标表最初 `CREATE TABLE` 对应的 `Standby LOCK + Storage CREATE` 也会被当前逻辑直接记为 `FB_WAL_UNSAFE_STORAGE_CHANGE`。
- 代码位置见：
  - `src/fb_wal.c:2438-2454`
  - 这里对 `RM_STANDBY_ID` / `RM_SMGR_ID` 命中关系后直接 `fb_mark_unsafe(ctx, FB_WAL_UNSAFE_STORAGE_CHANGE)`，没有额外区分“记录发生在 target 前还是 target 后”。

这说明当前“不懂得的地方”不是 workload，而是 unsafe 判定边界：

- 代码把“anchor 之前、但为同一 relation 建表所必需的历史记录”也当成了当前 flashback 窗口内不可接受的 storage change。
- 这会把本应合法的“relation 在 target 前创建、target 后只有普通查询或无变更”的场景误报成 `storage_change`。

本次实际观察到的关键现象：

1. 宽表版本在 workload 中很快把 `/isoTest` 顶到 `96% ~ 100%`。
2. 将 archive fixture 挪到 `/tmp` 后，`/isoTest` 仍会被主表 update 产生的数据文件增长打满。
3. 窄表 + `VACUUM` 版本虽然能明显降低主表体量，但为了累计到 `5GB` WAL 仍需要多轮 workload；在本机剩余空间下仍无法安全完成。
4. 最近一次窄表试跑中，实际确认的窗口 WAL 只有：
   - `target_ts = 2026-03-24 12:24:47.201463+08`
   - `start_lsn = 58/1900D138`
   - `pg_wal_lsn_diff(now, start_lsn) = 284948224`
   - 即约 `272MB`
   - 远未达到目标 `5GB`

## 当前结论

- 目前仍无法得到“单表 `10000000` 行 + `5GB` WAL + `off/on` 各 3 轮”的真实 flashback 速度结论。
- 原因已经从单纯的环境容量 blocker，变成了“现成夹具窗口被判定为 `unsafe=storage_change`”。
- 在不重造安全时间窗的前提下，当前只能继续测 `fb_scan_wal_debug()` 这类扫描调试路径，不能测真实 `fb_internal_flashback()`。

## 后续建议

至少满足以下任一条件后再重跑：

1. 保留当前已验证可用的 archive 迁移方案，继续确保 `PGDATA` 与 archive 不争盘。
2. 重新制作“不含 `storage_change`”的安全时间窗，再跑真实 flashback 3 轮。
3. 若只是验证并行扫描趋势而非真实 flashback 结论，可改测 `fb_scan_wal_debug()` / `fb_recordref_debug()`。
4. 若仍受容量影响，再考虑：
   - 为 `18pgdata` 所在文件系统额外预留 `>= 15GB` 可用空间。
   - 先清理 `/isoTest/18pgdata/pg_flashback/recovered_wal`、无关历史数据库或其他大目录，再 fresh 重跑。
5. 若只是验证功能趋势而非最终报告，可先把目标降为：
   - `10000000` 行
   - `1GB ~ 2GB` WAL
   - 先观察主表真实 flashback 入口在 `off/on` 下是否已有可见差异

## 补充

- 本次过程中已清理：
  - `fb_parallel_speed_bench`
  - `/tmp/main_parallel_bench_v3`
  - `/tmp/main_parallel_bench_v4`
- 当前仓库未产生代码改动，只记录阻塞信息。
