# TODO.md

## 当前公开接口

- [ ] 公开安装面仅提供 `pg_flashback(anyelement, text)`
- [x] 查询型调用形态固定为 `NULL::schema.table`
- [x] `pg_flashback()` 不创建结果表
- [x] `pg_flashback()` 不返回结果表名
- [x] `FROM pg_flashback(...)` 不再经过 PostgreSQL `FunctionScan` / `tuplestore`
- [x] 库内全表闪回落地新表的正式承接面固定为 `CTAS AS SELECT * FROM pg_flashback(...)`
- [x] `COPY (SELECT * FROM pg_flashback(...)) TO ...` 仅作为导出路径

## 本轮已完成

- [x] 按对外开源发布口径补齐许可证与 README
  - 根目录新增 `Apache-2.0` 许可证文件 `LICENSE`
  - 根目录新增发布版本文件 `VERSION`，当前版本为 `0.2.0`
  - 扩展安装版本已前滚到 `0.2.0`
  - 已补齐 `sql/pg_flashback--0.2.0.sql`
    与 `sql/pg_flashback--0.1.1--0.2.0.sql`
  - 根目录 `README.md` 已收口为面向开源用户的极简入口文档
  - README 当前只保留：
    - 核心功能与“为什么 PostgreSQL 自带做不到”
    - 基础调用方式
    - 使用前提条件
    - 几个常用参数的作用与最小用法
  - README 中 PostgreSQL 版本口径当前按本机已验证的 `PG12-18` 书写
  - `scripts/sync_open_source.sh` 已把 `LICENSE` / `VERSION` 纳入镜像白名单
- [x] 开源项目公开 Markdown 切到单文件中英双语
  - 范围包括：
    - `open_source/pg_flashback/README.md`
  - 双语切换固定为 `中文 | English` 锚点跳转
  - 当前开源导出面已进一步收口：
    - `open_source/` 根目录不再保留内部说明文件或 manifest
    - `open_source/pg_flashback/` 不再携带 `docs/`、`tests/`、研发记录或 AI 开发痕迹
- [x] 修复 release gate truth 使用 MVCC snapshot、而 flashback 仅按 commit timestamp 判定可见性的 correctness 缺口
  - 根因已确认：
    - `capture_truth_snapshots.sh` 在 `repeatable read` 事务里导出的 truth，
      本质上对应“目标时刻的 MVCC snapshot 可见性”
    - 旧版 `pg_flashback()` 只看 WAL commit timestamp，
      会把“target snapshot 时仍 in-progress，但随后提交且 commit_ts 仍早于 target_ts”的事务
      误判为 `committed_before_target`
    - 这会把 truth 中本应保留的旧版本错误替换成新版本，
      最终表现为 release gate `users` / `meetings`
      的 `row_count / sha256 mismatch`
  - 当前修复已落地：
    - 新增 GUC `pg_flashback.target_snapshot`
    - WAL record index 现会解析 `txid_current_snapshot()::text`
      的 active xid 列表，并把这些 xid 视为 target 时刻“仍未提交”
    - `tests/release_gate/bin/capture_truth_snapshots.sh`
      现会把 `target_snapshot` 写入 truth manifest
    - `tests/release_gate/bin/run_flashback_matrix.sh`
      现会把 `target_snapshot` 注入 query / COPY / CTAS-create
      的 flashback 命令
  - 当前已验证：
    - 手工对 `random_flashback_1.users` 失败行注入 snapshot 后，
      已恢复 truth 值
    - 手工对 `random_flashback_1.meetings` 失败行注入 snapshot 后，
      已恢复 truth 值
    - `bash -n tests/release_gate/bin/capture_truth_snapshots.sh`
      / `bash -n tests/release_gate/bin/run_flashback_matrix.sh`
      已通过
- [x] 修复无主键 bag residual 历史行首条丢失的问题
  - 根因已确认：
    - `fb_bag_apply_finish_scan()` 会把 `residual_cursor` 初始化到
      `entries_head`
    - 但 `fb_bag_emit_residual()` 首次发射 residual 时先推进到
      `all_next`
    - 导致 bag residual 链表头永远不会被发射
  - 当前修复已落地：
    - `fb_bag_emit_residual()` 改成显式维护当前 entry 的剩余发射次数
    - 只有当前 entry 发完后才推进到下一个 residual entry
    - `fb_flashback_bag` 已补最小 RED：
      - 无主键表 `update -> delete 到空表 -> vacuum`
      - 回看 `after_insert / after_update / after_delete_one`
        三个时间点均断言 residual 行完整返回
  - 当前已验证：
    - 手工 PG18 `alldb` 同类 case 恢复正确结果
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_bag'`
      `All 1 tests passed.`
- [x] 修复扩展同版本号接口漂移导致的升级缺口
  - 根因已确认：
    - 历史上曾在同一 `extversion = '0.1.0'` 下先后安装过
      `pg_flashback(text, text, text)` 与 `pg_flashback(anyelement, text)`
    - 仅执行 `make install` / `CREATE EXTENSION` 不会刷新已存在数据库中的扩展对象
  - 当前修复已落地：
    - 前滚 `default_version` 到 `0.1.1`
    - 恢复历史 `sql/pg_flashback--0.1.0.sql`
    - 新增 `sql/pg_flashback--0.1.1.sql`
    - 新增 `sql/pg_flashback--0.1.0--0.1.1.sql`
    - 新增升级回归 `fb_extension_upgrade`
  - 当前已验证：
    - `make ... installcheck REGRESS='fb_extension_upgrade fb_user_surface pg_flashback'`
      `All 3 tests passed.`
    - `alldb` 上执行 `ALTER EXTENSION pg_flashback UPDATE TO '0.1.1'` 后，
      用户两参调用不再报 `function does not exist`
- [x] 修复 `fb_replay` 回归夹具的幂等性缺口
  - `sql/fb_replay.sql` 已为
    `fb_replay_prune_lookahead_snapshot_isolation_debug()` 补齐 `DROP FUNCTION`
  - 已用最小复现确认：
    - `pg_regress ... fb_user_surface fb_replay`
      不再因 `CREATE FUNCTION already exists` 失败
  - 已同步 `expected/fb_replay.out`
  - 全量 `installcheck` 当前结果：
    - `All 36 tests passed.`
- [x] 修复 `fb_custom_scan` / `fb_replay_debug` 的重复 replay `heap insert` 问题
  - 根因已确认：
    - final replay 的 record cursor 仍可能把相邻重复 `record.lsn`
      的 WAL record 喂入一次以上
    - 同一条 `heap insert` 第二次回放时，目标 offset 已占用，
      于是报 `failed to replay heap insert`
  - 当前修复已落地：
    - `fb_replay_run_pass()` 对 exact duplicate `record.lsn` 做跳过
  - 当前已验证：
    - `sql/fb_custom_scan.sql` 直跑通过
    - `make ... installcheck REGRESS='fb_custom_scan' ...`
      `All 1 tests passed.`
    - `fb_replay_debug` no-count 场景恢复 `errors=0`
    - 同 session `count(*) -> full replay` 的实际结果集行数仍为 `20000`
- [x] 修复 prune lookahead 漏算 cleanup slot release 导致的 `failed to replay heap update`
  - 根因已确认：
    - final replay 会复用 warm pass 的 block state
    - `fb_replay` 的 prune lookahead 旧逻辑
      没有把后续 `PRUNE_VACUUM_CLEANUP` 释放出来的 slot
      反向折算回 future constraints
    - 于是 `users blk 10095` 上更早 prune image
      被误判成 future insert slot 不可用，final replay 错保留
      pre-cleanup 页状态
    - 后续在
      `BA/4472E390` 报 `failed to replay heap update`
  - 当前修复已落地：
    - `src/fb_replay.c` 为 `FB_WAL_RECORD_HEAP2_PRUNE`
      新增 future compose 的 slot-release 语义
    - `src/fb_replay.c` 额外补
      `state->page_lsn > record->end_lsn` hardening guard
    - 新增独立回归 `fb_replay_prune_future_state`
  - 当前已验证：
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_replay_prune_future_state'`
      `All 1 tests passed.`
    - `scenario_oa_50t_50000r.users @ '2026-04-08 00:38:25.357868+00'`
      已完整返回 `count = 50015`
- [x] 修复 prune future constraints / future guard 漏判导致的 `failed to replay heap multi insert`
  - 根因已确认：
    - `FB_WAL_RECORD_HEAP2_MULTI_INSERT` 旧逻辑未进入 prune lookahead 的
      future constraints
    - data prune future guard 又把 future old tuple 过度从
      `nowdead/redirected` 中剔除，与当前 dead/redirect 可追踪语义不一致
  - 当前修复已落地：
    - `src/fb_replay.c` 为 `HEAP2_MULTI_INSERT`
      补 future compose / same-block future support
    - `src/fb_replay.c` 调整 prune future guard，只屏蔽会真正破坏
      future old/new 约束的 `nowunused` / `nowdead` / `redirected`
    - `fb_replay_prune_future_state`
      新增 multi-insert preserve 最小回归
  - 当前已验证：
    - `SELECT fb_replay_prune_image_preserve_next_multi_insert_debug();`
      返回 `prune_image_preserve_next_multi_insert=true`
    - `scenario_oa_50t_50000r.leave_requests @ '2026-04-08 01:37:20.067024+00'`
      串行与默认并行都已返回 `count = 49887`
- [x] 建立统一“闪回失败修复台账”
  - 新文档：
    - `docs/reports/flashback-failure-fix-log.md`
  - 当前约束已固定：
    - 只收录“已定位根因并已完成修复”的 flashback failure
    - 每条记录固定为：
      - 时间
      - 报错现象
      - 原因
      - 修复方式
    - 后续每次修复新的闪回失败，都必须同步更新该文档
- [x] 调整 release gate `run_flashback_checks` 的 flashback 默认脚本口径
  - 当前修复已落地：
    - `tests/release_gate/bin/run_flashback_matrix.sh`
      现对每条 flashback 默认设置
      `PGOPTIONS='-c pg_flashback.memory_limit=6GB'`
    - `tests/release_gate/bin/common.sh`
      当前默认改成
      `FB_RELEASE_GATE_WARMUP_RUNS=0`
      / `FB_RELEASE_GATE_MEASURED_RUNS=1`
      ，同一条 flashback case 默认只执行 `1` 次
    - 同脚本现会打印每条 query/copy/ctas-create 的实际 flashback SQL
  - 当前已验证：
    - `bash tests/release_gate/bin/selftest.sh`
      `PASS`

## 当前进行中

- [ ] 重做 release gate 最终报告，提升中文可读性并反映测试过程
  - 目标：
    - 报告统一改成中文
    - 显式展示阶段过程、随机 snapshot / DML snapshot / flashback 执行矩阵
    - 将 `correctness` / `performance` / `missing golden baseline`
      分离展示
    - 增加 `target_snapshot` 覆盖率提示，帮助识别“是否使用最新 truth capture 口径”
    - 保证各阶段脚本彻底解耦，单步成功执行不会被历史产物污染
  - 当前已确认：
    - `tests/release_gate/output/latest/json/golden/pg18` 等价物仍为空基线，
      不能把“缺性能基线”继续渲染成“性能失败”
    - `output/latest/json/truth_manifest.json`
      当前不含 `target_snapshot`
    - `nohup.out` 中真正的 correctness mismatch 只有 `4` 条，
      其余大量 `FAIL` 来自报告把“未评估 / 缺基线”混成失败
    - `random_snapshot_capture.log` / `dml_snapshot_capture.log`
      当前按追加写入，已出现跨多轮 run 混杂
- [x] 修复“较早 prefix gap 误杀较新连续 suffix”的 WAL resolver 问题
  - resolver 不再因旧 prefix 上的不可恢复 gap 在 `2/9` 直接报 `missing segment`
  - 当前改为保留最新端连续 suffix；只有 suffix 自身不连续时才继续报 WAL 不完整
  - 新增回归 `fb_wal_prefix_suffix`
- [x] 修复串行 payload path 在 split materialize windows 下漏掉跨 segment 记录头的问题
  - `fb_flashback_toast_storage_boundary` 的跨 `80/5C -> 80/5D` heap insert 已恢复通过
  - 当前在 payload bgworker 重新启用前，串行 payload 路径不再使用 split windows
  - live case `documents @ 2026-03-29 14:10:13` 在 `memory_limit='8GB'` 下仍返回 `count = 4356409`
- [x] 止血 `documents @ 2026-03-29 14:10:13` 的 postmaster 级 crash：
  - 现场 `PANIC: stuck spinlock detected at get_hash_entry`
  - 当前先硬关闭 dynamic payload bgworker/DSM 路径，强制回落串行 payload materialize
  - 增加 full-range anchor fallback，避免 `fb_wal_sidecar` 因预过滤窗口裁掉 checkpoint 而误报缺锚点
  - live 复核：
    - `memory_limit='6GB'` 返回真实 preflight 内存报错，不再 crash
    - `memory_limit='8GB'` 返回 `count = 4356409`
- [x] 修复 summary-first payload 窗口残留重叠导致的重复 replay：
  - 新增全局 visit-window merge
  - 串行 payload materialize 增加 emit-floor，避免重叠窗口重复 append 同一条 WAL record
  - 新增回归 `fb_summary_overlap_toast`
  - 清空 `meta/summary` 后复跑 `fb_recordref` / `fb_flashback_toast_storage_boundary`，确认旧 fallback 路径仍可正确工作
  - live case `documents @ 2026-03-29 14:10:13` 已不再报 replay 内核错误，`8GB` 下返回 `count = 4356409`
- [x] 修复 summary-first prefilter 的 pthread worker 误用 `palloc/psprintf`，恢复 `3/9 build record index` 不再 backend `SIGSEGV`
- [x] 修复 WAL payload 并行失败后的回退路径，避免主动终止 worker 后又被正常 wait 校验重新抬成 `ERROR`
- [x] 复核用户 case `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'`：
  - 默认 `memory_limit=1GB` 当前返回真实 preflight 内存报错，不再 crash
  - `pg_flashback.memory_limit='8GB'` 已返回 `count = 4356409`
- [x] 记录开发期 core / gdb 临时文件统一落盘到 `/isoTest/tmp`，不再使用 `/tmp`
- [x] 修复 `FbApplyScan` 直接返回 apply 内部 slot，导致 residual 历史行参与上层表达式投影/排序时崩溃的问题
- [x] 修复 keyed 单列 typed-key fast path 的单 key hash 不一致，恢复 `WHERE 主键 = const` / `IN (...)` 对更新行与已删除历史行的正确命中
- [x] 用 `scenario_oa_12t_50000r.documents` 复核 `id=10`：
  - `2026-03-29 20:00:13` 返回 `1`
  - delete commit 实际发生在 `2026-03-29 22:42:32.744216+08`
  - `2026-03-29 22:45:00` 返回 `0`
- [x] 移除 `pg_flashback()` 内部 materialized SRF，统一收口为 `ValuePerCall`
- [x] 新增本机 nightly 自动提交脚本并安装 `01:00` 用户级 `cron`
- [x] 细化 `docs/architecture/`，并按当前代码现状重写三份中文手册
- [x] 删除旧公开入口 `pg_flashback(text, text, text)`
- [x] 删除旧结果表物化逻辑
- [x] 删除 `fb_parallel` 模块
- [x] 删除 `pg_flashback.parallel_apply_workers`
- [x] 将 `pg_flashback()` 改成直接查询型 SRF
- [x] 将 keyed apply 改为变化 key 驱动
- [x] 将 bag apply 改为 delta 驱动
- [x] 迁移回归到新接口
- [x] 迁移 deep SQL 主线到新接口
- [x] 重写核心维护文档到当前实现口径
- [x] 登记 `pg_flashback_summary_progress` 的 WAL 连续性观测缺口：
  - 清理部分较老归档后，query-side 已会因中间 segment 缺口报 `WAL not complete`
  - 当前 progress 视图仍只统计 summary 文件缺口，未把真实 WAL 缺口折算进 `missing_segments` / `first_gap_*`
- [x] 修复 `summary service` 未随源 WAL 删除而清理失活 summary 索引的问题
  - 已完成：
    - `summary service` cleanup 现会按当前可见 WAL 源集合探活
    - 超出 recent protect 窗口后，失活 segment 对应的 summary 文件会自动删除
    - cleanup 只接受严格匹配 `summary-<hash>.meta` 的正式文件名，不再误删构建中的 `.tmp.*`
  - 已验证：
    - `fb_summary_service` 回归新增“删除 WAL 后删除对应 summary 文件”断言并通过

## 当前待办
  - [x] 调整 release gate `run_flashback_checks`：
    - 每条 flashback case 的 CSV 产出后立即对比 truth accuracy
    - 在 `run_flashback_matrix.sh` 即时输出 correctness 结果
    - 保留 `evaluate_gate.sh` 的最终汇总职责
  - [ ] 彻底收敛 release gate `run_flashback_checks` 的 `3/9 build record index`
  - [x] 先补最小 RED / 观测，锁住 `summary-span` 与 `xact-status` 的新 counters
  - [x] 将 `summary-span` 从“query 时现拷 spans”升级成 cache 期 stable public slice
  - [x] 在 query-side `summary-span` 规划中加入 segment 去重与更早 merge，避免窗口爆炸
  - [x] 给 `xact-status` 接通 metadata 期 `xact_summary_log`，查询期先消费 query-local spool
  - [x] 为 `xact-status` 增加 summary-first 后的精确补洞路径，避免直接退化到 segment 级 WAL fallback
  - [x] 仅在精确补洞仍失败时才继续 WAL fallback，并继续压缩 fallback 覆盖范围
    - [x] 为 unresolved xid 增加“全局 summary exact lookup”，不再受 relation span windows 限制
    - [x] 优先覆盖“summary 全覆盖但 commit/abort 落在后续 segment”场景
    - [x] 修复 summary build 的 `RM_XACT_ID` parse 口径：
      - `src/fb_summary.c` 现已改成把完整 `XLogRecGetInfo(reader)`
        传给 `ParseCommitRecord()` / `ParseAbortRecord()`
      - `FB_SUMMARY_VERSION` 已前滚到 `10`，强制旧 summary 重建
      - `fb_recordref` 已新增 commit-subxact / abort-subxact RED，
        锁住“仅靠 summary xid outcome 即可解出，不再回退到 WAL”
    - [x] 用回归锁住：
      - [x] abort-subxact `unresolved_touched = 0`
      - [x] abort-subxact `unresolved_unsafe = 0`
      - [x] abort-subxact `fallback_windows = 0`
  - [x] 用 `./run_release_gate.sh --from run_flashback_checks` 现场复跑
  - [ ] 继续压缩 `3/9 30% metadata`，直到 summary 驱动步骤全部稳定落到 `< 5s`
    - [x] 首个 release gate query case 已压到：
      - `summary-span` `29.646 ms`
      - `xact-status` `525.584 ms`
      - `payload` `516.234 ms`
    - [x] `metadata` 已在同机二次复跑压到 `3229.323 ms`
    - [ ] 当前剩余唯一超目标项已转成 `payload` `10951.771 ms`
    - [ ] 继续追 archive source 上 `summary_payload_locator_records = 0`
      / `summary_payload_locator_public_builds = 0`
      / `summary_payload_locator_fallback_segments = 364`
      的残余 summary locator 缺口
      - [ ] 先对账 payload locator 四层链路：
        - [x] summary build 写入
        - [ ] summary cache load
        - [ ] query lookup/public slice
        - [x] archive source identity
      - [x] 补最小 RED，锁住“同一 archive segment 切换等价 archive 路径后仍复用同一 summary identity”
      - [x] 调整 summary identity/hash 契约，去掉对 archive 路径字符串的绑定
      - [x] 复核 summary service / cleanup / query lookup 在默认 `archive_command`
        与显式 `archive_dest` 双口径下不再互相打架
      - [ ] 在修复 archive source identity 后，复跑 release gate 现场，
        继续确认 `summary_payload_locator_public_builds` 与 `3/9 payload`
        是否同步收敛
  - [x] 继续追并修掉 `summary_xid_fallback=21` 的 residual xid outcome 缺口
    - [x] 现场已确认不是“summary 完全未命中”
    - [x] 现场已确认 `summary_xid_hits=27366`，但仍有
          `summary_xid_fallback=21`
    - [x] 现场已确认当前 release gate case 中
          `xact_summary_spool_records=0`
          / `xact_summary_spool_hits=0`
    - [x] 修复 `fb_summary_xid_resolution_debug(...)` 大 case backend 崩溃，
          恢复 unresolved xid 样本诊断
    - [x] 用 unresolved xid 样本对账 summary outcome / WAL，
          已确认 phantom xid 不存在于当前 archive / `pg_wal`
    - [x] 修复 summary file identity/hash 过弱的问题，
          阻断 stale summary 复用：
      - [x] `src/fb_summary.c` identity 现已绑定
            `source_kind + segment_name + file_size + st_mtime + st_ctime + xlp_sysid`
      - [x] 新 hash 生效后已强制当前稳定窗 summary 重建
    - [x] 复跑 release gate 首个 query case，
          已确认不再因 residual xid 掉回 WAL fallback：
      - [x] `summary_xid_fallback=0`
      - [x] `xact_fallback_windows=0`
      - [x] `3/9 55% xact-status` 已收敛到约 `1s`
  - [ ] 完成 correctness-only 口径的 release gate truth 对比
    - [x] `random_flashback_1.documents @ 2026-04-07 04:41:28.555065+00`
          已对上 truth：
          `sha256 = 4edde6e0b1e1ee94e1f9e2de12856bb1a50a85e73431dcbceae16a8e18117e0c`
          / `row_count = 1949969`
    - [x] 已确认
          `random_flashback_1.users` /
          `random_flashback_1.meetings`
          的 mismatch 根因是“truth snapshot 可见性”和
          “仅按 commit timestamp 判定”之间的口径缺口，
          不是 replay 内核损坏
    - [x] `capture_truth_snapshots.sh`
          现会记录 `txid_current_snapshot()::text`
          到 truth manifest 的 `target_snapshot`
    - [x] `run_flashback_matrix.sh`
          现会把 `target_snapshot`
          通过 `PGOPTIONS=-c pg_flashback.target_snapshot=...`
          注入 flashback query / COPY / CTAS-create
    - [ ] 用新 manifest 重新采集 truth 并复跑 correctness-only 对比，
          确认 `users` / `meetings` mismatch 清零
    - [x] 同一 `users` case 上的
          `failed to replay heap update`
          已定位并修掉：
          - 根因是 prune lookahead 未折算后续 cleanup 的 slot release，
            导致更早 prune image 被误判成 future insert 不可用
          - 当前已由 `fb_replay_prune_future_state` 锁住
    - [x] 修掉
          `random_flashback_1.users @ 2026-04-07 04:41:28.555065+00`
          在现有 `meta/summary` 下的
          `ERROR: pfree called with invalid pointer ...`
          - 根因已确认：
            `fb_summary_segment_lookup_payload_locators_cached()`
            第二轮收集 multi-match payload locator slice 时，
            会在确认 `slice_count` 之前先写
            `matched_slices[slice_index]` /
            `matched_counts[slice_index]`
          - 当最后一个正样本后面仍有 `slice_count = 0`
            的匹配 relation 时，会按
            `slice_index == match_count`
            越界写坏 scratch chunk header，
            最终在尾部 `pfree(positions)` 报 invalid pointer
          - 当前修复已落地：
            - 改成“先拿临时 `slice_count`，确认非零后再写 scratch arrays”
            - 删除旧 `positions` merge，改成
              append + sort + deduplicate
            - 新增回归 `fb_summary_payload_locator_merge`
          - 当前已验证：
            - `make ... installcheck REGRESS='fb_summary_payload_locator_merge'`
              `All 1 tests passed.`
            - live `users` COPY case 已完整跑通，不再报 invalid `pfree`
    - [ ] 解释并修掉同一 `users` case 在移空 `meta/summary` 后的
          `ERROR: too many shared backtracking rounds while resolving missing FPI`
    - [ ] 在上述 blocker 清掉后，继续把 `run_flashback_checks -> evaluate_gate`
          的 truth compare 跑完
  - [x] 修复 release gate `run_flashback_checks` 在 preflight
        `memory_limit` 报错后必须人工调参重跑的问题
    - [x] 补 shell 级最小 RED，锁住“首次报
          estimated flashback working set exceeds pg_flashback.memory_limit
          后，会自动以 `pg_flashback.memory_limit='6GB'` 重试一次”
    - [x] 调整 `tests/release_gate/bin/run_flashback_matrix.sh`，
          仅对该特定 preflight 报错启用一次性 6GB 重试
    - [x] 复跑 release gate `--from run_flashback_checks`，
          确认首个 case 不再停在 `memory_limit` preflight
- [x] 删除 release gate 的 frozen WAL fixture 路径，恢复直接使用 live archive
  - [x] 补 shell 级最小 RED，锁住 `run_flashback_matrix.sh` 不再切 frozen `archive_dest`
  - [x] 删除 `capture_truth_snapshots.sh` / `run_flashback_matrix.sh` / `create_flashback_ctas.sql` 中的 frozen WAL 复制与覆盖逻辑
  - [x] 删除 `common.sh` / `selftest.sh` 中 frozen helper 与相关断言
  - [x] 更新 `tests/release_gate/README.md`、`STATUS.md` 到 live-archive-only 新口径
  - [x] 复核：中间阶段运行仍不会误删 live archive

- [x] 修复 release gate 总入口在失败退出后误删归档现场的问题
  - [x] 补 shell 级最小 RED，锁住“跑过 `prepare_instance` 后退出也不得自动清空 archive dir”
  - [x] 调整 `tests/release_gate/bin/run_release_gate.sh` 的 cleanup 口径
  - [x] 更新 release gate 文档与状态文档

- [x] 修复 release gate 初始化阶段未在新建 `alldb` 后安装 `pg_flashback` 扩展的问题
  - [x] 补 shell 级最小 RED，锁住 `prepare_empty_instance.sh` 在 `createdb` 后会执行 `CREATE EXTENSION`
  - [x] 调整 release gate 初始化脚本
  - [x] 更新 release gate 文档与状态文档

- [x] 修复 deep/full 脚本自动清理 live archive 导致缺 checkpoint/FPI 的问题
  - [x] 补 shell 级最小 RED，锁住 `full` 模式 round cleanup 不得清空 live archive
  - [x] 调整 `tests/deep/bin/common.sh` / `run_all_deep_tests.sh` 的 cleanup 口径
  - [x] 更新 `tests/deep/README.md` 与状态文档到新语义

- [x] 修复 `count(*) FROM pg_flashback(...)` 的错误计数现场
  - [x] 补最小 RED：
    - [x] 为 `fb_custom_scan` 增加“target 后含 `INSERT + DELETE`”的计数回归
    - [x] 断言与非快路径 `SELECT *` 结果集计数一致
  - [x] 用 live case 复核：
    - [x] `scenario_oa_50t_50000r.documents`
          @ `2026-04-04 23:20:13 / 23:30:13 / 23:39:13 / 23:40:13`
    - [x] `scenario_oa_50t_50000r.approval_comments`
          @ `2026-04-04 23:00:13 / 23:20:13 / 23:40:13`
  - [x] 暂时禁用 `FbCountAggScan` / `count_only_fast_path`，让 `count(*)` 回退到正确的常规 flashback 执行链
  - [x] 回归验证：
    - [x] `fb_custom_scan`
    - [x] 相关 live case 的 `count(*)` 与 `SELECT *` 计数一致
- [ ] 重设计 `count(*) FROM pg_flashback(...)` 的 count-only 优化
  - [ ] 继续定位 `FbCountAggScan` / `FB_WAL_BUILD_COUNT_ONLY` /
        `target_{insert,delete,update}_count` 的真实 root cause
  - [ ] 在 summary 命中 / locator 命中 / metadata fallback 三类路径下补齐 dedicated 回归
  - [ ] 重新开放 `FbCountAggScan` 与 `count_only_fast_path`
  - [ ] 复核恢复后的 correctness 与性能收益

- [x] 收敛 `documents @ '2026-04-04 23:40:13'` 的 `4/9 replay discover` 与通用 WAL 物化浪费
  - [x] 已完成 live `perf/gdb` 定位，确认热点不在 anchor lookup
  - [x] 已新增 ADR，拍板 reusable WAL record materializer 方向
  - [x] locator-only cursor 连续读取不再 per-record 重建 reader
  - [x] deferred payload 连续物化不再 per-record 重建 reader
  - [x] locator-only 路径重新保留 discover shortcut 所需预计算缺页信息
  - [x] 为 WAL 物化层新增通用 reusable materializer：
    - [x] 复用 `XLogReaderState`
    - [x] 复用当前打开 segment
    - [x] 复用 archive/source 解析结果
  - [x] 将 locator-only cursor / deferred payload / replay fallback 统一切到该层
  - [x] 区分顺扫窗口与稀疏按 LSN 物化的 open hint，避免 locator path 重复 `fadvise`
  - [x] 为 `4/9` 补充观测：
    - [x] `record_materializer_resets`
    - [x] `record_materializer_reuses`
    - [x] `locator_stub_materializations`
  - [x] query-local WAL bgworker 先按实例 worker budget 限流，避免反复 register/kill 不可启动 worker
  - [x] 用 live case 复核：
    - [x] `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
          总时长 `1.39s (< 50s)`

- [x] 修复失败查询后的 `runtime/` spill 残留
  - [x] 先补最小 RED，锁住 `FbWalIndexScan` 报错后不残留 `fbspill-*`
  - [x] 给失败路径补当前 backend runtime 兜底清理，确保 abort 后不残留当前 owner 产物
  - [x] 复跑 `fb_runtime_cleanup`，并用失败场景复核当前 backend 不再残留 runtime 产物

- [ ] 将 summary payload locator 从“查询时现拼现排”升级成“build/cache 期稳定 slice”
  - [ ] 为 payload locator 新增一份架构决策文档，明确 stable slice / query cache / segment 去重契约
  - [ ] 更新 `STATUS.md` / `docs/architecture/overview.md` / spec / plan 到当前口径
  - [ ] 先补最小 RED，锁住新的 payload locator 调试观测：
    - [ ] `summary_payload_locator_segments_read`
    - [ ] `summary_payload_locator_public_builds`
  - [ ] 在 summary build 阶段按 relation 对 payload locators 做排序与去重
  - [ ] 在 query cache 阶段缓存 relation-scoped public locator slice，禁止每次 lookup 重新 `qsort`
  - [ ] 在 payload locator plan 阶段按 segment 去重，避免碎片 `payload_base_windows` 重复驱动 lookup
  - [ ] 用 live case 复核：
    - [ ] `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
          不再卡在 `3/9` 的 payload locator 规划

- [ ] 继续压 `documents @ '2026-04-04 23:40:13'` 的 payload materialize / spool 体积
  - [ ] 对 release gate 当前 `70% -> 100% payload` 补精确归因：
    - [ ] locator-only stub
    - [ ] locator serial materialize
    - [ ] fallback windows
    - [ ] replay 必需 image/body

- [ ] 修完 release gate `scenario_oa_50t_50000r.documents @ 2026-04-07 04:41:28.555065+00`
      的 replay warm blocker，并继续追到新的首个现场瓶颈
  - [x] 补最小 RED：
    - [x] `fb_replay_heap_update_block_id_contract_debug()`
          锁住 cross-block update 必须按 WAL `block_id`
          识别 new block / old block
  - [x] 修复 `src/fb_replay.c`：
    - [x] `fb_replay_heap_update()` 改为优先按
          `block_id=0/1` 认 new/old block，
          不再信任过滤后的 `record.blocks[]` 数组位置
  - [ ] 继续 live 复核：
    - [ ] `count(*)` / `./run_release_gate.sh --from run_flashback_checks`
          确认不再复现
          `failed to locate tuple for heap delete redo`
    - [ ] 若现场已前移，记录新的首个 blocker 并继续修
  - [x] 已确认 locator 规划热点已移除，当前 `3/9` 新主耗时不再是 `pg_qsort`
  - [x] 先把 summary payload locator 访问从“逐条 `XLogBeginRead`”改成批量顺扫
    - [x] 为 payload locator 访问 batching 新增 ADR / 架构登记
    - [x] 先补最小 RED，锁住 locator batching 观测：
      - [x] `payload_windows < payload_refs`
    - [x] 为 locator 模式增加精确 `record_start_lsn` 过滤，禁止批量顺扫放大 payload 结果集
    - [x] 为高密度 locator case 构建 covered-segment-run 级 visit windows
    - [x] 用 live case 复核：
      - [x] `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
            的 `3/9` 明确压到 `< 20s`
  - [x] 为 summary 已覆盖且并行开启的 locator-first 路径新增
    `locator-only payload stub` fast path
    - [x] `3/9` 不再为 locator-first 全量物化 payload body
    - [x] record spool 先只落 `record_start_lsn` stub
    - [x] cursor / replay 侧按需按 LSN 回填真实 record
    - [x] live case 复核：
      - [x] `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
            `3/9` 再次稳定压到 `< 20s`
            (`55% xact-status ~15-16s`, `100% payload ~1s`)
  - [ ] 收敛 `locator-only payload stub` 与 `precomputed_missing_blocks`
    的交互，恢复 `fb_replay` 的 discover-round shortcut 契约
    - [ ] 当前 `PGHOST=/tmp PGPORT=5832 make installcheck REGRESS='fb_recordref fb_replay'`
          中 `fb_recordref` 通过，但 `fb_replay` 仍有
          `skips_discover_rounds = f` 的差异
  - [ ] 先补最小 RED，锁住 `apply_image=false` 的 block image 不再进入 record spool
  - [ ] 让 payload capture 对“不可应用 image”只保留 replay 必需元数据，不再写入 `BLCKSZ` image
  - [ ] 复跑回归与 live case，观察：
    - [x] `3/9 payload` 已显著低于此前约 `178s`
    - [ ] preflight `estimated working set` 继续低于历史约 `8.45GB`

- [x] 将 summary 推进到 payload locator-first，而不是只停留在 relation spans
  - [x] 保持“一段 WAL 一个 summary sidecar”模型不变
  - [x] 新增 relation-scoped payload locator section
  - [x] 查询期 payload 优先按 locator 精确读取 record
  - [x] locator 缺失 / recent tail 未覆盖时继续安全回退现有 window 路径
  - [x] 为 locator section 控制 build 成本与文件体积
  - [x] 如必要，引入轻量压缩 / delta 编码而不改变 summary 模型
  - [x] 为 `fb_recordref_debug()` 增加 summary payload locator counters
  - [x] 用 `approval_comments @ '2026-04-04 23:40:13'` 复核 `3/9 payload`
        不再成为主瓶颈

- [x] 将 `open_source/pg_flashback` 全部 Markdown 改为统一中英双语文档
  - [x] 所有 Markdown 顶部增加 `中文` / `English` 跳转按钮
  - [x] README 改写为当前开源版本实际可用的用户手册
  - [x] `docs/README.md` / `tests/README.md` 与架构文档统一模板
- [ ] 建立 `PG14-18` 发布前功能/性能阻断 gate
  - [x] 新增 `tests/release_gate/` 独立目录与统一入口脚本
  - [x] 固化空实例清理与 `alldb` 重建流程
  - [x] 对接 `/root/alldbsimulator`，构造 `50 x 100MB` 表
  - [x] 固化 `1h` DML 压测流程与随机 seed 记录
  - [x] 固定目标表扩容到 `5GB`
  - [x] 固化五个随机时间点 truth snapshot 采集
  - [x] 覆盖随机闪回、单独 `insert/update/delete`、`10000` 行批量 `insert/update/delete`、混合 DML、`COPY TO`、`CTAS`
  - [x] 固化标准化 CSV 导出、`row_count/hash/diff` 正确性判定
  - [x] 新增 `PG14-18` golden baseline 文件结构
  - [x] 实现“相对比例 + 绝对增量”双阈值性能阻断
  - [x] 生成独立 Markdown 报告
  - [x] 固化 `/walstorage/{14,15,16,17,18}waldata` 测试前后清理
  - [x] 为总入口补齐阶段控制：
    - [x] `--list-stages`
    - [x] `--from <stage>`
    - [x] `--to <stage>`
    - [x] `--only <stage>`
    - [x] 阶段名改成用户可直接理解的动作语义
  - [x] 将 release gate README 改写为完整操作手册
    - [x] 说明每个阶段的作用、输入产物、输出产物
    - [x] 明确 `1h` DML 压测真实执行规则
    - [x] 记录默认总限速已调整为 `2000 ops/s`
  - [x] 按最新发布口径收敛编排与运维细节
    - [x] 将 `grow_target_table` 前移到 `1h` DML 压测之前
    - [x] 明确扩容阶段不采随机 truth snapshot
    - [x] release gate 统一日志输出补齐时间戳
    - [x] 修复从中间阶段启动时 `cleanup` 误删已有 WAL
- [ ] `full-output pg_flashback(...)` 快路径加速
  - [ ] 保持用户 SQL 形态继续是：
    - `SELECT * FROM pg_flashback(...)`
    - `CREATE TABLE AS SELECT * FROM pg_flashback(...)`
    - `COPY (SELECT * FROM pg_flashback(...)) TO ...`
  - [x] 在 planner/executor 中识别 simple full-output 场景
  - [ ] 不替换 PostgreSQL 原生 `CTAS` receiver
  - [x] 将优化重点转到：
    - `apply` 上游 tuple 产出/搬运成本
    - full-table materialization / export 专用 fast path
  - [x] 为 simple `SELECT` / simple `CTAS` 增加 explain/debug 口径：
    - `Flashback Full Output Fast Path`
    - `Flashback Output Dispatch`
  - [x] simple full-row output 命中时绕过 `ExecScan(...)`，直接走 `fb_flashback_apply_next()`
  - [ ] 将 `COPY (SELECT * FROM pg_flashback(...)) TO ...` 纳入同一套自动化观测/回归
  - [ ] 补 explain/性能回归，比较：
    - 普通查询
    - `CTAS`
    - `COPY (query) TO`

- [x] 为纯 `count(*) FROM pg_flashback(...)` 增加聚合下推快路径
  - [x] planner `UPPERREL_GROUP_AGG` 识别纯 `count(*)` 并改走 `FbCountAggScan`
  - [x] `FbCountAggScan` 固定走 `FB_WAL_BUILD_COUNT_ONLY`
  - [x] count-only 不再捕获 payload spool / reverse ops / apply
  - [x] metadata 阶段按 xid 累加主表 `insert/delete/update` 计数，`xact-status` 后直接汇总
  - [x] live case 复核：
    - [x] `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
          `3/9` 累计约 `19.6s`，达到 `< 20s`
  - [x] 2026-04-06 已临时关闭该优化：
    - [x] summary 覆盖 live case 下存在错误计数
    - [x] 当前先以 correctness 优先，待 count-only 链路修正后再重新开放

- [ ] 破坏性删除 `pg_flashback_to(regclass, text)`
  - [ ] 从公开安装面移除
  - [ ] 删除 `fb_export` 原表回退实现与头文件
  - [ ] 删除 `fb_flashback_to` 回归
  - [ ] 改写 `fb_user_surface` 为断言旧入口不存在
  - [ ] 清理 `PROJECT.md` / `README.md` / 架构文档 / ADR / 设计文档中的现行产品描述

- [ ] 将“WAL 回放失败导致闪回失败必须追根因并彻底修复”固化为当前主线执行约束
  - 不以表层报错变化、降级绕过、缺页判定止血或错误文案调整视为完成
  - 本次 `scenario_oa_12t_50000r.documents @ '2026-04-01 23:15:13'`
    必须继续修到 `84/AE079278 / blk=216125 / failed to replay heap insert` 的真实根因为止
- [x] 将“`3/9 build record index` 性能问题必须继续追根因并修复”固化为当前主线执行约束
  - 不把表层 NOTICE 子相位直接当成最终结论
  - 必须继续用 debug counter、`pg_stat_activity`、`gdb/perf` 等证据确认真实热点归属
  - 不接受停在“summary 没起作用”或“看起来卡在 metadata/xact-status/payload”这类表层判断
- [x] 修复 `scenario_oa_50t_50000r.documents @ '2026-04-08 13:33:00.700288+00'`
      的 `summary_xid_fallback=110`
  - 根因已确认：
    - 下午 `summary_xid_fallback=21` 是 stale summary identity，属于 phantom xid
    - 本次不是 phantom xid；样本 xid（如 `16218039`）是真实 committed xid
    - 真正问题是命中 segment 的旧/无效 summary sidecar 已存在，但 query path 没有在查询时按需重建
  - 当前修复已落地：
    - `src/fb_summary.c`
      的 `fb_summary_cache_get_or_load()` 对 invalid/missing candidate
      增加 query-side rebuild + reload
    - `FB_SUMMARY_VERSION` 已前滚到 `11`，旧 `v10` sidecar 当前会统一失效
  - 当前验证：
    - `pg_temp.fb_summary_xid_resolution_debug(...)` 返回：
      - `summary_hits=23352`
      - `summary_exact_hits=0`
      - `unresolved_touched=0`
      - `unresolved_unsafe=0`
      - `fallback_windows=0`
    - live query `count(*) from pg_flashback(...)` 当前完整返回
      且 `3/9 55% xact-status` 已降到 `+301.502 ms`
- [x] 修复验证期引入的 `fb_wal_fill_xact_statuses_serial()` cleanup crash
  - 根因已确认：
    - `FbWalSerialXactVisitorState state` 旧代码在多个 `goto cleanup` 之前未初始化
    - 当 unresolved xid 在 summary 阶段已全部清空时，cleanup 会对未初始化的
      `state.assigned_xids` 执行 `hash_destroy()`，导致 backend `SIGSEGV`
  - 当前修复已落地：
    - `src/fb_wal.c`
      将 `MemSet(&state, 0, sizeof(state))` 前移到函数入口
  - 当前验证：
    - 同一 live query 当前可完整执行到 `[done] total elapsed 35552.115 ms`
      并返回 `count = 1950007`
- [ ] 修复 `apply_image=false` 仍被当成页基线/页覆盖的 replay bug
  - 当前 live 复现场景：
    - `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'`
    - `scenario_oa_12t_50000r.documents @ '2026-03-31 22:50:13'`
    - 统一报 `87/6E00D568 / blk=1084494 / failed to replay heap insert`
  - 补最小 RED：
    - 锁住 `has_image=true && apply_image=false` 不得覆盖已有页状态
    - 锁住此类记录不得被 precomputed missing / anchor resolve 当成 materialized page
  - 修复后复核：
    - 相关 replay 回归
    - live SQL 不再报 `failed to replay heap insert`
- [ ] 继续收敛 `HEAP2_PRUNE / INIT_PAGE / HOT_UPDATE slot reuse` 的页态语义
  - 当前已确认：
    - `INIT_PAGE` 记录需要无条件重置已有 block state，不能只在“未初始化”时 `PageInit()`
    - 部分 `PRUNE` applicable image 会把当前 block 压回过旧页态，和后续 `INSERT/HOT_UPDATE` 的 slot reuse 发生冲突
    - `blk 1084485` 的 final replay 连续页态当前已实锤会走到：
      - `80/D6079340 PRUNE_ON_ACCESS dead=[1,2]`
      - `83/6674F870 PRUNE_ON_ACCESS dead=[3,4,5] unused=[6]`
      - `86/30211CD8 PRUNE_VACUUM_CLEANUP FPW`
      - 然后在 `87/6E031A78 UPDATE old_off=1` 报 `failed to locate tuple for heap update redo`
    - 最新安装态复现已确认：
      - `blk 1084485 @ 86/30211CD8` 当前会进入 preserve gate
      - 但判断结果是 `current_ok=false image_ok=false`
      - 因而这块当前不是“没走 preserve”，而是 preserve 判定本身认为前后页态都不支持未来 `old_off=1`
    - 同一轮 live log 已继续推进到新的 first blocker：
      - `blk 1084494 @ 87/6E098C40 DELETE off=3`
      - 其更早链路已确认为：
        - `83/6675CDF8 FPI_FOR_HINT imgmax=4`
        - `83/6675EC30 PRUNE_ON_ACCESS dead=[1,2,3] unused=[4]`
        - `86/30212248 PRUNE_VACUUM_CLEANUP FPW`
        - `87/6E00D568 INSERT off=4`
        - `87/6E062350 DELETE off=4`
        - `87/6E098C40 DELETE off=3`
    - 本轮已完成：
      - 新增最小 RED `fb_replay_prune_compose_future_constraints_debug()`
      - 已把 `prune lookahead` 修成按 future record 逆向组合前置页态约束
      - `fb_replay` 专项回归已通过
      - 新增最小 RED `fb_replay_prune_lookahead_snapshot_isolation_debug()`
      - 已把 `prune lookahead` entry 存储从浅拷贝修成深拷贝，避免同块更老 record 污染已存下来的 future 约束
      - live case `TOAST blk=17079 / off=42` 已被推过
    - 已验证一个错误方向：
      - 仅把 final pass 中的 `PRUNE/FPI` 改成“已有页态时只推进 LSN、不覆盖页内容”不能修穿 live case
  - 当前最新结论：
    - `87/6E0B1F10 / blk=26273 / off=27` 已确认不是新的 `prune/lookahead` 语义问题
    - 真因是历史 `summary v6` sidecar 截断了 `87/6D` relation span，
      漏掉 `87/6DD491C0` / `87/6DD4B110` / `87/6DD4B230`
  - 当前需要继续做：
    - 第一优先级先围住 cold-run first blocker：
      - `87/17F73AB8 / rel=1663/33398/16395737 / blk=38724 / off=1 / failed to locate tuple for heap delete redo`
    - 解释 `blk 1084485` 上 “连续 `PRUNE/FPI` 后页态全 dead，但后续 WAL 仍要求 `old_off=1` 为正常 tuple” 的真实语义
    - 继续回到真正尚未解决的 batch B / replay 语义现场
    - 复跑同类时间点，确认不再被历史坏 summary sidecar 拖回旧 blocker
- [ ] 修复 summary worker 长生命周期高 RSS 不回落的问题
  - 当前现场：
    - `pg_flashback summary worker 2` `PID 1173478` `RSS ~= 7.8GB`
    - `pg_flashback summary worker 1` `PID 1173479` `RSS ~= 7.4GB`
  - 当前目标：
    - 先确认是 MemoryContext / cache / mmap 持有还是实际泄漏
    - 补最小 RED 或 debug contract，锁住“worker 空闲后内存必须可回落/可复位”
    - 修复后复核 summary service 正常运行且不回归 `installcheck`
- [x] 失效旧 `summary v6` sidecar，避免历史坏 summary 持续污染 query-side payload windows
  - [x] 前滚 `FB_SUMMARY_VERSION` 到 `7`
  - [x] 补回归 `fb_summary_v6_rejected_debug()`
  - [x] 复核 `documents @ '2026-04-01 22:10:13'` 已不再停在 `26273`
- [x] 修复 trailing invalid `pg_wal` tail 误挡 archive/valid suffix 的 resolver 问题
  - [x] 补回归覆盖：
    - trailing invalid `pg_wal` tail 不应阻断 archive suffix
    - archive exact-hit + `pg_wal` 同名 mismatch 时仍必须优先取 archive
  - [x] 复核 live 现场，把假性的 `pg_wal recycled/mismatched` 错误面收敛为真实 retained suffix / 内存类 blocker
- [x] 修复 payload 窗口首条跨 segment 记录头仍会被裁掉的 residual 问题
  - 已新增 RED：
    - `fb_wal_payload_window_contract_debug()`
    - 锁住 `emit_start=89/F0001908` 时必须把 read window 回补到
      `89/EF000000`
  - 已完成修复：
    - payload read window 会为首窗补读前一连续 segment
    - payload emit gate 允许 `EndRecPtr = emit_start` 的紧邻前驱记录进入索引
  - 已复核：
    - `fb_recordref_block_debug(..., 9990)` 重新看到
      `89/EFFFF9D8 DELETE ... FPW`
    - `scenario_oa_12t_50000r.roles @ '2026-04-02 22:10:13'`
      不再报 `missing FPI for block 9990`
    - 默认 `1GB` 下当前返回真实 `memory_limit` 报错
    - `memory_limit = '3GB'` 下返回 `count = 98896`
- [x] 建立仓库内开源镜像目录与白名单同步机制
  - 已固定 `open_source/pg_flashback/` 为长期维护的开源镜像目录
  - 已固定根仓库继续保留内部研发资料
  - 已新增：
    - `open_source/README.md`
    - `open_source/manifest.txt`
    - `scripts/sync_open_source.sh`
  - 已明确开源镜像首版排除：
    - `STATUS.md` / `TODO.md` / `PROJECT.md`
    - `docs/`
    - `tests/`
    - 日志、构建产物、性能采样和其他临时输出

- [ ] 每次对外同步 GitHub 前执行并复核开源镜像刷新
  - 统一执行 `bash scripts/sync_open_source.sh`
  - 复核 `open_source/pg_flashback/` 中未混入内部资料或构建产物
  - 当前复核范围补充包含 `LICENSE`
- [x] 将 `open_source/` 排除出当前仓库 Git 跟踪
  - 根仓库 `.gitignore` 已加入 `open_source/`
  - 开源镜像目录继续作为本地导出目录按需重建
- [x] 固定“开源项目”术语指向 `open_source/`
  - 已在 `AGENTS.md` 登记：
    - “开源项目” 默认指 `open_source/` 目录中的内容
    - “开源版本” / “GitHub 版本” 也按同一口径理解

### Runtime 安全清理

- [x] 恢复“查询结束触发的 `runtime/` 安全 sweep”
- [x] 扫描整个 `DataDir/pg_flashback/runtime`
- [x] 仅删除 owner backend 已失活的 `fbspill-*` / `toast-retired-*`
- [x] 活跃 owner / 命名不匹配 / 状态不确定时直接跳过
- [x] 不恢复 `runtime_retention` 等 retention GUC
- [x] 不触碰 `recovered_wal` / `meta`
- [x] 补回归覆盖：
  - 自动清理本次查询残留
  - 自动清理历史 stale runtime 残留
  - 不误删当前活跃 backend 的 runtime 产物

### Summary 预建服务

- [x] 新增 segment 通用 summary 文件格式，优先服务 query-side prefilter
- [x] 停用 relation-pattern 级 `prefilter-*.meta` sidecar 持续写入
- [x] 新增 `shared_preload_libraries` 下的 summary launcher
- [x] 新增 summary shared queue + 多 worker pool
- [x] launcher 周期扫描 `archive_dest` / `pg_wal` / `recovered_wal`
- [x] 查询 prefilter 改为 summary-first，缺失时回退旧 `mmap + memmem`
- [x] 新增 `meta/summary` 容量上限与 summary 专属自动清理
- [x] 完成 PG18 preload 手工验证
- [x] 将 launcher 调度改成显式“冷热双队列 + recent-first”
- [x] 新增 summary 进度视图，展示 stable candidate 完成度与队列状态
- [x] 补齐 summary/service 专项回归与可观测 debug 接口
- [x] 将 `fb_summary_progress` 收口为 snapshot + hot/cold frontier 口径，避免新段持续产生时进度语义漂移
- [x] 将用户向 summary 进度视图重做为 `pg_flashback_summary_progress`
- [x] 将 queue / worker / cleanup 计数拆到 `pg_flashback_summary_service_debug`
- [x] 将 stable 时间窗 / 近端前沿 / 远端前沿 / 两端首个 gap 作为新的用户主口径
- [x] 新增并固定“用户可见 summary 观测面统一使用 `pg_flashback_` 前缀”的命名规则
- [x] 将 `pg_flashback_summary_progress` 收紧为“当前可见 WAL 连续性 + summary 覆盖”联合口径
  - [x] 将中间 WAL 缺口折算进 `missing_segments`
  - [x] 让 `first_gap_from_newest_*` / `first_gap_from_oldest_*` 能指向真实 WAL 缺口
  - [x] 让 `progress_pct` 对归档删洞后的场景不再虚高
  - [x] 补回归覆盖“删掉 archive 中间段后 progress 先缩 frontiers”的场景
  - [x] 增加“最近一次查询是否发生 summary 降级”口径
  - [x] 在用户视图暴露最近查询的 ready/fallback 状态与时间戳
  - [x] 补回归覆盖“删空 `meta/summary` 后查询回退、重建后恢复 ready”的场景
- [x] 将 summary 服务调度从“recent-first”收敛为“1 个 hot worker + 其余 cold-first”
- [x] 为 cold backlog 增加批量连续 segment claim，减少 shared queue 往返
- [x] 将服务侧“summary 已存在”判定改成快路径，避免反复 `open/read header`
- [x] 为 `pg_flashback_summary_progress` 增加 `estimated_completion_at`
- [x] 用最近 build 速率为 backlog 估算 ETA；速率不可用时返回 `NULL`
- [x] 修复 `pg_flashback_summary_progress` 的“长时间不推进”观感问题
  - [x] 将 cold backlog enqueue/claim 都改成 oldest-first
  - [x] 让 oldest backlog 不再被固定 queue capacity 长期饿住
  - [x] 用 `fb_summary_service_schedule_debug(integer, integer, integer)` 补回归锁住调度语义
- [x] 修复 `estimated_completion_at` 长期为 `NULL` 或返回错误 epoch-like 时间
  - [x] 放宽/补充 ETA sample 口径，不再因为 recent sample 过窄长期返回 `NULL`
  - [x] 改成安全的绝对时间构造，避免出现 `2000-01-01 ...` 这类错误值
  - [x] 补回归覆盖“ETA 为未来时间”

### Summary v3 紧凑 segment 索引

- [x] 将 `summary-*.meta` 从 bloom-only 摘要升级为 versioned 多 section 格式
- [x] 保留 `locator_bloom/reltag_bloom` 作为 segment 级第一层 gate
- [x] 新增 relation dictionary + relation spans section
- [x] 新增 xid outcome section，优先服务 touched xid 状态补齐
- [x] 查询侧命中 segment 后改为 span-driven WAL decode，不再默认整段顺序遍历
- [x] xid status 回填改为“summary outcome 优先，WAL 回扫兜底”
- [x] 补齐 sidecar 缺失 / 损坏 / 覆盖不足时的安全回退回归
- [x] `block-anchor summary v1`
  - [x] 在 `summary-*.meta` 中新增 relation-scoped block anchor section
  - [x] 查询侧优先使用 block anchor summary 解析 missing-block 的最近可用锚点
  - [x] replay backtrack gate 从 `record_index` 收敛到 `anchor_lsn`
  - [x] 保持现有 relation/xid summary 与 WAL fallback 语义
  - [x] 补专项回归与 debug 口径
- [x] `replay discover` 静态 missing-block 预计算
  - [x] 利用 `RecordRef` 顺序静态模拟 block initialized/no-op/missing 状态
  - [x] `missing-block=0` 时跳过整轮 discover/warm
  - [x] 仅对真正缺页基线的 block 再走 anchor resolve + warm
  - [x] 补 debug/回归，并用 live case 复核 `4/9` / `5/9` 阶段收益
- [x] 将 summary builder 的 `touched_xids` / `unsafe_facts` / `block_anchors` 改成边扫边去重，压低 backlog 构建 CPU

### P3 / P4 补齐

- [ ] 将 `fb_decode_insert_debug` 改成基于 `ForwardOp` 的开发期调试出口
- [ ] 增加 reverse-op / row-image 开发期调试出口

### P5 查询执行

- [x] apply 主链改为变化集驱动的小内存流式执行
- [x] 历史结果集改为直接查询型 SRF 返回
- [x] 修复 `PG18` `same-block HOT_UPDATE + FPW-only` 误报 `heap update record missing new tuple data`
- [x] keyed 主键变化与原表回退场景补齐
- [x] 补齐 TOAST relation truncate / storage_change 的用户案例回归
- [x] 增强 `storage_change` 报错诊断，直接显示 main/toast、create/truncate、xid、commit time
- [x] 将 TOAST `SMGR TRUNCATE` 从直接报 `storage_change` 改为“允许 flashback，最终缺 chunk 时再报错”
- [x] 将 standalone `standby_lock` 从误判的 `storage_change` blocker 中移除
- [x] 将 `pg_flashback.memory_limit` 默认值提升到 `1048576KB`，避免已验证可恢复的 live case 被默认 64MB 预算直接拦截
- [x] 修复 replay discover 对“跨页记录缺锚点导致的新页状态落后”处理，避免后续同页记录误报 `failed to replay heap insert`
- [x] 修复 checkpoint sidecar anchor 起扫范围错误裁到 `checkpoint_lsn`，恢复 `redo_lsn..checkpoint_lsn` 区间的必要 FPI/FPW 扫描
- [x] 补齐当前 `REGRESS` 缺失的 `expected/*.out`，恢复全量 installcheck 可执行状态
- [ ] apply / replay / TOAST 交界处继续补更多宽表与极端场景验证

### P5.5 用户接口与来源模型

- [x] 安装脚本改为创建 `pg_flashback(anyelement, text)`
- [x] 删除旧的结果表名调用形态
- [x] 删除旧的 `pg_flashback(text, text, text)` 公开暴露
- [x] 删除公开安装面的 `pg_flashback_rewind(regclass, text)`
- [x] 下线“创建 `table_flashback` 新表”的公开入口与对应回归/文档口径
- [x] 迁移回归与 deep 脚本调用点
- [x] 同步 README / STATUS / TODO / PROJECT / 维护文档 / ADR
- [x] 已加载扩展后，未定义的 `pg_flashback.*` GUC 名称直接报错，避免 typo 被静默接受
- [x] 未显式设置 `pg_flashback.archive_dest` 时，从 PostgreSQL `archive_command` 自动发现本地归档目录
- [x] 支持本地 `pg_probackup archive-push -B ... --instance ...` 归档目录自动识别
- [x] `archive_library` 非空或复杂/远程 `archive_command` 时回退为要求显式配置 `pg_flashback.archive_dest`
- [x] `show_progress` 开启时为每条输出追加增量耗时，并在结束时输出总耗时
- [ ] `fb_export_undo` 对外安装与实现决策
- [x] 取消 `DataDir/pg_flashback/{runtime,recovered_wal,meta}` 的自动删除机制：
  - 删除 `runtime_retention/recovered_wal_retention/meta_retention`
  - 删除 runtime 初始化与查询结束时的 cleanup
  - 保留目录创建与产物写入，不再对现有文件做保洁
- [x] 修复“archive 已有真实 segment 时，recycled / mismatched pg_wal 仍被提前 convert 并白白写入 recovered_wal”的 resolver 问题
  - 新增回归 `fb_recovered_wal_policy`
- [x] 继续收紧 `recovered_wal` 语义：
  - 删除 “archive exact-hit 复制到 recovered_wal” 路径
  - 只允许 `pg_wal` 被覆盖/错配且 archive 不可用时 materialize 到 `recovered_wal`
  - 回归覆盖 archive-hit 不落盘、archive-missing mismatch 才落盘
  - 已验证 `fb_recovered_wal_policy`、`fb_wal_source_policy`、`fb_wal_prefix_suffix`、`fb_wal_error_surface`
- [ ] 修复 resolver 过早 convert 非保留 suffix 的 mismatch `pg_wal`，避免查询根本不需要的段也落到 `recovered_wal`
  - 当前 root cause 已收敛为：
    - 在 retained suffix 确定之前，resolver 会遍历并 convert 所有 mismatch `pg_wal` candidate
    - 即使这些 candidate 最终会被 prefix-gap 裁掉，也已经把真实 segname 写进 `recovered_wal`
  - 需要补回归覆盖：
    - archive 提供当前查询所需连续 suffix
    - `pg_wal` 只有更老且最终不会保留的 mismatch 段
    - prepare 后 `recovered_wal` 仍保持空

### P5.6 内存与效率

- [x] apply 不再按当前表大小线性占用内存
- [x] 用 `CustomScan` 接管 `FROM pg_flashback(...)`，消除 `FunctionScan -> ExecMakeTableFunctionResult -> pgsql_tmp`
- [x] 将 `CustomScan` 输出改为 slot-native，消除 `Datum -> ExecStoreHeapTupleDatum()` 导致的 `ExecutorState` 线性膨胀
- [x] 将单个 `FbFlashbackScan` 拆成多节点 `CustomScan` 算子树，让 `EXPLAIN ANALYZE` 可见：
  - `FbWalIndexScan`
  - `FbReplayDiscoverScan`
  - `FbReplayWarmScan`
  - `FbReplayFinalScan`
  - `FbReverseSourceScan`
  - `FbApplyScan`
- [x] 实现 keyed fast path：
  - `WHERE 主键 = const`
  - `WHERE 主键 IN (const, ...)`
  - `ORDER BY 主键/唯一键 ... LIMIT N`
  - 第一阶段只支持单列稳定主键/唯一键，其余场景自动回退
- [x] 将 keyed fast path 扩展到单列稳定键 range 谓词：
  - `BETWEEN`
  - `< <= > >=`
  - 两侧 bound 的开闭区间组合
  - 与 `ORDER BY key` / `LIMIT` 的可安全组合
- [x] 去掉 `FbApplyScan` 末尾 `ExecCopySlot/tts_virtual_materialize` 输出拷贝链
  - [ ] `3/9` 架构收敛 follow-up：
  - [x] 将 `3/9 build record index` 的 `NOTICE` 细分为固定子相位：
    - `prefilter`
    - `summary-span`
    - `metadata`
    - `xact-status`
    - `payload`
  - [x] 为 `fb_recordref_debug()` 补 payload work counter：
    - payload windows
    - payload covered segments
    - payload scanned/decoded records
    - payload kept/materialized records
  - [x] 在保持 `9` 段进度不变的前提下，消除“`3/9 100%` 后仍长时间空白”的误导性观测
  - [x] 先做一轮保守版 payload window narrowing：
    - 仅合并同一 covered segment slice 内的碎片窗口
    - 不跨 segment 扩窗
    - live case 已验证 `3/9 payload` 从约 `97s` 降到约 `16s`
  - [ ] 继续收窄 build-index 尾段 payload work，减少对 replay/final 无贡献的 decode/materialize
    - [x] 将 payload 改成 `summary-span` 驱动的 sparse candidate-stream 读路径
      - 当前稳定口径是“merge-normalized sparse spans”，不是直接吃 raw spans
      - 已补 `payload_scan_mode` debug 观测
      - live case 已验证 `payload_scanned_records` 从约 `1960万` 降到约 `180万`
    - [ ] 缺 summary 的 segment 保持现有 fallback 全扫
    - [x] 为新 payload 读路径补 debug 可观测，避免后续回退到粗窗口顺扫时无从察觉
    - [ ] 继续压 sparse payload 的 seek 开销：
      - 当前 live 仍有 `payload_windows=259685`
      - 下一个收益点不再是 decode 量，而是 candidate window 数与跳读成本
      - [x] 先做一轮不改 payload 语义的 reader-reuse 收口：
        - 同一 sparse segment slice 内避免逐 window 重置 reader
        - 尽量复用 open file / reader 状态
        - 已补 `payload_sparse_reader_resets/reuses`
        - 串行 sparse path 与 payload worker path 都已接上
      - [ ] 为 reader-reuse 补稳定回归：
        - 当前 synthetic sparse fixture 还不能稳定命中 `payload_scan_mode=sparse`
        - 本轮先保留 `payload_sparse_reader_resets/reuses` 已进入 debug 输出的回归
    - [ ] 继续压 `xact-status` 的 summary 成本：
      - [x] 先去掉 query 期对每个命中 segment 的 xid outcome 整段复制
        - 当前改成复用 query-local summary cache 内的 public slice
        - live case 上 `xact-status` 已从约 `19s` 档位压到约 `16-17s`
      - [x] 把 `xact-status` 从 all-or-nothing 回退改成 unresolved-only fallback
        - summary 已命中的 xid status 直接保留，不再因少量 unresolved 整体失效
        - fallback 只允许扫描 unresolved `touched/unsafe xid`
        - 需要补回归，锁住不会重复累计 `target_commit/abort`
      - [x] 为 `xact-status` fallback 增加专用 coalesced windows
        - 不再直接复用高碎片 relation/span windows
        - 至少补 debug 输出：
          - `xact_fallback_windows`
          - `xact_fallback_covered_segments`
        - live case 验收：
          - `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
            的 `3/9` 压到 `< 20s`
      - [ ] 若继续往下压，需要新的 per-segment xid presence/index
        - 当前仅靠 relation-scoped touched xid 或事务语义剪枝都不安全
        - 已验证“按 before-target/aborted xid 直接裁 payload”会破坏 replay 正确性，不能走这条捷径
      - [x] 修复碎 summary-span 场景下的 payload scan-mode 误判：
        - 当前 live 现场：
          - `scenario_oa_50t_50000r.approval_comments @ '2026-04-04 23:40:13'`
          - `summary_span_windows=38666`
          - `payload_sparse_count=32184`
          - `payload_windowed_count=383`
          - 但 query-side 仍选 `payload_scan_mode=sparse`
        - 当前 root cause：
          - summary spans 已齐，不是 fallback 全扫
          - 真正问题是 scan-mode heuristic 没有识别“windowed 已足够收敛、sparse 只会放大同 slice 重复 decode”
        - 最小 RED：
          - 补稳定回归，锁住碎 summary-span synthetic case 选择 `windowed`
          - 同时锁住相关 debug 输出仍保留 `payload_windows/payload_scan_mode`
        - 修复目标：
          - 不改 replay 语义
          - 不重新扩大 payload 覆盖面
          - 仅避免这类 case 误选 `sparse`
        - 已完成：
          - query-side heuristic 已接入 `payload_covered_segments`
          - 新增回归 `fb_payload_scan_mode`
          - live spot-check 已确认该现场切回 `payload_scan_mode=windowed`
      - [x] `fb_recordref_debug()` 增加 `anchor_redo`
      - [ ] 将 `fb_recordref_block_debug()` 收口成正式调试出口或等价观测，而不是仅手工 `CREATE FUNCTION`
    - [ ] 用 `scenario_oa_12t_50000r.documents @ '2026-04-01 01:40:13'` 持续复测，直到 `3/9` 不再是主要慢点
    - [x] 在 sparse payload 冷态优化完成后，复现并修复：
      - `scenario_oa_12t_50000r.documents @ '2026-04-01 23:15:13'`
      - 表层 `missing FPI for block 216136` 已继续追到真正 root cause
      - 真正问题不是 replay 原语本身，而是旧版本 summary sidecar 的 partial relation spans 仍被当前查询信任
      - 当前正式修复为：
        - 前滚 `FB_SUMMARY_VERSION`
        - 让旧 summary 自动失效并回退到安全 WAL 扫描 / 新版 summary 重建
      - 原始现场当前已返回：
        - `count = 4356675`
    - [ ] 为“旧版本/旧语义 summary sidecar 必须自动失效回退”补回归
  - [ ] 将 `xid fill` 改成只读取命中 query window 的 summary segment，不再按 `resolved_segment_count` 全量扫 `summary-*.meta`
  - [ ] 为查询期 summary section 增加 backend-local cache，去掉同一查询内的重复 `open/read/close`
  - [ ] 增加 relation-scoped `touched xids` section，让 metadata 主链不再为 xid 收集回扫整段 WAL
  - [ ] 在保持 `meta/summary` 低存储原则下新增紧凑 `unsafe facts` section
  - [ ] 将 metadata 主链改成 summary-first，仅对 uncovered window 回退 WAL 扫描
  - [ ] 保留原有 metadata/xact WAL 路径，确保 meta 未及时生成时仍可正确回退
  - [ ] 删除现有 `meta/summary` 并重建后，复跑回归与 live case 验证真实收益
  - [ ] 在 block-anchor summary v1 稳定后，再评估是否继续扩展到 block span-driven replay 窄化
  - [ ] 继续减少进入 `XLogDecodeNextRecord` 但最终无 payload 的 record work
  - [ ] 继续合并 touched-xid / xid-status 的重复 hash bookkeeping
  - [ ] 继续复用 prefilter / visit-window / materialize-window 的重复 segment-window 计算
- [x] 从本机会话日志恢复 bounded spill Stage A 代码基线（`fb_spool` / `fb_spill` / `FbReverseOpSource`）
- [x] 继续从本机会话日志恢复 `2026-03-26 21:10` 后半段到 `2026-03-27` 的 spill follow-up / `fb_wal` sidecar / SRF 主链
- [x] 统一内存超限报错增加 `pg_flashback.memory_limit` 调参提示与可读单位
- [x] `pg_flashback.memory_limit` 支持用户直接传 `kB/MB/GB`
- [x] 修复 `pg_flashback.memory_limit='8GB'` 被错误拒绝的问题
- [x] 将用户侧参数名改为 `pg_flashback.memory_limit`，并把最大允许值收紧到 `32GB`
- [x] 新增 `pg_flashback.spill_mode`，并在 replay 前按 `memory_limit` 做 preflight 内存/磁盘选择
- [x] 将 flashback 主链并行控制统一收口为 `pg_flashback.parallel_workers`
  - 删除旧的 `pg_flashback.parallel_segment_scan`
  - 当前不接管 `pg_flashback.export_parallel_workers`
- [x] 将 `pg_flashback.parallel_workers` 默认值从 `0` 调整为 `8`
- [x] 将 resolver / sidecar 接到统一 flashback 并行参数
- [x] 将 WAL prefilter 接到统一 flashback 并行参数
- [x] 将 `parallel_workers=0` 改为“关闭并行但保留串行 prefilter”
- [x] 将 WAL payload / materialize 改造成不改变语义的并行阶段
  - 当前正式主路径已包含：
    - 进程级 payload worker + shared segment snapshot
    - raw spool merge + anchor rebuild
    - 连续大 payload window 细分
    - overlap read + logical emit boundary 的 correctness 修复
  - 已补自动化回归 `fb_wal_parallel_payload`
  - live case 已确认 `FbWalIndexScan` 与总时长均有明显收益
- [x] 将“缺 checkpoint anchor”判定前移
  - 缺 `target_ts` 前 checkpoint 时，不再扫完整个 `3/9 build record index` 后才报错
  - 在 metadata / payload 主扫描前先完成 anchor 可达性判定
  - 补回归覆盖“仅保留近端连续 WAL，但 target 早于最早 checkpoint”的场景
- [x] 收敛 WAL spool 初始化报错
  - 不再向用户直接暴露 `fb WAL record spool session is not initialized`
  - 改成可读的用户错误文案，并补回归覆盖
- [x] 完成 `failed to replay heap insert` 现场复核与止血
  - `documents @ '2026-04-01 08:10:13'` 在最新 build + restart 后已不再复现该错误
  - record spool 已确认包含 `off21..28` 相关完整 WAL，说明不是该段记录漏收
  - 同一条 SQL 在 `memory_limit='8GB'` 下当前返回 `count = 4356191`
  - `memory_limit='1GB'` 下当前返回真实 preflight 内存报错，而不是 replay 错误
  - 内核调试补充确认：
    - raw summary spans 对该 live case 确实乱序，当前 correctness 依赖后续全局 merge
    - 去掉 summary/payload merge + `emit_floor` 后，live case 会重新退化为 replay correctness 错误
    - 因此原 `failed to replay heap insert` 的本质根因已收敛为“payload window 非单调/重叠导致页状态落后”，不是 heap insert redo 原语本身
- [ ] root-cause 并重新启用 WAL payload dynamic bgworker/DSM 路径
  - 当前 live case 曾触发 postmaster 级 `stuck spinlock` / shared-memory instability
  - 现阶段已硬关闭 `fb_wal_materialize_payload_parallel()`，仅保留串行 payload materialize
  - 重新启用前至少需要：
    - 复现并定位具体 shared-memory / bgworker 生命周期问题
    - 保证 `fb_wal_parallel_payload` 回到真实并行 worker 口径
    - 复跑 `documents @ 2026-03-29 14:10:13`，确认日志无 `PANIC` / recovery
  - 本轮补充：
    - [ ] 取消 `fb_wal_materialize_payload_parallel()` 顶部硬返回
    - [ ] `fb_wal_parallel_payload` 改回断言 `payload_parallel_workers > 0`
    - [ ] 用用户提供的 `documents @ '2026-03-31 22:40:13'` SQL 连跑三次
- [ ] 将 WAL metadata scan 改造成不改变语义且稳定优于串行基线的并行阶段
  - 当前 metadata 两段式并行 prototype 已做过，并补过 `fb_recordref` safe/unsafe 合同回归
  - 但 live case 上未打赢串行 metadata 基线，当前保持关闭
  - 下一轮目标是减少 worker 启停 / 额外 WAL pass 开销后再重新打开
- [ ] 将 replay / reverse-source 改造成不改变语义的并行阶段
- [ ] 将 apply 改造成对不同 SQL 形态普遍受益的通用并行阶段
  - 当前已验证过一版 keyed query-side apply prototype：
    - shared reverse-source
    - parallel table scan
    - per-worker tuple spool
    - leader merge seen-keys + residual
  - 当前已补：
    - query 路径 shareable reverse-source 接线
    - typed seen-key merge
    - `fb_apply_parallel` correctness 回归
    - worker 数安全收口与失败回退
    - 大表保护阈值，避免未成熟 apply 并行拖慢 live case
    - 串行 apply 热路径的 direct single-typed probe / bloom 风格 negative filter / CustomScan raw slot return
  - 但 live case 仍明显受 `tuple spool + leader 回读` 传输模型拖累，当前保持关闭，后续需要改成更低传输开销的方案
  - 当前第一阶段聚焦：
    - keyed + 单列 typed key
    - 继续把 `documents(id)` 这类 keyed live case 打到稳定优于上一轮串行 apply 基线
    - 优先替换 worker -> leader tuple 文件 spool 回读为基于 `shm_mq` 的流式传输
    - seen-key 先继续保留小 spool，等 tuple 传输打赢后再决定是否继续共享化
- [ ] WAL 索引 / replay 主链继续向 bounded spill 演进
- [ ] 将 live case 当前依赖的高默认内存继续回收回真正的 bounded spill / eviction 路径
- [ ] deep full 的 flashback 并行 `parallel_workers=0/N` 端到端验证继续推进
- [ ] 按恢复后的代码重新补跑 installcheck / deep / 冷缓存场景验证
  - [x] `installcheck` 已重跑恢复为 `All 36 tests passed.`
  - [ ] 补跑 `tests/deep/`
  - [ ] 补跑冷缓存 / 更长时间窗场景

## 恢复记录

- [x] 已将当前可编译恢复点同步回当前工作区
- [x] 已记录稳定临时树 `/tmp/pgfb_stageA_clean`
- [x] 已将恢复记录继续同步到设计 / ADR / 架构文档

### P5.7 版本兼容与文档

- [x] 将 `README.md` 改写为正式客户使用手册
- [x] 抽取 `fb_compat` 并去掉当前 `PG18` 专属构建假设
- [x] 收敛到源码/构建目标 `PG10-18`
- [x] 跑通本机 `PG12-18` 编译矩阵
- [x] 在文档中明确 `PG10/11` 为“待补环境复验”

### deep / 正确性

- [ ] 收敛 batch B / residual `missing FPI`
- [ ] 持续补齐 TOAST full 与大时间窗验证

## 完成前检查

- [x] `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
- [x] `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`
- [ ] 与本轮改动相关的 deep 验证按需补跑
