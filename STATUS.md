# STATUS.md

## 当前代码口径（2026-03-29）

- 当前版本支持目标已扩展为：
  - 源码/构建目标 `PG14-18`
  - 本机实际验证矩阵 `PG14-18`
- 当前安装脚本只对外安装：
  - `fb_version()`
  - `fb_check_relation(regclass)`
  - `pg_flashback(anyelement, text)`
  - `pg_flashback_dml_profile(anyelement, text)`
  - `pg_flashback_dml_profile_detail(anyelement, text)`
  - `pg_flashback_debug_unresolv_xid(regclass, timestamptz)`
  - `pg_flashback_summary_progress` 视图
  - `fb_summary_path_debug(text)` / `fb_summary_candidate_debug(text)` /
    `fb_summary_build_candidate_debug(text)` 已确认仅属共享库内历史残留 helper，
    不属于公开安装面，也不再作为保留接口
- 当前用户调用形态固定为：
  - `SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);`
- 当前全表闪回落地策略固定为：
  - 库内另立新表：
    - `CREATE TABLE new_table AS SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);`
  - 对外导出：
    - `COPY (SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text)) TO ...`
  - `COPY` 不作为“直接创建闪回结果表”的主路径
- 当前结果模型固定为：
  - `pg_flashback()`：
    - 不创建结果表
    - 不返回结果表名
    - 默认仍是直接查询型 SRF
    - 扩展内部统一只走 `ValuePerCall`
    - `FROM pg_flashback(...)` 已改由扩展自带多节点 `CustomScan` 算子树接管，不再走 PostgreSQL 默认 `FunctionScan -> tuplestore`
- 当前旧入口与旧中间层已删除：
  - `pg_flashback_to(regclass, text)`
  - `pg_flashback(text, text, text)`
  - `pg_flashback_rewind(regclass, text)`
  - `fb_parallel`
  - `parallel_apply_workers`
  - 公开安装面的 `fb_flashback_materialize(...)`
- 当前主链已经收敛为：
  - `checkpoint + RecordRef + block redo + ForwardOp + ReverseOpSource + streaming apply`
- 当前 apply 已切到小内存口径：
  - keyed 只跟踪变化 key
  - bag 只跟踪变化 row identity
  - 不再按当前整表大小构造 apply 工作集
- 当前 spill / sidecar 主链已进入恢复后的继续收敛状态：
  - `fb_spool`
  - `sql/fb_spill.sql`
  - `sql/fb_wal_sidecar.sql`
  - `fb_wal` recent tail inline / sidecar 诊断口径
  - `pg_flashback()` 仅保留 streaming SRF 发射路径
- 当前运行时与来源解析已落地：
  - `archive_dest`
  - `archive_dir` 兼容回退
  - `debug_pg_wal_dir`
  - `memory_limit`
  - `spill_mode`
  - `parallel_workers`
  - `show_progress`
  - 未显式设置 `archive_dest` 时，可按 PostgreSQL 内核 `archive_command` 自动推断本地归档目录
  - 自动初始化 `DataDir/pg_flashback/{runtime,recovered_wal,meta}`
  - `recovered_wal/meta` 仍不做通用自动删除或保留期淘汰
  - `runtime/` 当前将恢复“查询结束触发的安全 sweep”：
    - 扫描整个 `runtime/`
    - 仅删除 owner backend 已失活的 `fbspill-*` / `toast-retired-*`
    - 活跃 owner、命名不匹配、状态不确定时一律跳过
  - `pg_flashback.parallel_workers` 默认值当前已调整为 `8`
- 当前 `pg_flashback()` 进度显示固定为 9 段：
  - stage `9` 已改为 residual 历史行发射
- 当前仓库发布结构补充固定为：
  - 根仓库继续保留内部研发资料
  - 新增 `open_source/pg_flashback/` 作为长期维护的开源镜像目录
  - 当前术语约定固定为：
    - 用户提到“开源项目”时，默认就是指 `open_source/` 目录中的内容
    - 用户提到“开源版本”/“GitHub 版本”时，也按同一口径理解
  - 开源镜像不作为首改目录
  - 统一通过 `scripts/sync_open_source.sh` 按白名单刷新
  - 开源镜像首版仅保留：
    - `Makefile`
    - `README.md`
    - `LICENSE`
    - `VERSION`
    - `pg_flashback.control`
    - `summaryd/`
    - `include/`
    - `src/`
    - `sql/`
    - `expected/`
  - 当前开源发布口径补充为：
    - 根目录许可证固定为 `Apache-2.0`
    - 根目录发布版本固定由 `VERSION` 文件声明
    - 扩展安装版本固定由 `pg_flashback.control` 的 `default_version` 与
      `sql/pg_flashback--*.sql` / `sql/pg_flashback--*--*.sql` 升级链共同维护
    - 根目录 `README.md` 固定为面向开源用户的极简入口文档
    - 只保留：
      - 核心功能与“为什么 PostgreSQL 自带做不到”
      - 基础使用方式
      - 使用前提条件
      - 几个常用参数的作用与最小用法
    - README 中 PostgreSQL 版本口径当前按本机已验证的 `PG14-18` 书写
    - 开源镜像不再携带架构文档、ADR、开发记录或其他研发说明
  - 明确不进入开源镜像：
    - `STATUS.md` / `TODO.md` / `PROJECT.md`
    - `docs/`
    - `tests/`
    - 日志、性能采样、构建产物与其他临时输出
  - `open_source/` 根目录也不再保留额外说明文件或 manifest：
    - 当前仅保留真正待发布的 `open_source/pg_flashback/`
    - 避免把内部同步流程、研发台账路径或 `.codex` 等仓内辅助信息带入开源交付面
  - `open_source/` 目录当前也不纳入本仓库 Git 跟踪：
    - 作为本地开源镜像导出目录保留
    - 由 `scripts/sync_open_source.sh` 按需重建
- 当前 `summary` 主线补充：
  - 已拍板启动 `block-anchor summary v1`
  - 首版目标不是全量 block span/row-image sidecar
  - 首版只在 segment summary 中补充 relation-scoped block anchor facts：
    - block 上最近可用 `FPI/INIT_PAGE` 锚点
    - 优先服务 `4/9 replay discover` / `5/9 replay warm`
  - 首版不承诺直接优化 `8/9 applying reverse ops`
  - 首版保持现有 relation/xid summary section 与 WAL fallback 语义不变
  - `pg_flashback_summary_progress` 还需补“最近一次实际查询是否发生 summary 降级”观测，避免把文件覆盖率 `100%` 误读成“最近查询必然走到 summary-first 索引”
  - `summary service` 需要对 `meta/summary` 做源 WAL 探活：
    - 若归档目录 / `pg_wal` / `recovered_wal` 中对应 segment 已不存在
    - 则对应 summary 索引文件也必须删除
    - 不能继续保留为“历史已构建但当前无源文件”的僵尸 summary
- 当前新增公开 DML profile 入口已拍板：
  - 入口同时提供：
    - 单行汇总 `pg_flashback_dml_profile(anyelement, text)`
    - 多行明细 `pg_flashback_dml_profile_detail(anyelement, text)`
  - 统计口径固定为：
    - 精确 WAL/index 统计
    - 按当前 replay/WAL `kind` 计操作数
    - `HEAP2_MULTI_INSERT` 按 1 次操作计，不按 tuple 数计
    - 同一目标时间点在无新目标 relation WAL 的前提下，多次调用必须返回稳定一致的计数
  - 汇总层需要暴露：
    - `total_ops`
    - `insert/update/delete/vacuum/other` 计数与比例
    - `*_pct` 当前口径固定为保留两位小数
  - 明细层需要暴露当前支持的 `FbWalRecordKind` 分布
- 开发期调试约定补充：
  - 手工导出 / 保留 core dump、gdb 临时 core 文件时，统一使用 `/isoTest/tmp`
  - 不再使用 `/tmp` 作为本项目的临时 core 落盘路径
- 当前执行约束补充：
  - 只要出现“WAL 回放失败导致闪回失败”的现场，必须继续追到 root cause 并彻底修复
  - 不接受把问题停留在表层报错变化、提前降级绕过、仅调整缺页判定或仅改错误提示
  - 只要出现 `3/9 build record index` 性能问题，必须继续追到真实热点与 root cause 并完成修复
  - 不接受把问题停留在表层阶段名、NOTICE 百分比或单次观测误读上就停止
  - 若现场显示为 `3/9 30% metadata` / `55% xact-status` / `70%-100% payload`
    的任一慢点，必须继续用 debug counter、`pg_stat_activity`、`gdb/perf`
    等证据确认真实耗时归属，再决定修复点
  - 本次 `scenario_oa_12t_50000r.documents @ '2026-04-01 23:15:13'`
    已按同一标准继续追查：
    - 即使 `missing FPI for block 216136` 已不再是表层错误
    - 仍必须继续修到 `84/AE079278 / blk=216125 / failed to replay heap insert` 的真实根因为止

## 当前进行中

- 2026-04-12 已完成 `pg_flashback_dml_profile()` 重复调用计数漂移修复：
  - 用户现场：
    - 同一 relation + 同一 `target_ts`，库内无新增业务操作时
      `total_ops / insert_ops / *_pct` 多次调用返回不一致
  - 当前 root cause 已定位为：
    - WAL index payload stats 在 reset / rebuild 路径中
      未同步清空 `target_kind_counts[]`
    - 同一次构建里若发生多次 stats reset 或 merge，
      DML profile 汇总会在旧 kind 计数上继续累计
  - 本次修复目标：
    - 保证重复调用返回稳定一致的 profile 计数
    - `*_pct` 统一按两位小数返回
    - 补回归覆盖“重复调用一致性 + 百分比格式”
  - 当前已完成修复：
    - `fb_wal_reset_record_stats()` 与
      `fb_wal_enable_incremental_payload_stats()` 统一补齐
      `target_kind_counts[]` 清零
    - 新增 `fb_dml_profile_counter_reset_contract_debug()`，
      锁住 kind 计数 reset 不留残值
    - 公开 profile 返回类型前滚到 `0.2.5`：
      - `pg_flashback_dml_profile(anyelement, text)`
      - `pg_flashback_dml_profile_detail(anyelement, text)`
      的 `*_pct / op_pct` 已改为 `numeric`
    - 百分比内部统一按两位小数字符串构造 numeric datum
  - 当前验证结果：
    - 手工 SQL：
      - `fb_dml_profile_counter_reset_contract_debug()`
        返回
        `kind_counts_reset=1 incremental_kind_counts_reset=1`
      - `pg_get_function_result(...)` 已确认
        summary/detail profile 的百分比列都已是 `numeric`
    - 14pg targeted regression：
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config PGPORT=5432 PGUSER=14pg installcheck REGRESS='fb_dml_profile_contract'`
      - `All 1 tests passed.`
    - 开源镜像：
      - 已执行 `scripts/sync_open_source.sh`
  - 当前残留：
    - `fb_dml_profile` / `fb_extension_upgrade` 的完整组合回归
      在本机 14pg 当前 WAL 环境下仍偏慢，
      本次未等待到完整结束
- 2026-04-12 深夜已用内核调试完成
  `scenario_oa_50t_50000r.documents @ 2026-04-11 07:50:40.916510+00`
  的 `3/9 100% payload ~= 101s` 现场复核、定因与修复：
  - 用户现场同口径在本机 14pg `alldb` 复现为：
    - `3/9 30% metadata (+7311.744 ms)`
    - `3/9 55% xact-status (+27845.013 ms)`
    - `3/9 100% payload (+140753.742 ms)`
    - 随后在 preflight 命中
      `estimated flashback working set exceeds pg_flashback.memory_limit`
  - 内核调试证据：
    - live `gstack` 直接命中：
      - `fb_wal_visit_payload_locators_exact()`
      - `XLogReadRecord() -> WALRead() -> pread64()`
    - `perf` 采样确认当前 `3/9` payload 真热点为：
      - `copy_user_enhanced_fast_string`
      - `hash_search_with_hash_value`
      - 其中扩展侧主栈落在：
        - `fb_wal_note_precomputed_missing_blocks()`
        - `fb_index_record_visitor()`
        - `fb_wal_visit_payload_locators_exact()`
  - 源码根因已收口为：
    - 当前 `fb_wal_should_visit_payload_locators_windowed_serial()`
      被硬编码为 `return false`
    - dense locator case 即使已生成可安全截断到“本窗口最后一个 locator”
      的 visit window，也被强制退回
      `fb_wal_visit_payload_locators_exact()`
    - 对 `documents` 这类数百万 locator 的 14pg 冷读现场，
      会重新放大为 per-locator `XLogReadRecord/WALRead`
  - 是否已知原因：
    - 是
    - 这属于当前文档里已经登记的 residual known cause：
      “serial locator-window visitor 运行时仍关闭，dense locator 继续走 exact path”
    - 但本次是第一次用 live `gstack/perf` 把用户 `101s` 现场
      与这条代码分支直接对上
  - 当前已完成修复：
    - `fb_wal_should_visit_payload_locators_windowed_serial()` 改为
      仅在：
      - `window_count < locator_count`
      - 且 `locator_count / window_count >= 256`
      时启用
    - 保持 sparse locator 继续走 exact path，避免重新放大过读
  - 当前验证结果：
    - 14pg targeted regressions：
      - `fb_wal_locator_reader_profile`
      - `fb_wal_locator_window_serial`
      - `All 2 tests passed.`
    - 14pg `alldb` 同一条 SQL 两个新 backend 复跑：
      - `3/9 100% payload (+11193.494 ms)`
      - `3/9 100% payload (+9194.570 ms)`
    - 相比修前同机复现：
      - `3/9 100% payload (+140753.742 ms)`
      - 当前 payload 已下降约 `92%~93%`
    - 第二次复跑里 `3/9 30% metadata (+26140.374 ms)` 仍出现独立波动；
      当前判断这不是本次 `payload 101s` 的同一 root cause，
      继续单独跟踪
- 2026-04-12 已启动 `scenario_oa_50t_50000r.documents @ 2026-04-11 07:40:40.223683+00`
  的 `locator-only` 主链改造：
  - 现场 SQL：
    - `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-11 07:40:40.223683+00') order by id;`
  - 当前复现与证据：
    - 14pg `alldb` 原样复跑结果：
      - 总耗时 `140.245s`
      - 在 `3/9 100% payload (+88.368s)` 后因
        `estimated flashback working set exceeds pg_flashback.memory_limit`
        失败
    - `pg_flashback_summary_progress` 同时显示：
      - `last_query_summary_ready = t`
      - `last_query_summary_span_fallback_segments = 0`
      - `last_query_metadata_fallback_segments = 0`
    - `perf` / `gdb` 已确认当前真热点不是 summary fallback，而是：
      - `fb_index_append_locator_stub()`
      - `fb_wal_capture_locator_stub_stats_parallel()`
      - `fb_wal_merge_payload_stats_log()`
      - `fb_wal_merge_missing_blocks()`
      - `WaitForBackgroundWorkerShutdown()` / `BufFileWrite` / `BufFileRead`
    - root cause 已收口为：
      - full locator-cover 路径仍先把数百万 locator 落成 `record_log` stub
      - worker 再把 payload stats / missing-block 落成 sidecar 临时文件
      - leader 最后再完整回读并 merge
      - 形成 `stub 落盘 -> sidecar 落盘 -> leader 回读` 的三次大搬运
  - 当前已拍板实现方向：
    - `FbWalRecordIndex` 直接持有 query-local `locator stream`
    - `FbWalRecordCursor` 直接消费虚拟 locator head stream
    - full locator-cover 路径不再写 locator stub
    - payload stats worker 改为内存返回聚合结果
    - missing-block sidecar 当前暂时保留
  - 当前实现状态：
    - `fb_wal_locator_stream_contract_debug()` 已落地
    - full locator-cover 路径已改为 direct locator stream，不再写 head stub spool
    - payload stats worker 已改为 DSM 内存回传，不再写 `wal-payload-stats-worker-*.bin`
    - `FbWalRecordCursor` 已支持虚拟 locator head stream + tail spool 混合读取
  - 当前验证状态：
    - 新增回归：
      - `fb_wal_locator_stream`
    - 14pg targeted regressions：
      - `fb_wal_locator_stream`：`All 1 tests passed.`
      - `fb_replay_final_progress`：`All 1 tests passed.`
    - 14pg `alldb` 原始 SQL 复跑：
      - 默认配置：
        - 总耗时从 `140.245s` 降到 `33.281s`
        - `3/9 100% payload` 从 `88.368s` 降到 `21.575s`
        - preflight 估算从 `1.848GB` 降到 `1.396GB`
      - `spill_mode='disk'`：
        - 运行到 `52.196s`
        - 已越过 `3/9`，改为在 `4/9 BlockReplayStore` 命中 `1GB` limit
      - `spill_mode='disk', memory_limit='4GB'`：
        - 运行到 `68.764s`
        - 仍在 `4/9 BlockReplayStore` 命中 `4GB` limit
  - 当前结论：
      - `3/9 locator-only stub` 主热点已被移除
      - 但 `3/9` 仍未完成收敛：
        pure locator 场景当前还会在 payload 末尾落回
        `fb_wal_finalize_record_stats()` 全量二次回读
      - 新主 blocker 已收口为：
        `3/9 single-pass payload metadata contract` 仍未建立完整
  - 2026-04-12 夜间继续对
    `scenario_oa_50t_50000r.documents`
    的 `2026-04-11 07:40:40.223683+00` /
    `2026-04-11 07:50:40.916510+00` 做
    `fb_recordref_debug` 对比后，已确认：
    - 两个时间点的 payload locator 计划完全一致：
      - `payload_refs=4353476`
      - `payload_covered_segments=851`
      - `payload_sparse_reader_resets=20760`
      - `payload_sparse_reader_reuses=4332716`
      - `summary_payload_locator_segments_read=892`
      - `summary_payload_locator_fallback_segments=0`
    - 因而 `3/9 payload ~= 11.9s` 与 `~= 62.9s`
      的分叉不是“10 分钟时间差导致 payload plan 改变”，
      而是当前 exact-locator 物理读法本身不稳定：
      - 逻辑上仍是同一套 locator plan
      - 物理上仍混合了隐式 small-gap 跟读
        与 per-locator exact begin-read
      - 在 release-gate 干扰/冷读形态下，
        同一批 locator 会出现明显时延分叉
    - 当前修复方向已更新为：
      - 不再让 exact path 隐式承担 small-gap 顺扫
      - 改为把可合并的 dense locator run
        显式切到“按最后 locator 截断”的
        serial windowed visitor
      - exact path 自身只保留真正的 exact reuse，
        避免同一 payload plan 因 cache/干扰形态不同
        出现大幅波动
  - 2026-04-12 深夜已完成上述分叉修复并做 clean restart live 复测：
    - 当前修复：
      - exact locator runtime 不再隐式跨 small gap 顺扫
      - dense locator 计划改为在正式扫描前，
        先按 covered segment / LSN range 发
        `POSIX_FADV_WILLNEED` 预取
      - `windowed_serial` 继续保留为关闭态，
        不再把 dense case 强行切到另一套 visitor
    - 修复后 live `fb_recordref_debug`：
      - `07:40`：`real 12.06s`
      - `07:50`：`real 11.19s`
      - 两个时间点 payload 计数继续完全一致，
        但读法已收敛为：
        - `payload_sparse_reader_resets=3658980`
        - `payload_sparse_reader_reuses=694496`
    - 修复后 clean restart 全量 SQL：
      - `07:40`
        - `3/9 payload (+9666.451 ms)`
        - 总耗时 `10801.944 ms`
      - `07:50`
        - `3/9 payload (+10049.896 ms)`
        - 总耗时 `11494.410 ms`
    - 当前结论：
      - 同 payload plan 的 `~11.9s -> ~62.9s`
        分叉已消失
      - 现场差异根因不是时间点对应不同 payload plan，
        而是旧 exact-locator 读法对冷读/干扰形态过于敏感
      - 新策略已把 `documents` 两个时间点都稳定压回
        `~10s` 量级
  - 2026-04-12 深夜继续追 `scenario_oa_50t_50000r.users @ 2026-04-11 07:50:40.916510+00`
    的 `3/9 payload ~= 33.680s` 现场后，已确认：
    - 这不是新的 `users` payload 算法热点
    - clean restart / 单 backend 下同一条 SQL 当前实测：
      - `3/9 metadata (+895.688 ms)`
      - `3/9 payload (+483.935 ms)`
      - `[done] total elapsed 4359.087 ms`
    - 同一版本下，若并发压上多条 dense `documents`
      flashback / recordref 查询：
      - `users` 会被放大到：
        - `3/9 metadata (+13716.835 ms)`
        - `3/9 payload (+53111.109 ms)`
        - `[done] total elapsed 74292.004 ms`
    - 被放大时对慢 `users` backend 直接查看 `/proc/<pid>/stack`
      已确认卡在：
      - `generic_file_buffered_read`
      - `xfs_file_buffered_aio_read`
      - `ksys_pread64`
    - 当前根因已收口为：
      - dense `documents` 的 `3/9 payload`
        会并发占满 shared WAL buffered IO
      - `users` 的慢样本本质上是
        `pread64 -> WALRead -> XLogReadRecord`
        的跨 backend 争用放大
      - 不是 `users` 自身切到了新的错误 payload plan
    - 当前已完成修复：
      - 为 dense streaming payload reader 增加了全局 advisory gate
      - gate 只作用于超大 dense `documents` 类 payload exact-reader
      - 稀疏 / 轻查询不进入该 gate
    - 当前验证结果：
      - clean restart / 单 backend：
        - `users @ 07:50`
          - `3/9 payload (+332.845 ms)`
          - `[done] total elapsed 4103.639 ms`
      - 并发压上 4 条 dense `documents` `fb_recordref_debug`：
        - 修复前：
          - `users @ 07:50`
            - `3/9 metadata (+13716.835 ms)`
            - `3/9 payload (+53111.109 ms)`
            - `[done] total elapsed 74292.004 ms`
        - 修复后：
          - `users @ 07:50`
            - `3/9 metadata (+1061.235 ms)`
            - `3/9 payload (+618.850 ms)`
            - `[done] total elapsed 6629.694 ms`
      - 同时 `pg_stat_activity` 已直接观察到：
        - dense `documents` backend
          `wait_event_type='Lock'`
          `wait_event='advisory'`
    - 当前结论：
      - 这次 `users payload 33s` 属于已知原因：
        dense `documents` 并发导致的 shared WAL buffered IO 争用
      - 不是新的 `users` payload path 逻辑回归
      - 该争用放大当前已被 gate 收口
  - 下一步：
    - 先建立通用 `single-pass payload metadata` 主线：
      - locator / sparse / windowed fallback 共用首次 payload 访问
        的增量 collector
      - `record stats` / `replay block metadata` /
        `precomputed missing-block` 全部在首次 payload 访问顺手产出
      - `fb_wal_finalize_record_stats()` 仅保留给“metadata 不完整的兜底旧路径”
    - 完成后再继续压 `4/9 replay discover / BlockReplayStore`
- 2026-04-12 晚间对 `random_flashback_2` payload 现场继续取证后，
  已新增一个比 `BlockReplayStore` 更靠前的 payload blocker：
  - 当前复现：
    - `scenario_oa_50t_50000r.documents @ 2026-04-11 07:50:40.916510+00`
      在 14pg `alldb` 上：
      - `3/9 payload ~= 91.805s`
      - 之后在 preflight 命中
        `estimated flashback working set exceeds pg_flashback.memory_limit`
    - `scenario_oa_50t_50000r.meetings @ 2026-04-11 07:50:40.916510+00`
      在 14pg `alldb` 上：
      - `3/9 payload ~= 31.213s`
      - 整条查询总耗时 `34.427s`
    - 同批 `approval_tasks` 现场已恢复正常：
      - `3/9 payload ~= 1.067s`
      - 整条查询总耗时 `4.610s`
 - 2026-04-12 晚间已对用户现场再次原样复核：
   - 现场 SQL：
     - `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-11 07:40:40.223683+00') order by id;`
   - 14pg `alldb` 当前实测：
     - `3/9 100% payload (+24.023s)`
     - 总耗时 `28.095s`
     - 之后仍在 preflight 命中
       `estimated=1395882112 bytes limit=1073741824 bytes`
   - live `gdb` 栈已直接确认 backend 停在：
     - `fb_wal_finalize_record_stats()`
     - `fb_wal_record_cursor_read_skeleton()`
     - `fb_wal_load_record_by_lsn()`
     - `XLogReadRecord() -> WALRead() -> pread64()`
   - 当前 root cause 已更新为：
     - 这条查询虽然已经去掉 `locator stub + payload stats sidecar`
       主热点
     - 但 pure locator payload 仍未启用通用增量 metadata collector
     - 因而在 `3/9` 尾部仍会追加一遍完整 cursor/WAL reread
   - 当前已拍板的新修复方向：
     - 不再只修 pure locator 特例
     - 改为建立通用 `single-pass payload metadata contract`
       让 locator / sparse / windowed fallback 都默认不再走
       `fb_wal_finalize_record_stats()` 二次全量回读
   - 当前实现与验证结果：
     - 已新增 contract regression：
       - `fb_wal_payload_metadata_contract`
     - `FbWalRecordIndex` 已新增
       `payload_metadata_complete` 契约位
     - locator payload 已切到与 sparse / windowed fallback 共用的
       增量 metadata collector
     - `fb_wal_finalize_record_stats()` 已收缩为
       metadata incomplete-only fallback
     - 14pg `alldb` 同一条 SQL 最新复跑：
       - 在已做过前序复跑的 follow-up 样本里：
         - `3/9 100% payload` 从 `24.023s` 降到 `16.384s`
         - 总耗时从 `28.095s` 降到 `17.435s`
       - 该数值不再视为 cold-run 保证值
       - preflight 估算当前仍为 `1.396GB`
     - 同一现场 live `gdb` 已确认新栈停在：
       - `fb_wal_visit_payload_locators_exact()`
       - 不再停在 `fb_wal_finalize_record_stats()`
     - 用户后续在同机 `alldb` 的再次复跑已证明：
       - `3/9 payload` 仍可能回到 `35s+`
       - 因而当前真正剩余的主热点已经收口为
         exact locator 首读的 cold-ish IO 成本
     - 继续收口 exact locator 冷读后，已拿到可复现的新收益：
       - dense exact locator run 当前不再默认走 `FB_WAL_OPEN_SPARSE`
       - 改为 `streaming` open pattern：
         - 只给 `POSIX_FADV_SEQUENTIAL`
         - 不附带全窗口顺扫使用的 `WILLNEED`
       - 新增 contract regression：
         - `fb_wal_locator_reader_profile`
       - 14pg `alldb` 同一条 SQL 在两个全新 backend 连续复跑：
         - `3/9 payload +15.929s`，总耗时 `17.303s`
         - `3/9 payload +13.319s`，总耗时 `14.753s`
       - 相比用户本轮给出的同机样本
         `3/9 payload +35.850s`，已出现可复现下降
   - 下一步：
     - 继续压 exact locator 首读本身的 decode / per-record 常数
     - 继续收口 preflight 估算与 `4/9 BlockReplayStore`
       的后续 blocker
  - 现场证据：
    - `gdb` 直取 backend 栈已确认 `documents` payload 慢点不在
      summary fallback / locator stub / payload stats sidecar
    - 真热点停在：
      - `fb_replay_precompute_index_metadata()`
      - `fb_replay_build_prune_lookahead()`
      - `fb_wal_record_cursor_read_lookahead()`
      - `fb_spool_cursor_read() -> FileRead/pread64`
    - 即：
      - 当前 `3/9` 在 locator-stream 并行 stats-only 路径下
        仍会把 worker 产出的 `lookahead_log` 再完整反读一遍
      - 把原本想前移优化的 prune lookahead 工作，
        重新变成了新的 payload 主热点
  - 当前结论：
    - 现有“`3/9` eager prune precompute + lookahead spool reread”
      形态已判定为错误方向，不保留
    - `3/9` 允许继续前移：
      - block lifecycle metadata
    - 但在真正的 dense future-block delta 落地前，
      不允许在 `3/9` 再做一遍 lookahead spool 全量反扫
  - 当前修复方向：
    - 若 replay prune metadata 仍依赖 `lookahead_log`
      才能构建，则 `fb_replay_precompute_index_metadata()` 直接跳过
    - `6/9 final` 继续走现有 lazy fallback，
      由 `fb_replay_prepare_final_control()` 按需构建 prune lookahead
  - 当前已完成修复：
    - `fb_replay_should_precompute_index_metadata()` 新增 eager gate：
      - direct locator-stream / `lookahead_log` 场景不再在 `3/9` 预计算
    - direct locator-stream stats-only worker
      不再写 `wal-lookahead-worker-*.bin`
    - `fb_wal_visit_payload_locators_exact()` 已补 sequential locator reuse：
      - 相邻 locator 命中同一 reader 连续位置时，不再每条都 `XLogBeginRead()`
  - 当前 14pg live 复跑结果：
    - `random_flashback_1.approval_tasks`
      - 用户原日志：`3/9 payload ~= 145.969s`
      - 当前复跑：`3/9 payload ~= 0.415s`
    - `random_flashback_2.approval_tasks`
      - 用户原日志：`3/9 payload ~= 63.993s`
      - 当前复跑：`3/9 payload ~= 0.529s`
    - `random_flashback_2.meetings`
      - 用户原日志：`3/9 payload ~= 64.144s`
      - 当前复跑：`3/9 payload ~= 0.291s`
    - `random_flashback_2.users`
      - 用户原日志：`3/9 payload ~= 30.351s`
      - 当前复跑：`3/9 payload ~= 0.291s`
    - `random_flashback_2.documents`
      - 本轮修复前当前主干复现：
        - `3/9 payload ~= 91.805s`
      - 当前复跑：
        - `3/9 payload ~= 7.520s ~ 9.087s`
        - 之后仍在 preflight 命中 `memory_limit = 1GB`
  - 当前结论更新：
    - `3/9 eager prune precompute` 与 `lookahead spool` 回写 bug 已修掉
    - `users/meetings/approval_tasks` 的 payload 热点已不再存在
    - 对 `documents` 的 perf 采样已确认剩余 `3/9 payload ~= 6.5s`
      不再主要烧在 exact locator 冷读，而是：
      - `fb_wal_fill_xact_statuses_serial()`
        - `fb_merge_visit_windows()`
        - `fb_find_segment_index_for_lsn()`
      - `fb_build_summary_payload_locator_plan()`
        - `fb_summary_merge_payload_locator_slices()`
        - `pg_qsort()`
    - 当前判断：
      - segment index 线性查找
      - payload locator slice 全量拷贝后再 `qsort`
      是 `documents` 剩余 payload 未继续下探的直接原因
  - 当前单跑补测：
      - `documents` 在上述两项修复后，
        `3/9 payload` 已进一步收敛到 `~6.68s`
      - 但仍略高于用户原日志 `59.75s` 的 `10%` 门槛
  - 2026-04-12 夜间继续对 release gate 串行现场补测后，
    已确认上一轮结论只覆盖了“单 backend / 非 release-gate 完整口径”的一部分，
    还存在一个更靠后的 locator 访问器系统性回退：
    - 14pg `alldb` 串行复跑
      `scenario_oa_50t_50000r.documents @ 2026-04-11 07:50:40.916510+00`
      且注入
      `PGOPTIONS='-c pg_flashback.memory_limit=6GB -c pg_flashback.target_snapshot=1451566:1451569:1451566'`
      时，当前现场仍能稳定看到：
      - `3/9 payload ~= 15.531s`
    - 同口径 `approval_tasks` 已恢复到：
      - `3/9 payload ~= 0.835s`
      - `[done] total ~= 7.072s`
    - 现场分析：
      - 目前 `documents` 的 `3/9` 已不再主烧在
        `fb_wal_capture_locator_stub_stats_parallel()` /
        `fb_wal_materialize_payload_locators_parallel()` /
        eager `fb_replay_precompute_index_metadata()`
      - 剩余热点已收口到 direct locator stream 的串行 locator 访问器：
        - 当前 exact path 只有“下一条 locator 恰好等于上一条 `EndRecPtr`”
          才复用 reader
        - 对 dense 但非严格连续的 `documents` locator 集，
          仍会大量退化为 per-locator `XLogBeginRead()`
      - 之前被否决的“window 顺扫”试验并不是方向错误，
        真 bug 在于：
        - 读窗上界错误沿用了 segment 末尾
        - 前一个 window 会顺扫吞掉后一个 window 的 locator
        - 触发
          `summary payload locator skipped during batched visit`
    - 下一刀修复方向：
      - 重建 serial locator-window visitor，
        但读窗必须按“本 window 最后一个 locator”截断，
        不能再放大到 segment 尾
      - 目标先压 `documents` 的 `3/9 payload < 10s`，
        再继续复核 `6/9` / `8/9` 的 TOAST 长尾
  - 2026-04-12 深夜继续用 regression-only live counter 复核后，
    已确认 `documents` 当前最核心的系统性 bug 不是
    exact locator seek 本身，而是“locator mode 状态错配”：
    - 14pg `alldb` 现场手工创建 `fb_recordref_debug(regclass,timestamptz)`
      后，同一条 release gate case 返回：
      - `payload_scan_mode=locator`
      - `summary_payload_locator_records=4353476`
      - `summary_payload_locator_fallback_segments=0`
      - 旧实现下：
        - `head_gap_refs=4353476`
      - 这说明：
        - summary 已经 100% 覆盖 payload locators
        - 但 query-side 既没有真正切到 `locator_stream`
        - 又把全部 435 万条 payload 继续落进 `record_log`
    - 当前根因代码已锁定：
      - `fb_wal_build_record_index()` 在 pure locator case 中
        先把 `payload_scan_mode` 标成 `locator`
      - 但 `index->locator_stream` 只在已关闭的并行 stub-stats 分支赋值
      - 同时 `state.payload_log` 仍无条件指向 `index->record_log`
      - 最终形成：
        - 名义 locator
        - 实际无 locator stream
        - 还全量写 head spool
    - 当前已完成修复：
      - pure locator case 现在无条件挂 `index->locator_stream`
      - pure locator case 不再把 payload 落入 `index->record_log`
      - pure locator case 不再复制 payload body
      - locator-only 路径也不再启用会被后续 `finalize_record_stats()`
        丢弃的 incremental payload stats
    - live counter 复核结果：
      - 同一现场 `head_gap_refs` 已从 `4353476` 降到 `0`
      - `payload_scan_mode=locator` 保持不变
      - `payload_refs=4353476` 仍在，但已转为真正的 locator stream
    - 当前判断：
      - release gate `documents` 剩余耗时已不再是
        “假 locator + 真全量 head spool” 这条错配 bug
      - 还需要继续在干净单进程现场复核新的 `3/9 payload`
        稳态值，并继续压 locator-only replay/final 长尾
  - 已否决的试验：
      - exact locator small-gap 顺扫
      - 14pg live 复跑会把 `documents`
        `3/9 payload` 反向放大到 `~61.57s`
      - 结论：中间非目标 record 足够密时，
        顺扫比 `XLogBeginRead()` 重置更差，不能保留
      但相对本轮 root-cause 复现已降到 `7.520s`，约 `91.8%` 下降
  - 下一步：
    - 继续只盯 `documents` 的 exact locator 冷读页复用 / 去重
    - 不再回到 eager precompute / lookahead spool 旧路径
    - 高内存完整链路新证据：
      - `spill_mode='disk', memory_limit='12GB'`
      - `6/9 replay final and build forward ops` 约 `35.640s`
      - `8/9 applying reverse ops` 两段累计约 `147.103s`
      - 整条 `count(*)` 总耗时约 `261.340s`
    - 当前结论已更新为：
      - `4/9 BlockReplayStore` 仍是默认 limit 下的首个内存门槛
      - 但高内存口径下真正最大的隐藏时长热点已经转移到
        `8/9 applying reverse ops`
    - `8/9` 当前新增 root cause 证据：
      - `scenario_oa_50t_50000r.documents` 主键存在：
        `CREATE UNIQUE INDEX ... documents_pkey ... (id)`
      - 但 `pg_class.relpages = 458789`
      - 当前 `fb_apply_parallel_supported()` 仍写死
        `FB_APPLY_PARALLEL_MAX_REL_PAGES = 32768`
      - 导致这类大表即使是 keyed + 有主键索引，也会被直接挡在并行
        apply 之外
      - 当前假设优先级最高的修复点就是：
        收口/放开这条 `relpages` 上限 gate，并用现场 SQL 复测
    - 2026-04-12 新增 `6/9 replay final` 根因收口：
      - 活体 `gdb` 已确认 `fb_replay_build_prune_lookahead()` 会通过
        `fb_wal_record_cursor_read()` 触发 `fb_wal_load_record_by_lsn(..., include_payload=true)`
      - 这条路径真实只依赖：
        - `main_data`
        - `HEAP2_PRUNE` 的 block data
        - block/image 标志位
      - 不需要 tuple payload / page image 本体
      - 当前已改为 `lookahead metadata-only` 读取：
        - `fb_wal_record_cursor_read_lookahead()`
        - locator/deferred spool 场景都只补 prune lookahead 真正需要的 metadata
        - 新增回归 `fb_replay_prune_lookahead_payload`
          锁住 `payload_loads=0`
    - 2026-04-12 新增 `4/9 replay discover` 根因收口：
      - `perf` 显示约 `53%` 样本落在 `clear_page_erms`
      - `gdb` 已确认热点位于
        `fb_replay_ensure_block_ready() -> MemSet(state, 0, sizeof(*state))`
      - root cause 为：
        - 新 block 进入 `BlockReplayStore` 时先整页清零
        - 随后又立即 `memcpy(image)` 或 `PageInit()` 再写一遍整页
      - 当前已改为字段级 `fb_replay_init_new_block_state()`
        初始化，不再对 `state->page` 做无意义整页清零
      - 新增回归 `fb_replay_block_state_init`
        锁住 `page_byte_preserved=true`
    - 2026-04-12 当前继续追 `4/9 replay discover` 活体热点：
      - 同轮 `perf/gdb` 已确认 `clear_page_erms` 退下后，
        热点前移到：
        - `fb_wal_visit_resolved_records()`
        - `fb_replay_warm_store()`
        - `hash_search`
        - `fb_toast_store_put_tuple()`
        - `fb_replay_get_block()`
      - 其中新增高优先 root cause 为：
        - discover 阶段只需要缺页锚点 / block 基线是否可回放
        - 但当前共享 backtrack warm pass 与 discover pass
          仍携带 `discover_toast_store`
        - 导致 TOAST relation 在 discover 热路径里仍执行
          `fb_toast_store_put_tuple()` / chunk hash 插入
        - 这些结果不会复用到最终查询的 warm/final 历史 TOAST 重建
      - 当前收口方向已固定为：
        - discover 不再创建/写入 query-local TOAST store
  - 2026-04-12 已追加拍板下一阶段实现顺序：
    - 目标不是只压单一热点，而是一起完成 4 刀联动收口：
      1. 将 block 生命周期元数据前移到 `3/9`
      2. 用 query-local compact store 重写 `BlockReplayStore`
      3. 删除 `6/9` backward prune prepass
      4. 将 discover 收窄到 `frontier apply`
    - 当前约束：
      - `3/9` 只能 piggyback 现有 payload/index 物化链路
      - 不允许为 `last_use` / `prune future` 再新增第二遍 WAL 或 cursor 扫描
      - `3/9` 新增 wall time 预算固定为 `<= 5s`
      - 若首轮实现使同一条 14pg live SQL 的 `3/9` 增量超过 `5s`，
        则该实现形态直接判失败，不继续叠加更多前移逻辑
    - 当前实现拆分：
      - 第一步先补 `3/9 -> replay` 生命周期 / prune 元数据契约与回归
      - 第二步切 `6/9` 到 dense future-block vector，删除 backward prepass
      - 第三步把 `BlockReplayStore` 切到 compact store
      - 第四步再让 discover 只消费 missing-block frontier 闭包
    - 当前落地进展：
      - `FbWalRecordIndex` 已开始在 `3/9` 顺手产出 per-block
        `last_replay_use_record_index` /
        `last_prune_guard_record_index`
      - `fb_replay_run_pass()` 已按上述生命周期元数据即时 retire block state，
        不再默认把 block 一直留到阶段尾
      - `fb_wal_finalize_record_stats()` 已补上 locator-stream /
        finalize skeleton pass 的 metadata 重建，避免 build 收尾阶段把
        `replay_block_metadata` 清空
      - `fb_replay_build_prune_lookahead()` 当前已改成生成 dense vector，
        `final` 热路径不再对每条 prune 记录做
        `record_index -> hash_search`
      - `BlockReplayStore` 已切到 query-local compact store：
        - open addressing 单 probe
        - query-local state arena + freelist
        - 不再依赖 `dynahash + HASH_FIND/HASH_ENTER`
      - discover 已尝试收窄到 frontier-only skeleton closure：
        - active closure 命中的 record 只扩 missing frontier
        - 不再对 discover 期 active record 做部分 page apply
      - 当前仍未完成的部分：
        - prune future delta 还没有按 payload 物化即时产出，
          仍是在 build 末尾额外 backward cursor 生成
        - discover frontier closure 的 live 正确性还没有稳定
    - 2026-04-12 最新 live 验证结果：
      - focused individual regressions 当前通过：
        - `fb_replay_metadata_contract`
        - `fb_replay_block_state_init`
        - `fb_replay_discover_skip_payload`
        - `fb_replay_discover_materialize_reuse`
        - `fb_replay_discover_toast`
        - `fb_replay_prune_lookahead_payload`
        - `fb_replay_final_progress`
      - 14pg `alldb` 同一条现场 SQL 冷/热复跑都**未通过**：
        - SQL：
          `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-11 07:40:40.223683+00') order by id;`
        - 当前新实现的 warm-cache 复跑：
          - `3/9 payload +30.046s`
          - `4/9 discover round=1` 三段合计约 `51.256s`
          - 随后报错：
            `missing FPI for block 487549`
        - 当前结论：
          - 将 prune lookahead precompute 前移到 build 末尾的实现形态
            已明显突破 `3/9 <= 5s` 预算
          - frontier-only discover 现在仍会漏收某些缺页闭包，
            live case 尚未过
    - 当前预期收益：
      - `4/9 replay discover` 目标降幅 `60%`
      - `6/9 replay final` 目标降幅 `60%`
      - `3/9 build record index` 允许小幅上升，但必须受 `<= 5s` 预算约束
        - warm/final 继续保留 TOAST store 语义不变
        - 新增回归 `fb_replay_discover_toast`
          锁住 `toast_puts=0`
    - 当前验证状态补充：
      - 14pg targeted regressions：
        - `fb_replay_prune_lookahead_payload`：`All 1 tests passed.`
        - `fb_replay_block_state_init`：`All 1 tests passed.`
        - `fb_replay_discover_toast`：`All 1 tests passed.`
        - `fb_replay_final_progress`：`All 1 tests passed.`
        - `fb_apply_parallel_gate`：`All 1 tests passed.`
        - `fb_wal_locator_stream`：`All 1 tests passed.`
      - 14pg live `gdb` 复核：
        - backend 栈已收口到
          `fb_wal_visit_resolved_records() -> fb_replay_raw_warm_visitor() -> fb_replay_warm_store()`
        - `fb_replay_warm_store(... toast_store=0x0, ...)`
          已确认 discover/shared warm backtrack 不再携带 TOAST store
        - 当前前台热点已转成 `pread64()/WALRead`，
          不再停在 `fb_toast_store_put_tuple()`
      - 14pg live `perf` 新分布：
        - `fb_replay_discover` 下约 `71%` 样本落在
          `fb_replay_run_pass()`
        - 其中两块主热点分别是：
          - `fb_wal_record_cursor_read() -> fb_wal_record_materializer_load_record()
            -> fb_wal_load_record_by_lsn() -> XLogReadRecord()/ValidXLogRecord()`
          - `fb_replay_ensure_block_ready() -> fb_replay_get_block() -> hash_search()`
        - `fb_replay_warm_store()` 自身只占约 `8%`
      - 当前新增高优先 root cause 为：
        - discover pass 当前统一通过
          `fb_wal_record_cursor_read(... FULL)` 读取 locator stream
        - 即使记录随后只命中
          `fb_replay_record_touches_missing_block()`
          并提前 `continue`
        - 也已经提前做了
          `fb_wal_load_record_by_lsn()` / CRC / payload 物化
        - 这类“early-skip record 先 full materialize 再丢弃”已成为
          discover 的新主浪费面
      - 下一轮收口方向已固定为：
        - discover cursor 先走 skeleton 读取
        - 只有真正进入 `fb_replay_apply_record()` 时才补 payload/image/data
        - 先用回归锁住
          “discover skip path payload_loads=0”
      - 2026-04-12 同轮 live `perf` 继续收口后发现：
        - discover 切到 `skeleton + on-demand materialize` 后，
          `fb_replay_run_pass()` 内热点拆成了：
          - `fb_wal_record_cursor_read_skeleton()`
          - `fb_replay_cursor_materialize_for_phase()
             -> fb_wal_materialize_record_payload()`
        - applied record 当前会先做一次 skeleton `XLogReadRecord()`
          再做一次 payload `XLogReadRecord()`
        - 若 discover 期大多数记录最终都会 apply，
          这种“两次 WAL 读”会抵消甚至吞掉 skip-path 收益
      - 当前进一步收口方向已更新为：
        - 保留 discover skip-path 不补 payload
        - 但 applied record 必须复用同一 materializer / reader，
          不能再二次 `XLogReadRecord()`
        - 先补 contract 锁住
          “skeleton -> materialize 只读一次 WAL record”
    - 当前现场状态补充：
      - 本轮 clean high-memory 复测仍未把完整 SQL 收口到 `< 60s`
      - 新一轮 14pg `alldb` 冷跑：
        - `spill_mode='disk', memory_limit='4GB'`
        - `3/9 payload ~= 30.322s`
        - `4/9 replay discover` 已见：
          - `precomputed ~= 0.537 ms`
          - `round=1 0% ~= 53.729s`
          - `round=1 50% ~= 23.136s`
        - 最终仍在 `BlockReplayStore` 命中 `4GB` limit
          (`elapsed=144.28s`)
      - 当前判断为：
        - discover 内 TOAST store 热点已被移除
        - 但完整现场的 `4/9` 主耗时已前移到
          WAL 冷读 / `fb_replay_get_block` / `BlockReplayStore`
        - 这轮还不能宣称完整现场已经收口
      - 最新一轮 `count(*)` 冷跑里，`3/9 payload` 仍曾抬到
        `85.271s`
      - 说明当前还存在新的 `3/9` 冷读主热点，整条链路尚未达标
    - 继续以“完整 SQL `< 60s`”为目标收口

- 2026-04-11 已启动公开 DML profile 用户入口：
  - 目标：
    - 新增 `pg_flashback_dml_profile(anyelement, text)`
    - 新增 `pg_flashback_dml_profile_detail(anyelement, text)`
  - 统计口径：
    - 精确 WAL/index 统计
    - 只建 record index，不进入 replay/apply
    - 按当前 replay/WAL kind 计操作数
  - 当前实现状态：
    - 已补 `FbWalRecordKind` 级计数
    - 已补 SQL 安装面与 `0.2.3 -> 0.2.4` 升级链
    - 已补 `fb_user_surface` / `fb_extension_upgrade` / `fb_dml_profile` 回归资产
    - `fb_dml_profile.sql` 已修成可重复执行，不再因残留测试表在 `CREATE TABLE` 处失败
  - 14pg 当前验证状态：
    - `PG_CONFIG=/home/14pg/local/bin/pg_config make clean && make && make install` 已完成
    - 临时 14pg 集群已确认 `0.2.4` 对外安装面包含
      `pg_flashback_dml_profile(anyelement, text)` 与
      `pg_flashback_dml_profile_detail(anyelement, text)`
    - 手工执行 `sql/fb_dml_profile.sql` 时，setup + DML 阶段可通过
    - 但完整 profile 查询在 14pg 临时实例上仍会长时间停留在
      `fb_wal_build_record_index() -> fb_build_summary_span_visit_windows()`
      的 on-demand summary rebuild 路径，尚未拿到一次完整的 fresh pass 证据
  - 下一步：
    - 继续收口 14pg on-demand summary rebuild 热点
    - 补一轮 fresh 14pg 完整 profile 通过证据
    - 同步开源镜像

- 2026-04-11 已修复 14pg `scenario_oa_50t_50000r.documents @ 2026-04-11 07:40:40.223683+00`
  在 `5/9 replay warm` 后看似“卡死”的进度盲区：
  - 现场 SQL：
    - `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-11 07:40:40.223683+00') order by id;`
  - 当前复现与证据：
    - 14pg 实机复现中，`5/9 100% replay warm` 仍可单段耗时约 `47s-64s`
    - 但用户感知“卡死”窗口里的 backend 实际已进入：
      - `fb_replay_final_build_reverse_source()`
      - `fb_replay_build_prune_lookahead()`
      - `fb_wal_record_cursor_read()`
      - `fb_wal_record_materializer_load_record(... include_payload=true)`
      - `XLogReadRecord()` / `WALRead()` / `pread64()`
    - `pg_stat_activity` 同时显示 backend 仍为 `active`，
      等待点落在 `WALRead` / `BufFileRead`
    - 已确认 root cause 不是 warm 死循环或锁等待，而是：
      - `fb_replay_final_build_reverse_source()` 先执行
        `fb_replay_build_prune_lookahead()`
      - 再调用 `fb_replay_progress_enter(&final_control)`
      - 导致 `6/9 replay final and build forward ops` 的前置重活
        完全落在 `5/9 100%` 之后、`6/9 0%` 之前的无进度空窗里
      - 用户侧会误判为“SQL 卡死在 5/9”
  - 当前实现已收口为：
    - 新增 `fb_replay_prepare_final_control()`
    - final 阶段当前改为：
      - 先设置 `final_control.phase = FB_REPLAY_PHASE_FINAL`
      - 立刻进入 `6/9`
      - 再执行 `fb_replay_build_prune_lookahead()`
    - 新增 regression-only 合约入口
      `fb_replay_final_progress_contract_debug()`
      锁住 “prune lookahead 启动时当前 progress stage 已经是 `6/9`”
  - 当前验证状态：
    - 14pg 重新
      `make clean && make && make install`
      `PG_CONFIG=/home/14pg/local/bin/pg_config`
      通过
    - 新增单测：
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS=fb_replay_final_progress PGUSER=14pg`
      - 结果：`All 1 tests passed.`
  - 下一步：
    - 同步开源镜像

- 2026-04-11 已修复 14pg `scenario_oa_50t_50000r.users`
  闪回失败主链：
  - root cause 1：
    - 旧 `meta/summary` sidecar 沿用旧版 summary 格式
    - PG14 `XLOG_HEAP2_VACUUM` 没有被旧 sidecar 纳入 summary-backed payload replay
    - 导致 toast 页 `1663/4629021/4629070 blk 0` 未回收 dead tuple storage
    - 后续在 `B/0315E678` 重放 `heap insert off=36` 时空间不足报错
  - root cause 2：
    - query 初始化阶段在 `fb_apply_begin()` 后立刻销毁
      `state->reverse/state->spool`
    - parallel apply worker 随后再打开 reverse-run spill 文件时命中
      `could not open fb spill file`
  - 当前实现已收口为：
    - `FB_SUMMARY_VERSION` 前滚到 `12`
    - 强制淘汰未携带 PG14 `HEAP2_VACUUM` 的旧 summary sidecar
    - `fb_prepare_flashback_query()` 不再提前释放 reverse/spool，
      生命周期延后到 query release
    - 清理本轮为该问题添加的 summary/vacuum 临时 WARNING 日志
  - 当前验证状态：
    - 14pg 重新 `make && make install` 已完成
    - 已清空 `/isoTest/14pgdata/pg_flashback/meta/summary`
      并用新版本 `summaryd` 重建 sidecar
    - 新 summary 文件头已确认版本字节为 `0c 00`（v12）
    - 原始现场 SQL：
      `select * from pg_flashback(NULL::scenario_oa_50t_50000r.users, '2026-04-11 07:40:40.223683+00') order by id;`
      在 14pg 上复跑退出码为 `0`
      且结果文件 `/tmp/pg_flashback_users_verify.out`
      已生成 `50011` 行输出
  - 下一步：
    - 同步开源镜像

- 2026-04-11 已收口 `3/9 build record index` 的 payload notice 语义：
  - 现场已确认：
    - `NOTICE: [3/9 70%] ... payload`
      只是 payload 子阶段起点
    - `NOTICE: [3/9 100%] ... payload`
      才是 payload 子阶段完成
    - 两条当前共用同一个 `payload` 标签，用户侧会误读成同一事件重复打印
  - 当前实现已改为：
    - 删除对外 `70% payload` notice
    - 保留 `100% payload` 作为 payload 子阶段完成信号
    - `expected/fb_progress.out` 已同步锁住新的 notice 口径
  - 当前验证状态：
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config src/fb_wal.o` 通过
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config src/fb_wal.o` 通过
    - 运行时回归暂被当前工作树里的既有装载问题阻塞：
      - `could not load library ".../pg_flashback.so": undefined symbol: shmem_request_hook`
    - 该阻塞出现在本次为 PG14 执行 `make install` 后的现有工作树整体装载面，不是本次 notice 改动本身的语义风险
  - 下一步：
    - 待当前工作树装载问题收口后，补跑 `fb_progress` / 最小手工 notice 验证

- 2026-04-11 已完成公开 summary service 调试视图收口：
  - 已删除当前公开安装面：
    - `pg_flashback_summary_service_debug`
    - `fb_summary_service_debug_internal()`
  - 用户侧 summary 观测当前只保留：
    - `pg_flashback_summary_progress`
  - 兼容性口径已收口为：
    - 扩展版本前滚到 `0.2.3`
    - 已补 `sql/pg_flashback--0.2.2--0.2.3.sql`
    - 共享库内仍保留历史升级链所需 symbol，避免 `0.1.0 -> 0.2.3`
      中途经过旧脚本时缺符号
  - 当前验证状态：
    - `make clean && make && make install`
      `PG_CONFIG=/home/18pg/local/bin/pg_config` 通过
    - 临时 PG18 实例下已确认：
      - `CREATE EXTENSION pg_flashback` 后
        `pg_flashback_summary_service_debug` / `fb_summary_service_debug_internal()`
        均不存在
      - `CREATE EXTENSION pg_flashback VERSION '0.1.0'`
        后升级到 `0.2.3` 通过，且上述对象同样不存在
  - 下一步：
    - 同步开源镜像

- 2026-04-11 已确认继续收口共享库内自用 debug helper：
  - 本轮目标：
    - 删除默认不安装、仅供研发手工 `CREATE FUNCTION` 挂载的 debug SQL helper 导出
    - 删除它们对应的默认回归入口与文档引用
  - 保留边界：
    - 保留明确给用户使用的公开入口
    - 保留 `fb_pg_flashback_support`
    - 保留 `fb_summary_progress_internal()` 作为公开视图底座
  - 当前实现已继续前推到：
    - 历史版本安装/升级 SQL 也不再创建
      `fb_summary_service_debug_internal()` /
      `pg_flashback_summary_service_debug`
    - `fb_summary_service_debug_internal()` 已可从共享库导出面物理删除
  - 当前验证状态：
    - `nm -D pg_flashback.so` 已确认剩余 fmgr 导出只剩：
      - `fb_version`
      - `fb_check_relation`
      - `fb_pg_flashback_support`
      - `fb_summary_progress_internal`
      - `pg_flashback`
      - `pg_flashback_debug_unresolv_xid`
    - PG18 临时实例已确认：
      - `CREATE EXTENSION pg_flashback` 成功
      - `CREATE EXTENSION pg_flashback VERSION '0.1.0'`
        后升级到 `0.2.3` 成功
      - 两条路径下
        `fb_summary_service_debug_internal()` /
        `pg_flashback_summary_service_debug`
        都不存在
    - 验证过程中顺手修复了 PG18 装载阻塞：
      - 扩展内文件 IO 已统一走 `FileReadV/FileWriteV` 兼容包装
      - 重新按 PG18 全量重编扩展对象，避免旧版本残留 `.o`
        把 `< PG15` 的 GUC 占位符符号重新带回 `.so`
  - 下一步：
    - 同步开源镜像

- 2026-04-11 已修复 `3/9 build record index` 并行 payload 收尾重扫过慢：
  - 现场 root cause 已收口为：
    - `fb_wal_payload_merge_log()` 旧逻辑只做 worker spool 原样拼接
    - 主进程缺少并行 payload 的增量统计与 `precomputed_missing_blocks`
    - 最终必然落回 `fb_wal_finalize_record_stats()`
      对整份 merged `record_log` 做第二次全量扫盘
    - `gdb` 现场已确认旧热点栈位于：
      - `fb_spool_read_exact()`
      - `fb_wal_record_cursor_read_skeleton()`
      - `fb_wal_finalize_record_stats()`
  - 当前修复已落地：
    - `src/fb_wal.c`
      - 并行 payload merge 改为按 entry 读取 worker spool，
        在合并时同步执行：
        - `fb_index_note_materialized_record()`
        - `fb_wal_note_precomputed_missing_blocks()`
      - 非 locator-only 并行 payload 路径会在进入 worker merge 前
        先初始化增量统计状态
      - 若 merge 期发现无法就地统计的 entry，
        会主动释放增量状态并回退到旧 finalize 路径，
        避免错误统计
    - `sql/fb_wal_parallel_payload.sql`
      - 已补并行/串行 `payload_scanned_records`
        与 `payload_kept_records` 一致性断言
  - 当前验证状态：
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config src/fb_wal.o`
      通过
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config install`
      通过
    - 14pg 现场复核同一条查询：
      - `scenario_oa_50t_50000r.documents @ 2026-04-11 07:40:40.223683+00`
      - `3/9 100% payload` 已从原先约 `67.7s`
        降到本次复核的约 `20.8s-25.4s`
    - 新版 `gdb` 抽样已确认 backend 热点转移到：
      - `fb_replay_build_prune_lookahead()`
      - `fb_replay_final_build_reverse_source()`
      - 不再落在 `fb_wal_finalize_record_stats()`
  - 下一步：
    - 等 `fb_wal_parallel_payload.sql` 手工回归收完，
      再补开源镜像同步
    - 继续观察 6/9 replay final 的新主热点是否还需要单独优化

- 2026-04-11 已定位 `scenario_oa_50t_50000r.documents @ 2026-04-11 07:40:40.223683+00`
  的新 `3/9 payload` 热点：
  - 现场 SQL：
    - `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-11 07:40:40.223683+00') order by id;`
  - 当前复现状态：
    - 冷缓存首轮复现：
      - `3/9 30% metadata` `+3618.439 ms`
      - `3/9 55% xact-status` `+2135.440 ms`
      - `3/9 100% payload` `+62055.147 ms`
      - 随后在 replay preflight 报
        `estimated flashback working set exceeds pg_flashback.memory_limit`
    - 热缓存复现：
      - `3/9 100% payload` 仍稳定在约 `23.7s-25.6s`
      - 同样在 preflight 以
        `estimated=1848643616 bytes limit=1073741824 bytes`
        结束
  - 现场证据已确认这次慢点不是：
    - `summary-span` / locator plan
    - payload fallback 扫描真实 WAL 窗口
    - locator-first 主路径里的全量 payload body 写 spool
  - `gdb` 活体抽样两次都确认 backend 长时间停留在：
    - `fb_wal_finalize_record_stats()`
    - `fb_wal_record_cursor_read_skeleton()`
    - `fb_spool_cursor_read()` / `fb_spool_read_exact()`
  - 第二个 payload 期样本进一步确认当前真实调用链为：
    - `fb_wal_finalize_record_stats()`
    - `fb_wal_record_cursor_read_skeleton()`
    - `fb_wal_record_materializer_load_record(... include_payload=false)`
    - `XLogBeginRead()` / `XLogReadRecord()`
    - `fb_wal_read_page()` / `WALRead()`
  - 对应代码路径已经对上：
    - `payload_locator_count > 0 && parallel_workers > 1`
      时先走 `fb_index_append_locator_stub()`，
      并把 `state.locator_only_payload_capture = true`
    - payload 收尾随后命中
      `fb_wal_build_record_index()` 里的
      `fb_wal_finalize_record_stats(index)`
    - finalize 顺扫 spool 时遇到 `locator_only` entry，
      会在 `fb_wal_record_cursor_read_mode()`
      里调用 `fb_wal_record_materializer_load_record()`
      再按 LSN 回读原始 WAL skeleton
  - 当前根因判断：
    - 这次现场不是“summary 不够好”或“locator-first 没生效”
    - 而是 `locator-only payload stub` fast path 之后，
      payload 收尾仍要顺扫整份 spool
    - 对每个 `locator_only` stub，
      finalize 还会再按 `record_lsn` 回读原始 WAL，
      只为重新累计：
      - kept / target record stats
      - `precomputed_missing_blocks`
    - 结果变成：
      - 先把 payload 写成大量 locator stub
      - 再把这些 stub 全量顺扫一遍
      - 顺扫过程中再按 LSN 二次触发 `XLogReadRecord`
    - 这条链已经把 `locator-only` 节省下来的 payload body 物化收益又吃回去一大截
  - `/proc/<pid>/io` 证据：
    - 第一次冷缓存 payload 期采样到：
      - `write_bytes` 约 `522 MB`
      - query 结束后 `read_bytes` 从约 `375 MB`
        增到约 `1182 MB`
      - 说明 payload 后段主要在回读，而不是继续写 payload spool
    - 热缓存复跑时，8 秒内：
      - `rchar` 从约 `20.19 GB` 增到约 `23.03 GB`
      - `syscr` 从约 `3654 万` 增到约 `4150 万`
      - `read_bytes` 基本不变
      - 说明热点已变成缓存命中的高频 spool/WAL reread
  - 当前结论：
    - 这次 `3/9 payload` 的真实热点已经收口为：
      `locator-only finalize reread`
    - 下一轮优化要打的不是 summary 覆盖率，
      而是让 `locator-only` 路径也能像之前的并行 payload merge 一样，
      避免最终再次落回 `fb_wal_finalize_record_stats()`
  - 当前已完成第一阶段优化：
    - 保留 `locator-only stub` 作为最终 `record_log`
    - 没再沿用“直接把 locator 改写成 deferred skeleton record”的错误实现
    - 改为新增并行 `locator stats sidecar`：
      - worker 第一次按 locator 读取原始 WAL 时，
        直接产出轻量 `payload stats` sidecar
        与 `missing-block` sidecar
      - leader 合并 sidecar 后，直接恢复：
        - kept / target DML 统计
        - `precomputed_missing_blocks`
      - 纯 `locator-only` 场景不再落回
        `fb_wal_finalize_record_stats()`
  - 当前验证状态：
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config src/fb_wal.o` 通过
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config pg_flashback.so` 通过
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config install` 通过
    - 活体 `gdb` 已确认 payload 期 leader 不再停在
      `fb_wal_finalize_record_stats()`，
      而是在等待并行 payload worker 回传 sidecar
    - 同一现场 SQL 复跑结果：
      - 改造前热缓存：
        - `3/9 100% payload` 约 `23.7s-25.6s`
      - 改造后热缓存：
        - `3/9 100% payload` 约 `19.0s-21.1s`
      - 改造后冷样本：
        - `3/9 100% payload` `+57528.205 ms`
        - 相比最初现场 `+70520.220 ms`
          下降约 `12.99s`，约 `18.4%`
  - 当前边界：
    - 本轮只收口了
      `payload_locator_fallback_base_count == 0`
      的纯 `locator-only / 100% summary` 场景
    - mixed `locator + fallback windows`
      仍保留原 finalize 兜底语义
  - 继续沿同一现场做冷跑剖面后，新的主热点已收口为：
    - payload worker 冷读原始 WAL 本身，而不是 leader 收尾
    - 活体抓栈已确认 worker 卡在：
      - `pread64()`
      - `WALRead()`
      - `fb_wal_read_page()`
      - `XLogReadRecord()`
      - `fb_wal_visit_window_batch()`
    - 冷跑采样时，worker 实际磁盘读量已明显偏大：
      - worker `1244292`：
        - `read_bytes = 2176782336`
        - `window_count = 53`
      - worker `1244297`：
        - `read_bytes = 3583311872`
        - `window_count = 53`
      - 说明当前冷跑主成本已经转成 payload worker 的大体量 WAL 冷读
  - 当前代码级 root cause 判断：
    - `fb_build_payload_locator_visit_windows()` 当前按
      `gap_limit = max(wal_seg_size / 8, XLOG_BLCKSZ * 8)`
      合并 locator run，见 `src/fb_wal.c`
    - 但 run 一旦形成，`read_end_lsn` 会直接扩到 run 末 segment 尾
    - 后续 `fb_wal_prepare_payload_read_window()` 又把
      `start_lsn` 直接回退到 run 首 segment 头，
      必要时再额外借前一整个 segment
    - 结果是：
      - locator-run 的实际读取粒度接近“整段/整组 segment 扫描”
      - 不是“围绕 locator 的局部精确读取”
      - 如果同一 segment 上有多个 window，
        worker 还会多次从 segment 头重新找首条 record
      - 并行分片当前按 `window_count` 平分，
        不是按 segment/byte locality 分，
        冷跑时容易把同一 segment 的读取成本放大
  - 下一步：
    - 冷跑优先的下一轮优化应先打：
      - locator visit window 的读取粒度
      - locator 并行任务的 segment/locality 分片
    - mixed `locator + fallback` 的 finalize 收口暂后置
  - 2026-04-11 新约束已拍板：
    - locator 路径严禁再通过 `fb_wal_prepare_payload_read_window()`
      把 `start_lsn` 回退到 run 首 segment 头
    - locator 路径严禁再为 decode 目的额外借前一整个 segment
    - locator 合并若存在，也必须严格由 summary locator 驱动，
      不得退化成“按整段 WAL 扩窗读取”
  - 按上述约束完成实现后，冷跑复核结果：
    - locator 路径已改成 strict summary-exact read：
      - 不再经过 `fb_wal_prepare_payload_read_window()`
      - 不再在 locator worker 路径调用 `fb_wal_visit_window_batch()`
    - locator stats worker 分片也已改成：
      - 按 summary locator 数量切分
      - 尽量在 segment 边界收口
      - 不再按 merged window 数量均分
    - 当前冷缓存现场 SQL：
      - `3/9 100% payload` 约 `+61948.428 ms`
  - 这轮后的新热点已确认收口为：
    - exact locator 冷读原始 WAL：
      - 活体 worker 栈：
        - `pread64()`
        - `WALRead()`
        - `fb_wal_read_page()`
        - `XLogReadRecord()`
        - `fb_wal_visit_payload_locators_exact()`
      - 分片不均衡已明显改善：
        - 旧样本约 `1605499 / 717750 / 248227`
        - 新样本约 `732270 / 726796 / 726521 / 729119`
      - 但单 worker 仍有约 `0.6GB ~ 1.3GB`
        实际冷读
    - `precomputed_missing_blocks` 的 hash 构建：
      - 活体 worker 栈已落到：
        - `element_alloc()`
        - `hash_search(HASH_ENTER)`
        - `fb_wal_note_precomputed_missing_blocks()`
      - 说明在 strict locator 路径下，
        missing-block 记账已经成为新的 CPU/内存热点
  - 下一步：
    - 冷跑优先的下一轮优化应改成：
      - exact locator 冷读的去重/页缓存复用
      - `precomputed_missing_blocks` 的 hash 插入成本

- 2026-04-11 已修复 unresolved xid 调试口径误报：
  - 现场 root cause 已收口为 `fb_wal_xid_debug_fallback_reason()` 优先级错误：
    - 旧逻辑先判断 `!all_assignment_found`
    - 即使同一 xid 已经满足：
      - `resolved_by = exact_all`
      - `all_outcome_found = true`
      - `span_outcome_found = false`
    - 仍会被误报成 `summary_missing_assignment`
  - 当前修复已落地：
    - `src/fb_wal.c`
      - 先识别并返回：
        - `summary_outcome_outside_span`
        - `summary_assignment_outside_span`
      - 只有在全局 summary 里也确实找不到 assignment 时
        才继续返回 `summary_missing_assignment`
    - `sql/fb_unresolved_xid_debug.sql`
      - 已补最小回归，锁住：
        - `resolved_by = exact_all`
        - `diag` 含 `all_outcome_found=true`
        - 不得再报 `summary_missing_assignment`
  - 当前验证状态：
    - `PG14` 下已完成重新编译与安装
    - 由于本机现有 14pg 集群上的 `fb_summary_build_available_debug()`
      现场执行时间异常长，runtime 复核还在继续收口
  - 下一步：
    - 在干净 summary 现场补完这条回归的 runtime 复核
    - 继续观察是否还存在真正的 `summary_missing_assignment` 案例

- 2026-04-11 已完成 14pg external summary progress 时间戳为空修复：
  - 已确认不是 `pg_flashback_summary_progress` 视图读值问题
  - 现场 root cause 已收口为 standalone build chain：
    - `make clean` 之前不会删除 `summaryd/*.o` / `summaryd/vendor/*.o`
    - 14pg setup 可继续复用其他 PG 版本残留的 standalone vendor 对象
    - 结果是 daemon 侧 `XLogReaderState` 布局与当前 PG14 头文件口径错配
    - 进一步表现为：
      - `summaryd/fb_summaryd_standalone_shim.c`
        的首条 record 兼容扫描在 `memcpy(state->readBuf, ...)`
        处崩溃或读到空 summary
      - `pg_flashback_summary_progress`
        持续不显示 `stable_oldest_ts/stable_newest_ts`
  - 当前修复已落地：
    - `Makefile`
      - `clean` 已补齐 standalone summaryd binary / object 清理
      - PG14 build 已切换到匹配的 vendor 源：
        - `summaryd/vendor/xlogreader_pg14.c`
        - `summaryd/vendor/xactdesc_pg14.c`
        - `summaryd/vendor/dynahash_pg14.c`
    - `summaryd/fb_summaryd_standalone_shim.c`
      - 已补 PG18 `FileReadV/FileWriteV` 兼容实现
      - 保持 PG14-17 继续走本地 `FileRead/FileWrite`
    - `tests/release_gate/bin/selftest.sh`
      - 已锁住：
        - `make clean` 必须清 standalone 产物
        - PG14 build 必须命中 PG14 vendor 源
        - PG18 build 继续命中当前 vendor 源
  - 当前验证已完成：
    - `make PG_CONFIG=/home/14pg/local/bin/pg_config clean && make ... summaryd/pg_flashback-summaryd`
      通过
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config clean && make ... summaryd/pg_flashback-summaryd`
      通过
    - 14pg 真环境前台执行
      `./summaryd/pg_flashback-summaryd --config ... --foreground --once`
      后：
      - `meta/summary/` 已落出非空 summary 文件
      - 现场抽样 summary 大小恢复到 `~0.5MB-1.2MB`
      - `352 bytes` 空 header 文件数为 `0`
      - `pg_flashback_summary_progress`
        已显示：
        - `stable_oldest_ts = 2026-04-11 03:39:10.584043-04`
        - `stable_newest_ts = 2026-04-11 04:25:19.338981-04`
  - 下一步：
    - 用 14pg 再走一轮 watchdog / bootstrap setup 真实安装路径，确认同口径二进制也稳定恢复

- 2026-04-11 已完成 14pg 一键安装 `CREATE EXTENSION` 缺符号修复：
  - 现场 root cause 已收口为 bootstrap 构建阶段缺少强制 clean：
    - `scripts/b_pg_flashback.sh` 原先只执行 `make && make install`
    - 当工作树残留旧对象文件 / 旧 `pg_flashback.so` 时
      可能把新的 `sql/pg_flashback--0.2.2.sql`
      和旧二进制一起安装到 `/home/14pg/local`
    - 最终在 `CREATE EXTENSION pg_flashback`
      执行到
      `pg_flashback_debug_unresolv_xid(regclass, timestamptz)`
      时命中
      `could not find function "pg_flashback_debug_unresolv_xid" in file .../pg_flashback.so`
  - 当前修复已落地：
    - `scripts/b_pg_flashback.sh`
      在 setup 的 `Build / Install` 阶段固定先执行
      `make clean`
      再执行 `make` / `make install`
    - 同时继续追到 14pg 真实 root cause：
      - 磁盘上的 `/home/14pg/local/lib/postgresql/pg_flashback.so`
        已经导出
        `pg_flashback_debug_unresolv_xid`
      - 但 `SHOW shared_preload_libraries;`
        仍返回 `pg_flashback`
      - 说明 postmaster 还持有旧版预加载库映像；
        未重启时即使磁盘 `.so` 已更新，
        `CREATE EXTENSION` 仍会命中旧映像并报缺符号
    - `scripts/b_pg_flashback.sh`
      已新增 legacy preload 收口：
      - 若检测到 `shared_preload_libraries`
        仍包含 `pg_flashback`
      - 自动重写 `postgresql.conf` /
        `postgresql.auto.conf` 中的
        `shared_preload_libraries`
        去掉 `pg_flashback`
      - 自动执行一次
        `pg_ctl -D "$PGDATA" restart -m fast -w`
      - 重启后再次校验 preload 已不再包含
        `pg_flashback`
      - 再继续执行 `CREATE EXTENSION` / `ALTER EXTENSION UPDATE`
    - `tests/release_gate/bin/selftest.sh`
      已新增顺序断言，锁住 bootstrap 构建顺序必须为：
      `make clean -> make -> make install`
      并锁住 legacy preload 处理入口仍在
    - `README.md` / `open_source/pg_flashback/README.md`
      已补：
      - bootstrap 会自动移除 legacy preload 并重启
      - 手工安装需要先 `make clean`
      - 若仍有 `shared_preload_libraries = 'pg_flashback'`
        需先移除并重启
  - 下一步：
    - 用 14pg 重新执行一次 bootstrap setup，确认
      “legacy preload + 旧构建产物” 组合现场也不会再报缺符号

- 2026-04-11 已完成 summary watchdog / cron 自恢复模型收口：
  - 已拍板：
    - `scripts/pg_flashback_summary.sh` 的对外控制面只保留：
      - `start`
      - `stop`
      - `status`
    - runner 不再要求用户手工传 `--config` / `--pid-file` /
      `--log-file` / `--summaryd-bin`
    - bootstrap 固定写：
      - `~/.config/pg_flashback/pg_flashback-summaryd.conf`
    - runner 固定管理：
      - `~/.config/pg_flashback/pg_flashback-summaryd.watchdog.pid`
      - `~/.config/pg_flashback/pg_flashback-summaryd.pid`
      - `~/.config/pg_flashback/pg_flashback-summaryd.log`
    - `start` 启动的不是单个 child daemon，而是 shell watchdog：
      - watchdog 每 `1s` 检查 `summaryd` child
      - child 丢失则自动拉起
    - `scripts/b_pg_flashback.sh setup`
      额外安装当前数据库 OS 用户的 `crontab`
      保活项：
      - `* * * * * scripts/pg_flashback_summary.sh start`
      - `cron` 只负责保证 watchdog 存在
      - `1s` 级自恢复仍由 watchdog 承担
    - `scripts/b_pg_flashback.sh --remove`
      需要同时清理：
      - watchdog
      - child daemon
      - cron entry
      - config / pid / log
    - `summaryd` 遇到“缺 WAL / decode-tail 缺段 / 打开候选段命中 `ENOENT`”
      一类现场时，语义收口为：
      - 记录等待态
      - 保持进程存活
      - 等下一轮重试
      - 不允许把服务生命周期交给异常退出
  - 当前文档已补：
    - `docs/specs/2026-04-11-summaryd-watchdog-and-cron-design.md`
    - `docs/decisions/ADR-0036-summaryd-watchdog-and-cron-supervision.md`
    - `docs/architecture/overview.md`
  - 当前实现已补：
    - `scripts/pg_flashback_summary.sh`
      已切到固定路径 watchdog 模型，并完成 child 自恢复
    - `scripts/b_pg_flashback.sh`
      已改成调用固定 `start/stop/status`
      并安装/清理 per-user cron block
    - `summaryd/fb_summaryd_core.c`
      已将缺 decode-tail segment / `ENOENT` 缺段现场
      收口为 waitable error，保持 `service_enabled=true`
    - `README.md` / `summaryd/README.md`
      已移除 `run-once` / runner `--config` 旧用户面
    - `README.md` / `open_source/pg_flashback/README.md`
      安装章节已按当前真实交付面拆成：
      - `一键安装`
      - `用户自定义手动安装`
      - 手动安装明确按 bootstrap 实际步骤展开：
        `build/install -> extension create/update -> archive_dest -> fixed config -> watchdog -> cron keepalive`
    - `Makefile`
      的 `check-summaryd` 已纳入：
      - `missing_segment_race_smoke`
      - `bootstrap_cron_smoke`
  - 验证：
    - `bash tests/summaryd/summary_runner_smoke.sh`
    - `bash tests/summaryd/bootstrap_help_smoke.sh`
    - `bash tests/summaryd/bootstrap_manual_runner_smoke.sh`
    - `bash tests/summaryd/bootstrap_cron_smoke.sh`
    - `bash tests/summaryd/readme_surface_smoke.sh`
    - `bash tests/summaryd/missing_segment_race_smoke.sh`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
  - 下一步：
    - 继续观察真实 `archive_dest` 清理窗口下
      watchdog + cron 长跑稳定性
    - 若后续再出现其他 waitable WAL 缺口，
      继续细化 waitable error 分类而不是退回服务失败态

- 2026-04-11 已完成 standalone `summaryd` 自行消失问题修复：
  - 现场结论已确认：
    - `/home/18pg/data` 与 `/isoTest/18pgdata` 是同一 `PGDATA`
    - `summaryd` 并非被手工停止
    - `coredumpctl` 已确认 `pg_flashback-summaryd` 在 `2026-04-11 09:09:08 CST`
      发生 `SIGABRT`
    - root cause 为：
      `fb_summary_collect_build_candidates()`
      -> `fb_summary_collect_selected_segments()`
      -> `fb_summary_read_wal_segment_size_from_path()`
      在“目录扫描已发现、真正打开时 segment 已失踪”的窗口内抛出未兜底 `ERROR`
  - 当前修复已落地：
    - `src/fb_summary.c`
      已将候选段 `AllocateFile()` 命中 `ENOENT`
      收口为“忽略本轮失踪 candidate”，不再直接打断整个 sweep
    - `summaryd/fb_summaryd_core.c`
      已为 standalone candidate collection / cleanup 路径补 `PG_TRY`
      防止再因收集阶段异常导致进程直接 `abort`
    - 新增 `tests/summaryd/missing_segment_race_smoke.sh`
      用 `LD_PRELOAD` 稳定模拟“segment 扫描后失踪”现场，并锁住
      `summaryd --once` 仍可成功发布 `state.json`
  - 验证：
    - `bash tests/summaryd/missing_segment_race_smoke.sh`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
    - `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_summary_service fb_summary_daemon_state'`
      - `All 2 tests passed.`
  - 下一步：
    - 继续观察真实 `archive_dest` 清理窗口下的 `summaryd` 长跑稳定性
    - 若后续再出现 standalone `last_error` 冻结现场，继续补更细的 error 分类与状态观测

- 2026-04-11 已完成 unresolved xid 调试入口与 replay 错误面增强：
  - 已完成：
    - 新增公开安装函数：
      - `pg_flashback_debug_unresolv_xid(regclass, timestamptz)`
    - 当前版本已前滚到：
      - `0.2.2`
    - 新函数当前返回结构化 xid 级诊断列：
      - `xid`
      - `xid_role`
      - `resolved_by`
      - `fallback_reason`
      - `top_xid`
      - `commit_ts`
      - `summary_missing_segments`
      - `fallback_windows`
      - `diag`
    - `src/fb_replay.c` 当前已为 replay 失败补统一稳定键值：
      - `phase`
      - `recidx`
      - `record_kind`
      - `xid`
      - `lsn`
      - `end_lsn`
      - `rel`
      - `fork`
      - `blk`
      - `off`
      - `toast`
      - `has_image`
      - `apply_image`
      - `has_data`
      - `init_page`
    - 已补回归：
      - `fb_unresolved_xid_debug`
      - `fb_user_surface`
      - `fb_extension_upgrade`
      - `fb_wal_error_surface`
  - 验证：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config clean`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
    - `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_user_surface fb_extension_upgrade fb_wal_error_surface fb_unresolved_xid_debug'`
      - `All 4 tests passed.`
  - 下一步：
    - 继续把 `pg_flashback_debug_unresolv_xid(...)` 的 `fallback_reason`
      从当前 summary/assignment 近似分类，收敛到更细的 root-cause 口径
    - 若后续再出现真实 `failed to replay ...` 现场，
      继续补齐更多 record 类型的统一错误详情字段

- 2026-04-11 已完成 summary runner 脚本化与 recent-first 调度收口：
  - 已完成：
    - 新增 `scripts/pg_flashback_summary.sh`，固定提供：
      - `start`
      - `stop`
      - `status`
      - `run-once`
    - `scripts/b_pg_flashback.sh`
      已完全去掉 bootstrap 主路径上的 `service/systemd` 生命周期管理，
      改为只写 config 并调用脚本 runner
    - bootstrap 当前固定约束为：
      - 非 root
      - 当前 OS 用户必须拥有 `PGDATA`
      - config / pid / log 固定落到
        `~/.config/pg_flashback/`
    - `summaryd/fb_summaryd_core.c`
      与 `src/fb_summary_service.c`
      已统一切到 `archive_dest + pg_wal` 合并候选后的 recent-first
    - 已删除当前交付面中不再保留的：
      - `summaryd/pg_flashback-summaryd.service`
      - `tests/summaryd/bootstrap_service_scope_smoke.sh`
      - `tests/summaryd/bootstrap_no_user_bus_smoke.sh`
  - 验证：
    - `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS=fb_summary_service`
    - `PG_CONFIG=/home/18pg/local/bin/pg_config make check-summaryd`
    - `bash scripts/sync_open_source.sh`
    - 本机 `18pg` runner 实机验证：
      - `run-once` 使用 `pg_wal` recent-tail 临时 config 可成功退出
      - `start/status/stop` 可成功返回
  - 下一步：
    - 继续在 deep / release 场景观察 summary recent-first 下的追平速度与 ETA 口径

- 2026-04-10 已完成 bootstrap 无 user bus 场景修复：
  - 当前已确认 root cause：
    - 在 `CentOS 8/9` 默认 `user scope` 路径下，
      `scripts/b_pg_flashback.sh`
      即使没有可用的 user bus，
      仍会继续执行 `systemctl --user`
    - 本机 `18pg` 当前现场为：
      - `Linger=yes`
      - `XDG_RUNTIME_DIR` 为空
      - `/run/user/<uid>/bus` 不存在
      - setup 在 `Service Setup` 阶段稳定报
        `Failed to connect to bus: No such file or directory`
  - 当前修复已完成：
    - 新增 `user systemd` 可用性探测
    - 默认 `user scope` 且无可用 user bus 时，
      自动回落到 `direct manager`
    - direct manager 当前直接拉起：
      - `pg_flashback-summaryd --config ... --foreground`
    - 已补齐：
      - direct manager 的“已初始化完成”判定
      - direct manager 的 setup 就绪等待
      - direct manager 的 remove 进程清理与 pid/log 清理
    - README / `summaryd/README.md`
      已补齐无 user bus 自动回落说明
    - 新增回归：
      - `tests/summaryd/bootstrap_no_user_bus_smoke.sh`
  - 当前验证已完成：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
    - 本机 `18pg` 真实 setup/remove 验证：
      - setup 输出 `service_manager=direct`
      - 已生成
        `PGDATA/pg_flashback/meta/summaryd/{state.json,debug.json}`
      - 造 WAL + `pg_switch_wal()` 后，
        `PGDATA/pg_flashback/meta/summary/`
        已生成 `66` 个 `summary-*.meta`
      - remove 后已确认：
        - 不残留 `pg_flashback-summaryd` 进程
        - `/home/18pg/.config/pg_flashback/pg_flashback-summaryd.conf`
          已删除
        - `/home/18pg/.config/pg_flashback/pg_flashback-summaryd.pid`
          已删除
  - 下一步：
    - 继续保持 `systemd manager` / `direct manager`
      两条 bootstrap 路径的 smoke 与实机验证口径一致

- 2026-04-10 已完成 bootstrap `archive_dest` 默认来源修复：
  - 当前已确认 root cause：
    - `scripts/b_pg_flashback.sh`
      在未显式传入 `--archive-dest` 时，
      直接把 `ARCHIVE_DEST` 写成 `${PGDATA}/pg_wal`
    - 这会覆盖 PostgreSQL 已配置的 `archive_command`
      归档目录 autodiscovery
    - 结果是 `pg_flashback_summary_progress`
      只统计 `pg_wal` recent tail，
      而不是完整 archive
  - 当前修复已完成：
    - bootstrap 默认优先查询 PostgreSQL：
      - `archive_library`
      - `archive_command`
    - 当 `archive_command` 可识别为本地归档路径时，
      自动把该目录写入：
      - summaryd config `archive_dest=...`
      - `ALTER DATABASE ... SET pg_flashback.archive_dest = ...`
    - 仅在 autodiscovery 不可用时，
      才回退到 `${PGDATA}/pg_wal`
    - 已新增回归：
      - `tests/summaryd/bootstrap_archive_autodiscovery_smoke.sh`
    - `README.md` / `summaryd/README.md`
      已同步更新默认 archive 来源说明
  - 当前验证已完成：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
    - 本机 `18pg` / `alldb` 真实复核：
      - 修复前：
        - `current_setting('pg_flashback.archive_dest')`
          为 `/isoTest/18pgdata/pg_wal`
        - `pg_wal` 实际只有 `129` 段
        - `pg_flashback_summary_progress`
          显示 `completed_segments=129`
      - 修复后重新 bootstrap：
        - `current_setting('pg_flashback.archive_dest')`
          变为 `/walstorage/18waldata`
        - `debug.json` 中 `archive_dest`
          变为 `/walstorage/18waldata`
        - `pg_flashback_summary_progress`
          变为：
          - `stable_oldest_segno=50177`
          - `stable_newest_segno=51574`
          - `completed_segments=1`
          - `missing_segments=1397`
          - `progress_pct=0.0715307582260372`
      - 已确认当前统计窗口已切到完整 archive，而不再误报为
        `pg_wal` recent tail 的 `129 / 129`
  - 下一步：
    - 继续观察大 backlog archive 下的 summary 追平速度与 ETA 口径

- 2026-04-10 已启动 summary backlog recent-first 调度修复：
  - 当前已确认现场表现：
    - `archive_dest + pg_wal` 候选窗口虽然已合并
    - 但 backlog 很大时，`pg_flashback_summary_progress`
      主要表现为 `first_gap_from_oldest_segno` 向新推进
    - 最新端 `first_gap_from_newest_segno`
      长时间不动，用户感知等价于“先做最远的，再慢慢往近推进”
  - 当前已确认 root cause：
    - 内嵌 `fb_summary_service`
      的 cold backlog enqueue 当前按 oldest-first 填充
    - 外置 `summaryd` standalone core
      当前也按 oldest-first 串行扫描候选
  - 本轮目标固定为：
    - `archive_dest + pg_wal` 合并候选后，统一改成 recent-first
    - 最近段优先进入 summary，避免 backlog 先消化最老端
    - 保持当前并发模型不退化，必要时同步收口 debug / 回归断言

- 2026-04-10 已启动 summary daemon 交付面重构：
  - 当前已确认新要求：
    - 不再保留 bootstrap 中的 `service/systemd` 生命周期管理
    - 用户入口改为脚本式手动启停
    - 需要保留一键：
      - `start`
      - `stop`
      - `status`
      - `run-once`
  - 本轮目标固定为：
    - 完全删除 bootstrap 中的 systemd/service 相关逻辑
    - 保留 `pg_flashback-summaryd` 二进制本体
    - 新增纯脚本式 summary runner
    - setup/remove 改为围绕脚本 runner 管理 config / pid / log / extension

- 2026-04-10 已完成仓库产物清理：
  - 已删除当前口径下永不会被用户使用的构建/运行残留
  - 已清理范围包括：
    - 根仓库中的 `*.o` / `*.so` / 本地二进制 / `results/`
      / `cron_daily_update.log`
      / `summaryd/pg_flashback-summaryd.conf`
      / release gate 单次运行输出
    - 开源镜像中误带入的 `summaryd/*.o`
      / `summaryd/vendor/*.o`
      / `summaryd/pg_flashback-summaryd.conf`
  - 已补齐防回流：
    - `.gitignore` 新增运行生成配置、cron 日志与 release gate 输出忽略
    - `scripts/sync_open_source.sh`
      已改为全镜像删除 `*.o` / `*.so` / `*.bc`
      与生成态 `summaryd/pg_flashback-summaryd.conf`
    - `Makefile` 的 `check-summaryd`
      已新增 `tests/summaryd/open_source_artifacts_smoke.sh`
  - 当前验证已完成：
    - `bash -n tests/summaryd/open_source_artifacts_smoke.sh scripts/sync_open_source.sh`
    - `bash scripts/sync_open_source.sh`
    - `bash tests/summaryd/open_source_artifacts_smoke.sh`
    - 已确认根仓库与 `open_source/pg_flashback/`
      不再残留目标产物

- 2026-04-10 已完成 bootstrap 交付面收口：
  - 根 README / 开源 README 已改为只保留 `scripts/b_pg_flashback.sh`
    的交互式使用方式
  - `scripts/bootstrap_pg_flashback.sh` 已直接改名为
    `scripts/b_pg_flashback.sh`
  - 新脚本当前默认通过交互式提示收集
    `pg_config` / `PGDATA` / `dbname` / `dbuser` / `db-password`
  - 已补重复初始化判定：
    - 若目标环境已经具备同名 service、对应 config/unit 文件、
      扩展已安装到当前版本、且数据库级 `pg_flashback.archive_dest`
      已等于目标值，则再次执行 setup 返回 `already_initialized`
      并直接退出
    - 若只满足其中一部分，则视为半初始化现场，继续按 setup 修复
  - `summaryd/README.md`、开源镜像同步脚本与 smoke test
    已同步切到新脚本名与新入口
  - 当前验证已完成：
    - `bash -n scripts/b_pg_flashback.sh tests/summaryd/bootstrap_help_smoke.sh tests/summaryd/readme_surface_smoke.sh scripts/sync_open_source.sh`
    - `bash tests/summaryd/bootstrap_help_smoke.sh`
    - `bash tests/summaryd/readme_surface_smoke.sh`
    - `bash scripts/sync_open_source.sh`

- 2026-04-10 已启动 bootstrap 交互输入即时校验修复：
  - 当前已确认 root cause：
    - `scripts/b_pg_flashback.sh`
      的 `prompt_value_if_needed()` 只校验“非空”
    - 对 `pg_config` / `PGDATA`
      没有在输入当下执行可执行性/目录存在性校验
    - 因此错误路径会继续进入下一轮 prompt，
      而不是立刻报错退出
  - 本轮目标固定为：
    - 对每个交互输入在可判断的范围内立即校验
    - `pg_config` 错误时，不再继续提示 `PGDATA`
    - `PGDATA` 错误时，不再继续提示数据库参数
  - 当前修复已完成：
    - 新增字段级即时校验：
      - `pg_config` 必须“存在且可执行”
      - `PGDATA` 必须“目录存在”
      - 交互输入的数据库密码不能为空
    - 错误输入当前会在该字段读取后立刻 `die`
      ，不再继续进入下一轮 prompt
    - 已新增回归：
      - `tests/summaryd/bootstrap_prompt_validation_smoke.sh`
    - `Makefile` 的 `check-summaryd`
      已挂入该 smoke test
    - README / 开源 README 已补充“交互输入即时校验”说明

- 2026-04-10 已完成 bootstrap 数据库端口交互修复：
  - 已确认 root cause：
    - `scripts/b_pg_flashback.sh`
      启动时把 `DB_PORT` 预设成 `${PGPORT:-5432}`
    - 导致交互模式不会再提示数据库端口
    - `--remove` 等路径会直接沿用默认 `5432`
      去连接本地 socket
  - 当前修复已完成：
    - 交互模式新增 `Database port` 提示
    - `DB_PORT` 改为仅在交互读取或显式 `--db-port`
      时赋值，不再静默写死

- 2026-04-10 已完成 bootstrap service scope / 启动校验修复：
  - 已确认 root cause：
    - `scripts/b_pg_flashback.sh`
      在 `CentOS 8/9` 上只按平台默认 `user scope`
      ，没有额外核对当前 OS 用户是否拥有目标 `PGDATA`
    - root 下对 `18pg` 集群执行 setup 时，
      会把 config/unit 写到 root 的 user manager，
      再尝试拉起第二个 `summaryd`
    - 若同一 `PGDATA` 已有 `18pg` 自己的 daemon 持有 cluster lock，
      新服务会持续 `auto-restart`
    - 旧脚本又只执行 `enable --now`
      ，没有校验 service 是否真的进入 `active`
      ，最终把失败现场误报成 `bootstrap_result=ok`
  - 当前修复已完成：
    - 新增 `PGDATA` owner 识别：
      - 默认 `user scope` 仅在“当前 OS 用户就是 `PGDATA` owner”时保留
      - 否则自动回落到 `system scope`
    - `system scope` unit 当前统一按 `PGDATA` owner 身份运行，
      不再错误使用当前 shell 用户
    - setup 在 `enable --now` 后会轮询校验 service 是否进入 `active`
      ；未成功时直接报错并附带 `systemctl status`
    - dry-run / setup / remove 输出当前已补 `service_os_user`
      观测字段
    - 已新增 smoke：
    - `tests/summaryd/bootstrap_service_scope_smoke.sh`
        覆盖 foreign-owner `PGDATA` 的 scope 回落
      - 同时锁住“服务未真正 active 时 bootstrap 不得返回 ok”

- 2026-04-10 已完成 bootstrap 非 root 执行约束收口：
  - 当前新增要求已落实：
    - `scripts/b_pg_flashback.sh`
      必须在非 root OS 用户下执行
    - root 运行会在任何 prompt / build / systemd / psql
      动作之前直接报错退出
  - 当前修复已完成：
    - 新增早期 `EUID` 拦截，连 `--help` 也不再允许 root 进入
    - 新增 smoke：
      - `tests/summaryd/bootstrap_nonroot_smoke.sh`
        锁住“root 运行立即失败”
    - 现有 bootstrap smoke
      已统一改为在 `18pg` 用户上下文中执行
    - `README.md` / `summaryd/README.md`
      已补齐“必须以非 root 用户运行”的对外说明

- 2026-04-10 已完成 bootstrap 固定服务名收口：
  - 当前新增要求已落实：
    - `b_pg_flashback.sh` 生成/管理的 service 名称固定为
      `pg_flashback-summaryd`
    - 不再按 `PGDATA basename` 派生不同 service 名
  - 当前修复已完成：
    - 脚本内部固定 `SERVICE_NAME=pg_flashback-summaryd`
    - `setup/remove/status` 输出当前统一使用固定 service 名
    - `--service-name` 覆盖入口已移除
    - `README.md` / `summaryd/README.md`
      已补齐固定 service 名说明
    - `bootstrap_help_smoke`
      已锁住固定 service 名口径

- 2026-04-10 已完成 bootstrap 交互默认值收口：
  - 当前新增要求已落实：
    - 交互式执行 `./b_pg_flashback.sh`
      时，能从环境拿到的字段会先作为 `[默认]` 展示
    - 用户直接回车采用默认，手工输入则覆盖默认
    - `PG_CONFIG_BIN`
      的默认值当前只取 `which pg_config`
      ，不读取环境变量
    - 端口默认值当前只取 `PGPORT`
      （无值时再回落 `5432`）
    - 提示顺序当前固定为：
      - `pg_config`
      - `PGDATA`
      - `dbname`
      - `dbuser`
      - `db-password`
      - `db-port`
  - 当前修复已完成：
    - 新增 env/default 解析：
      - `PGDATA`
      - `PGDATABASE`
      - `PGUSER`
      - `PGPASSWORD`
      - `PGPORT`
      - `PGHOST`
      - `PGFLASHBACK_ARCHIVE_DEST`
      - `PGFLASHBACK_INTERVAL_MS`
      - `PGFLASHBACK_SYSTEMD_SCOPE`
    - 新增 smoke：
      - `tests/summaryd/bootstrap_prompt_defaults_smoke.sh`
        锁住环境默认值与
        `dbuser -> db-password -> db-port`
        提示顺序
    - `README.md` / `summaryd/README.md`
      已补齐默认值来源与提示顺序说明

- 2026-04-10 已完成 bootstrap 启动失败诊断输出补齐：
  - 当前新增要求已落实：
    - 当 `summaryd` service 启动失败时，
      终端输出直接打印可执行的诊断命令
  - 当前修复已完成：
    - 失败输出当前会打印：
      - `systemctl status`
      - `journalctl`
      - 前台 `--foreground` 启动命令
    - `user scope` 失败时，额外打印：
      - `XDG_RUNTIME_DIR`
      - `DBUS_SESSION_BUS_ADDRESS`
        导出命令
    - `tests/summaryd/bootstrap_service_scope_smoke.sh`
      已锁住上述输出

- 2026-04-10 已完成 bootstrap remove 的 legacy daemon 清理修复：
  - 已确认 root cause：
    - `--remove`
      只处理当前固定名 `pg_flashback-summaryd.service`
    - 旧命名规则留下的
      `pg_flashback-summaryd-*.service/.conf`
      与其运行中的 daemon
      不会被覆盖
    - 已安装的 `$(pg_config --bindir)/pg_flashback-summaryd`
      也不会删除
  - 当前修复已完成：
    - remove 当前会扫描同一 `PGDATA`
      下的 legacy `pg_flashback-summaryd*.conf`
    - 对命中的 legacy service：
      - stop
      - disable
      - 删除 unit/config
    - 额外按 `--config <path>` 识别并终止同一 `PGDATA`
      的残留 `pg_flashback-summaryd` 进程
    - remove 当前会删除已安装的
      `$(pg_config --bindir)/pg_flashback-summaryd`
    - 已新增回归：
      - `tests/summaryd/bootstrap_remove_legacy_service_smoke.sh`
    - 新增即时校验：
      - `DB_PORT` 必须是纯数字
    - README / `summaryd/README.md`
      已同步补充端口提示说明
  - 当前验证已完成：
    - `bash tests/summaryd/bootstrap_help_smoke.sh`
    - 已确认 `--remove` 交互输入 `5832`
      后，脚本会把 `-p 5832` 传给 `psql`

- 2026-04-10 已完成 bootstrap 数据目录保留与安全清理收口：
  - 当前行为已改为：
    - `--remove` 不再删除 `PGDATA/pg_flashback`
    - remove 结束时会单独打印保留目录与“需手工删除”的提示块
    - setup 若发现已有 `PGDATA/pg_flashback`
      ，只做受限安全清理：
      - 清空 `runtime/`
      - 清理 `meta/summaryd` 的 stale `state/debug/lock`
      - 清理 `meta/summary` 的临时 `.tmp.*`
      - 保留 `recovered_wal/` 与正式 summary/meta 文件
  - 终端输出当前已改为分阶段展示：
    - `Preflight`
    - `Existing Data Directory Check`
    - `Safe Cleanup`
    - `Build / Install`
    - `Database Changes`
    - `Service Setup`
    - `Retained Data Directory`
  - 当前验证已完成：
    - `bash tests/summaryd/bootstrap_data_dir_safety_smoke.sh`
    - 已确认 remove 保留 `PGDATA/pg_flashback`
      且 setup 只做受限安全清理

- 已确认下一条 summary 主线架构变更：
  - 目标改为把当前 preload/bgworker 形态的 summary 服务外移为库外
    daemon `pg_flashback-summaryd`
  - 最终目标固定为：
    - 不依赖 `shared_preload_libraries`
    - 不要求重启 PostgreSQL
    - 保留完整 summary 体验：
      - 后台预热
      - `pg_flashback_summary_progress`
      - `pg_flashback_summary_service_debug`
      - `meta/summary` 自动清理
  - 当前已记录：
    - 设计文档 `docs/specs/2026-04-09-summary-daemon-without-preload-design.md`
    - 架构决策 `docs/decisions/ADR-0035-external-summary-daemon-without-preload.md`
    - 实施计划 `docs/superpowers/plans/2026-04-09-summary-daemon-no-preload-plan.md`
  - 当前实现切入点固定为：
    - 先新增独立可执行目标 `pg_flashback-summaryd`
    - 先做顶层 `Makefile` 构建/安装入口与 daemon CLI 骨架
    - 再逐步把 summary 状态面从 shared memory 收口到状态文件
  - 当前约束补充为：
    - daemon 默认不连接数据库
    - `PGDATA` 作为实例唯一锚点
    - 开源用户需通过同一条 `make` / `make install` 同时获得扩展与 daemon
  - 2026-04-09 已补一轮 `pg_flashback_summary_progress` 稳定性修复：
    - 根因已确认是 external state 已发布的 snapshot 边界
      与实时 `fb_summary_collect_build_candidates()` 候选集被混算
    - 当前已将“snapshot 边界锚定”收窄为 external state 路径，
      保持 shmem/preload 路径继续按 session-local 候选集统计
    - 已补回归锁住：
      `fb_summary_service`
      中“已发布 snapshot 比实时 WAL 候选多 1 个尾段”时
      `stable_newest_segno` / `missing_segments` 不再跳动
    - 已完成验证：
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_daemon_state fb_summary_service fb_summary_prefilter fb_summary_v3'`
      - `alldb` 现场连续 `SELECT * FROM pg_flashback_summary_progress`
        在人为 `pg_switch_wal()` 扰动后已确认只出现
        `190/1 -> 191/0` 的单次收敛，不再出现用户先前贴出的来回跳动
  - 2026-04-09 晚间已完成下一阶段主改造：
    - daemon 的 `build/cleanup`
      已从 `libpq + debug helper` 形态下沉为真正不依赖数据库连接的 core
    - 当前实现固定为：
      - 保持 query-side / SQL 面不变
      - daemon 直接读取 `PGDATA` / `archive_dest` / `pg_wal`
      - 优先复用现有 `fb_summary.c` 的 build 主链
      - 通过独立 frontend-safe / standalone shim
        提供 runtime path、内存、WAL reader 与 cleanup 所需依赖
      - README / 开源 README 已同步明确写清：
        - 如何编译
        - 编译产物在哪里
        - `PGDATA/pg_flashback/` 目录与产物何时生成
        - daemon 如何启动
  - 2026-04-09 深夜已修复 external daemon ETA / stale state 缺口：
    - 根因已确认有两层：
      - SQL 侧状态解析对空字符串时间戳直接走 `timestamptz_in('')`，
        会把 `pg_flashback_summary_progress`
        / `pg_flashback_summary_service_debug` 直接打成报错
      - standalone daemon 每轮空扫都会丢掉
        `throughput_window_started_at` / `last_build_at`
        / `throughput_window_builds`
        的 sample window，导致即使刚完成一轮 build，
        下一轮 `state.json` 仍会把 ETA 输入清空
    - 当前修复已落地：
      - `src/fb_summary_state.c`
        已把空字符串时间戳解释为“字段缺失/值为 0”
      - `summaryd/fb_summaryd_core.c`
        已将 throughput window 改为跨 iteration 保留，
        仅在窗口过期后重置
      - `summaryd/pg_flashback_summaryd.c`
        已把 state file 发布时间统一改为内部 `TimestampTz`
        -> JSON 字符串的单点格式化，避免混用字符串缓存导致的脏值
      - `sql/fb_summary_daemon_state.sql`
        / `expected/fb_summary_daemon_state.out`
        已补回归锁住：
        - 外部 state 提供有效 throughput window 时 ETA 可见
        - 外部 state 给出空时间戳时视图不再报错，ETA 正确返回 `NULL`
    - 当前验证已完成：
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_daemon_state'`
      - `alldb` 现场复测：
        - daemon state 已正常发布
  - 2026-04-10 清晨已补 daemon 交付路径文档澄清：
    - `README.md` / `summaryd/README.md`
      已明确区分：
      - `make` 后仓库内产物 `summaryd/pg_flashback-summaryd`
      - `make install` 后安装产物
        `$(pg_config --bindir)/pg_flashback-summaryd`
  - 2026-04-10 已追加下一条 summary 视图可观测性改造：
    - 当前缺口已确认：
      - 仅看现有 `service_enabled` /
        `pg_flashback_summary_service_debug`
        仍无法明确分辨“当前是否由 external daemon 供数”
      - 外部 daemon 已启动时，也缺少单独的心跳时间列用于观察实时状态刷新
    - 当前目标固定为：
      - 在 summary 视图中显式暴露状态来源 `external/shmem/none`
      - 显式暴露 external state 是否存在、是否 stale
      - 显式暴露 external daemon 最近一次状态发布时间
  - 2026-04-10 上午已完成 summary 视图 external daemon 可观测性补齐：
    - 当前修复已落地：
      - `pg_flashback_summary_progress`
        / `pg_flashback_summary_service_debug`
        已新增：
        - `state_source`
        - `daemon_state_present`
        - `daemon_state_stale`
        - `daemon_state_published_at`
      - `src/fb_summary_state.c`
        已把 external state file 的 `st_mtime`
        收口为 `published_at`
      - `src/fb_summary_service.c`
        已将 external/shmem/none 作为显式状态来源写入 public SQL 面
      - `src/fb_summary_service.c`
        同时修复了无 preload 场景下
        `fb_summary_cleanup_plan_debug()`
        / `fb_summary_service_cleanup_debug()`
        对空 `fb_summary_service_lock`
        直接取锁导致 backend crash 的缺口
      - 扩展版本已前滚到 `0.2.1`，
        并补齐 `0.2.0 -> 0.2.1` 升级链，
        避免 public 视图签名漂移
      - `README.md` / `summaryd/README.md`
        / 开源 README
        已补外部 daemon 观测字段说明
    - 当前验证已完成：
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_daemon_state fb_summary_service fb_extension_upgrade'`
      - `su - 18pg -c '/home/18pg/local/bin/pg_flashback-summaryd --pgdata /isoTest/18pgdata --archive-dest /isoTest/18pgdata/pg_wal --once --foreground'`
      - `su - 18pg -c "/home/18pg/local/bin/psql -p 5832 -d contrib_regression -x -c \"select service_enabled, state_source, daemon_state_present, daemon_state_stale, daemon_state_published_at, estimated_completion_at from pg_flashback_summary_progress; select service_enabled, launcher_pid, state_source, daemon_state_present, daemon_state_stale, daemon_state_published_at, scan_count, build_count, cleanup_count, last_scan_at from pg_flashback_summary_service_debug;\""`
    - 当前现场结果已确认：
      - `pg_flashback_summary_progress`
        可直接看到
        `service_enabled=t`
        `state_source=external`
        `daemon_state_present=t`
        `daemon_state_stale=f`
      - `pg_flashback_summary_service_debug`
        可直接看到
        `launcher_pid`
        `scan_count/build_count/cleanup_count`
        与 `last_scan_at`
    - 因该改动触及公开 SQL 视图签名，当前要求同步前滚扩展版本并补升级链
          `throughput_window_started_at` / `last_build_at`
        - `pg_flashback_summary_progress`
          已从 `service_enabled=f, estimated_completion_at=NULL`
          恢复为
          `service_enabled=t, estimated_completion_at=2026-04-09 23:53:33.421168+08`
  - 2026-04-10 上午已接受新的交付面需求：
    - 目标固定为：
      - 新增数据库 OS 用户可直接执行的一键 bootstrap 脚本
      - 用户只需提供数据库、账号与密码，
        即可自动完成 build/install、扩展安装升级、summary daemon 注册与启动
    - 当前实现策略已拍板：
      - 采用 `systemd` 形态，不再使用临时 `nohup/pidfile`
      - 按 CentOS 主版本做默认适配：
        - `CentOS 7` 默认 `system scope`
        - `CentOS 8/9` 默认 `user scope`
      - 同时保留 `--systemd-scope=user|system` 显式覆盖
  - 2026-04-10 上午用户继续补充 bootstrap 删除语义：
    - 同一脚本除一键搭建外，还必须提供一键删除
    - 删除动作的最小范围固定为：
      - 关闭并禁用 summary daemon 进程
      - 删除 bootstrap 生成的 systemd unit / config 文件
      - 在目标数据库执行 `DROP EXTENSION pg_flashback`
  - 2026-04-10 上午已完成 bootstrap script + CentOS systemd 适配：
    - 当前实现已落地：
      - 新增 `scripts/bootstrap_pg_flashback.sh`
      - 默认 `setup` 动作会执行：
        - `make` / `make install`
        - `CREATE EXTENSION IF NOT EXISTS pg_flashback`
        - `ALTER EXTENSION pg_flashback UPDATE TO '0.2.1'`
        - 生成 summaryd config / systemd unit
        - 注册并启动 `pg_flashback-summaryd`
      - 新增 `--remove` 动作会执行：
        - `stop + disable` summary daemon
        - 删除 bootstrap 生成的 config / unit 文件
        - `DROP EXTENSION IF EXISTS pg_flashback CASCADE`
      - CentOS 默认适配固定为：
        - `CentOS 7 -> system scope`
        - `CentOS 8/9 -> user scope`
      - 同时支持：
        - `--systemd-scope=user|system`
        - `--dry-run`
        - `--db-password` 或交互式密码输入
      - 开源镜像同步规则已补：
        - `scripts/bootstrap_pg_flashback.sh`
          现会进入 `open_source/pg_flashback/`
    - 当前验证已完成：
      - `bash -n scripts/bootstrap_pg_flashback.sh tests/summaryd/bootstrap_help_smoke.sh`
      - `bash tests/summaryd/bootstrap_help_smoke.sh`
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
      - `scripts/bootstrap_pg_flashback.sh --dry-run --pg-config /home/18pg/local/bin/pg_config --pgdata /isoTest/18pgdata --dbname contrib_regression --dbuser 18pg --db-password secret`
      - `scripts/bootstrap_pg_flashback.sh --dry-run --remove --pg-config /home/18pg/local/bin/pg_config --pgdata /isoTest/18pgdata --dbname contrib_regression --dbuser 18pg --db-password secret`
      - `su - 18pg -c '/root/pg_flashback/scripts/bootstrap_pg_flashback.sh --dry-run --pg-config /home/18pg/local/bin/pg_config --pgdata /isoTest/18pgdata --dbname contrib_regression --dbuser 18pg --db-password secret'`
  - 2026-04-10 上午已补 fresh-cluster bootstrap 缺口并完成真实 setup/remove 手测：
    - 根因已确认有三层：
      - `src/fb_summary.c`
        的 `fb_summary_collect_selected_segments()`
        在 external state 存在、但 session 未显式设置
        `archive_dest/archive_dir` 时，
        会对空目录字符串直接 `pfree()`，
        导致 fresh cluster 查询
        `pg_flashback_summary_progress`
        backend crash
      - bootstrap 旧逻辑只把 `archive_dest`
        写给 daemon config，
        没把查询侧默认 GUC 一并落到数据库，
        导致新会话直接查 progress/debug 视图时
        无法按 daemon 实际归档路径统计
      - `CentOS 8/9 user scope`
        在 `su - 18pg` 场景下，
        若仅有 `/run/user/<uid>/bus`、
        但环境里缺 `DBUS_SESSION_BUS_ADDRESS`，
        `systemctl --user` 会误判不可用
    - 当前修复已落地：
      - `src/fb_summary.c`
        已对 `archive_dir` / `pg_wal_dir` / `recovered_dir`
        的释放改为先判空
      - `scripts/bootstrap_pg_flashback.sh`
        `setup` 现会自动执行
        `ALTER DATABASE ... SET pg_flashback.archive_dest = ...`
      - `scripts/bootstrap_pg_flashback.sh`
        `remove` 现会自动
        `RESET pg_flashback.archive_dest/archive_dir`
      - `scripts/bootstrap_pg_flashback.sh`
        已在 `user scope`
        自动补 `XDG_RUNTIME_DIR`
        与 `DBUS_SESSION_BUS_ADDRESS`
      - `sql/fb_summary_daemon_state.sql`
        / `expected/fb_summary_daemon_state.out`
        已补回归锁住：
        - `RESET archive_dir/archive_dest`
          后 external state 仍可安全读视图
        - external state 场景下
          `state_source/daemon_state_*`
          继续按预期返回
    - 当前验证已完成：
      - `bash tests/summaryd/bootstrap_help_smoke.sh`
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5840 PGUSER=18pg installcheck REGRESS='fb_summary_daemon_state'`
      - fresh PG18 临时集群真实 `setup`：
        - `systemctl --user status pg_flashback-summaryd-bootstrap-temp`
          显示 `active (running)`
        - `pg_flashback_summary_progress`
          返回：
          - `completed_segments = 2`
          - `missing_segments = 0`
          - `progress_pct = 100`
          - `state_source = external`
        - `pg_flashback_summary_service_debug`
          返回：
          - `summary_files = 2`
          - `completed_summaries = 2`
          - `missing_summaries = 0`
      - 同一临时集群真实 `remove`：
        - `systemctl --user status pg_flashback-summaryd-bootstrap-temp`
          返回 `Unit ... could not be found`
        - `/home/18pg/.config/systemd/user/pg_flashback-summaryd-bootstrap-temp.service`
          与
          `/home/18pg/.config/pg_flashback/pg_flashback-summaryd-bootstrap-temp.conf`
          已删除
        - `pg_extension` 中已无 `pg_flashback`
        - `current_setting('pg_flashback.archive_dest', true)` /
          `current_setting('pg_flashback.archive_dir', true)`
          均为空
        - `select * from pg_flashback_summary_progress`
          返回
          `ERROR: relation "pg_flashback_summary_progress" does not exist`

- 已完成修复 PG14 `documents @ 2026-04-09 06:25:40.377546+00`
  的 `failed to replay heap insert`，并已确认 `PG14-18` 全部带上运行时修复：
  - 根因已确认：
    - 失败 relation 为 TOAST `1663/319744/319813 blk=92684`
    - 同块更早有 `HEAP2 PRUNE`，需要先释放 `nowunused` slot
    - pre-PG17 `fb_replay_heap2_prune()` 旧逻辑只推进 `page_lsn`，
      没有执行真实 prune
    - 后续 insert 因页 free space 未释放而误报
      `failed to replay heap insert`
  - 当前修复已落地：
    - `src/fb_replay.c`
      已补齐 pre-PG17 `HEAP2_PRUNE` 的真实 replay
      与 future compose slot release 语义
    - `src/fb_guc.c`
      已移除 string GUC check hook
      对 boot/default `*newval` 的非法释放
    - `src/fb_compat.c`
      已补齐 `< PG15` 的 GUC compat 分配返回路径
      并收敛 `guc_malloc/guc_strdup/guc_free`
      / `shm_mq_send` 版本 guard
    - `src/fb_spool.c`
      已补 `sys/uio.h`
    - `src/fb_replay.c`
      已为 PG17+ 增加
      `heap_execute_freeze_tuple()` 本地兼容实现
  - 当前验证已完成：
    - `PG14-18` clean build/install 全部通过
    - `PG14-18` 运行时 `LOAD 'pg_flashback'` 全部通过
    - `PG14-18`
      `fb_replay_prune_releases_space_debug()`
      全部返回
      `prune_releases_space=true free_before=28 free_after=124 insert1=true insert2=true`
    - PG14 live case
      `documents @ 2026-04-09 06:25:40.377546+00`
      已完整返回 `count = 1950041`
  - 下一步：
    - 把这条 prune 修复补进 release gate / 深测口径，
      避免后续再被旧 TOAST/cleanup 场景回归

- 已完成清理当前 18pg 测试残留，并把 release gate 默认测试口径切到 14pg：
  - 当前脚本默认连接口径已调整为：
    - `FB_RELEASE_GATE_PG_MAJOR=14`
    - `FB_RELEASE_GATE_PSQL=/home/14pg/local/bin/psql`
    - `FB_RELEASE_GATE_CREATEDB=/home/14pg/local/bin/createdb`
    - `FB_RELEASE_GATE_DROPDB=/home/14pg/local/bin/dropdb`
    - `FB_RELEASE_GATE_PGPORT=5432`
    - `FB_RELEASE_GATE_PGUSER=14pg`
    - `FB_RELEASE_GATE_OS_USER=14pg`
  - 归档目录解析口径已调整为：
    - 默认优先取 `/home/<major>pg/wal_arch` 的 realpath
    - 仅当对应链接不存在时，才回退到
      `FB_RELEASE_GATE_ARCHIVE_ROOT/<major>waldata`
    - 因而当前本机将按实际环境解析为：
      - `PG14 -> /isoTest/14waldata`
      - `PG15 -> /isoTest/15wal`
      - `PG16 -> /isoTest/16waldata`
      - `PG17 -> /isoTest/17waldata`
      - `PG18 -> /walstorage/18waldata`
  - 当前变更已落地：
    - `tests/release_gate/bin/common.sh`
      新增按 PG major 派生默认 bin/user/port 的逻辑
    - `tests/release_gate/bin/selftest.sh`
      已同步改为版本感知断言，并用 PG14 默认口径覆盖 fake prepare/matrix 场景
    - `tests/release_gate/README.md`
      已同步更新默认连接参数与 archive 目录解析说明
  - 当前 18pg 残留已清理：
    - 已删除 `alldb`、`contrib_regression` 与全部 `fb_*` / `pgfb_*` 测试数据库
    - 已清空 `tests/release_gate/output/latest/` 历史产物并重建空目录骨架
    - 已清空 `/walstorage/18waldata/*`
    - 已清空 `/isoTest/18pgdata/pg_flashback/{runtime,recovered_wal,meta/summary/*}`
      与 `meta/*.meta`
  - 当前验证已完成：
    - `bash -n tests/release_gate/bin/common.sh tests/release_gate/bin/run_release_gate.sh tests/release_gate/bin/prepare_empty_instance.sh tests/release_gate/bin/run_flashback_matrix.sh tests/release_gate/bin/evaluate_gate.sh tests/release_gate/bin/render_report.sh tests/release_gate/bin/selftest.sh`
    - `bash tests/release_gate/bin/selftest.sh`：`PASS`
  - 当前未做：
    - 尚未执行一次真实 `PG14` release gate；
      当前 `14pg` 已恢复可连，但 gate 现场尚未完整复跑

- 已完成删除 release gate 的 golden baseline / 性能回归评估机制：
  - 当前口径已改为：
    - gate verdict 只由 correctness 决定
    - `gate_elapsed_ms` / `measured_elapsed_ms`
      只记录每个 case 的单次执行耗时
    - 不再依赖 `golden/pg<major>.json`
      或 `thresholds.json` 做性能阻断
  - 当前变更已落地：
    - `tests/release_gate/bin/run_flashback_matrix.sh`
      不再写出 `baseline_key`
    - `tests/release_gate/bin/evaluate_gate.sh`
      已移除 golden baseline / performance status / 阈值判定
    - `tests/release_gate/bin/render_report.sh`
      已删除“性能结论”“未评估与阻塞项”等段落，
      统一改成场景矩阵直接展示单次耗时
    - `tests/release_gate/bin/run_release_gate.sh`
      的 `evaluate_gate` 阶段说明已同步更新
    - `tests/release_gate/README.md`
      与 `tests/release_gate/bin/selftest.sh`
      已同步到新口径
  - 当前验证已完成：
    - `bash -n tests/release_gate/bin/run_release_gate.sh tests/release_gate/bin/run_flashback_matrix.sh tests/release_gate/bin/evaluate_gate.sh tests/release_gate/bin/render_report.sh tests/release_gate/bin/selftest.sh`
    - `bash tests/release_gate/bin/selftest.sh`：`PASS`
    - 已直接重写当前
      `tests/release_gate/output/latest/json/gate_evaluation.json`
      与
      `tests/release_gate/output/latest/reports/release_gate_report.md`
  - 下一步：
    - 若后续需要性能趋势，单独设计新的耗时采集/基准体系，
      不再复用 release gate verdict

- 已完成修复 PG14 / release gate 在
  `PGOPTIONS=-c pg_flashback.memory_limit=6GB`
  下的 backend startup 崩溃：
  - 现场现象：
    - `run_flashback_checks` 首条
      `random_flashback_1.documents`
      query case 尚未真正进入 flashback 主逻辑，
      backend 就在连接启动阶段报
      `free(): invalid pointer`
    - postmaster 日志显示：
      - `server process ... was terminated by signal 6: Aborted`
      - 连接侧表现为
        `server closed the connection unexpectedly`
  - 根因已确认：
    - release gate 会通过
      `PGOPTIONS=-c pg_flashback.memory_limit=6GB`
      把 `pg_flashback.memory_limit` 注入新 backend
    - pre-PG15 兼容层 `fb_guc_malloc_compat()` /
      `fb_guc_strdup_compat()` 旧实现错误使用
      `TopMemoryContext` 分配 string GUC 的 canonical value / extra
    - 但 PG14 核心 `guc.c` 对 string/extra 字段收口使用的是 libc `free()`
    - 两端分配/释放语义不一致，最终在
      `set_string_field()` / `set_extra_field()`
      触发 `free(): invalid pointer`
  - 当前修复已落地：
    - `src/fb_compat.c`
      已把 pre-PG15 的
      `fb_guc_malloc_compat()` /
      `fb_guc_strdup_compat()` /
      `fb_guc_free_compat()`
      统一改成与 PostgreSQL GUC 核心一致的
      `malloc/free` 语义
    - 新增回归
      `sql/fb_guc_startup.sql`
      / `expected/fb_guc_startup.out`
      并纳入 `Makefile`：
      - 用子 `psql` 真实覆盖
        `PGOPTIONS=-cpg_flashback.memory_limit=6GB`
        的新 backend 启动路径
  - 当前验证已完成：
    - RED：
      `PGUSER=14pg make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS='fb_guc_startup'`
      在旧 postmaster 上稳定失败，
      结果为子 `psql` 连接异常中断
    - 安装新二进制后，已按
      `su - 14pg -c '/home/14pg/local/bin/pg_ctl -D /home/14pg/data restart -m fast -w'`
      重启 14pg，
      确保 `shared_preload_libraries` 载入新 `.so`
    - GREEN：
      `PGUSER=14pg make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS='fb_guc_startup'`
      ：`All 1 tests passed.`
    - 手工验证：
      `env PGOPTIONS='-cpg_flashback.memory_limit=6GB' psql ... -c "select current_setting('pg_flashback.memory_limit');"`
      已返回 `6GB`，不再崩溃
    - 现场同款
      `scenario_oa_50t_50000r.documents @ '2026-04-09 06:25:40.377546+00'`
      query 路径已能进入
      `1/9 -> 3/9 metadata`
      阶段，不再在连接启动时被打断

- 已完成修复 `random_flashback_2.users @ 2026-04-08 13:50:06.909167+00`
  的 MVCC snapshot `xmax` 漏判：
  - 已确认根因：
    - 旧实现虽然已经支持 `pg_flashback.target_snapshot`
    - 但 WAL 可见性判定只解析并使用了
      `txid_current_snapshot()::text` 的 `xip_list`
    - 对于“`target snapshot` 当时尚未来得及分配/可见，
      但随后提交且 `commit_ts < target_ts`”的事务，
      若该 xid 不在 `xip_list`、而是落在 `snapshot_xmax` 之后，
      旧逻辑仍会误判成 `committed_before_target`
    - 本次现场失败行
      `id = 5454055463773606426`
      就属于这类 case：
      `amount_value`
      从 truth 的 `126643477504843.69`
      被错误替换成了更新后的 `434383075640069.00`
  - 当前修复已落地：
    - `include/fb_wal.h` / `src/fb_wal.c`
      现会同时解析并保存 `target_snapshot` 的
      `xmin` / `xmax` / `xip_list`
    - WAL record status 与 count-only 判定
      现统一把
      - `xip_list` 中仍 in-progress 的 xid
      - 以及 `xid >= snapshot_xmax`
      视为 target snapshot 时刻不可见
    - `src/fb_guc.c`
      已同步把 `pg_flashback.target_snapshot`
      的说明更新到完整 MVCC 语义
    - 新增最小回归
      `sql/fb_target_snapshot.sql`
      / `expected/fb_target_snapshot.out`
      锁住该类 `snapshot_xmax` correctness
  - 当前验证已完成：
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_target_snapshot fb_flashback_keyed pg_flashback'`
      ：`All 3 tests passed.`
    - 对 `tests/release_gate/output/latest/csv/truth/random_flashback_2__users.csv`
      对应时间点重新导出 flashback CSV 后，
      与 truth 做逐行 CSV 比较：
      - `50021` 行全部一致
      - 失败行 `id=5454055463773606426`
        已恢复为 truth 值 `126643477504843.69`
  - 下一步：
    - 用这版二进制重新执行
      `tests/release_gate/bin/run_flashback_matrix.sh`
      / `run_release_gate.sh --from run_flashback_checks`
      复核剩余 random / dml case 是否还有独立 correctness 缺口

- 已完成修复 `scenario_oa_50t_50000r.documents @ '2026-04-08 13:33:00.700288+00'`
  的 `3/9 55% xact-status` unresolved tail：
  - 当前与下午现场的差异已收敛为：
    - 下午 `summary_xid_fallback=21` 的根因是 stale summary identity，属于 phantom xid
    - 本次 `summary_xid_fallback=110` 的根因不是 phantom xid，而是命中 segment 上已有旧/无效 summary sidecar，
      但 query path 只把它当“summary 已存在”，没有在查询时按需重建
  - 现场已验证：
    - 样本 xid（如 `16218039`）是真实 top-level committed xid，不是 phantom xid
    - 命中 commit segment 的旧 sidecar 版本为 `10`
    - 重建后的 sidecar 版本为 `11`，并且已包含目标 xid outcome
    - 继续用 PG14 live probe 复核样本 xid（`39930` / `40681`）后，
      已确认这批 residual xid 不是 subxid-assignment 缺口：
      - 清走旧 summary 前，probe 会表现为“summary 已存在但 outcome 缺失”
      - 清走旧 summary 后，先表现为 `summary_missing_segments > 0`
      - 用当前代码现建后，同一 xid 已稳定变成
        `unresolved_after_summary=false`
        / `all_outcome_found=true`
      - 说明真实问题就是旧 `v10` sidecar 被继续命中；
        当前 `v11 + query-side rebuild` 路径已经能直接把它修正
  - 当前修复已落地：
    - `src/fb_summary.c`
      在 `fb_summary_cache_get_or_load()` 中对 invalid/missing candidate 增加 query-side 一次性重建并重试加载
    - `FB_SUMMARY_VERSION` 前滚到 `11`，使旧 `v10` sidecar 在 query/build 路径上统一失效
  - 当前复核结果：
    - `pg_temp.fb_summary_xid_resolution_debug(...)` 返回：
      - `summary_hits=23352`
      - `summary_exact_hits=0`
      - `unresolved_touched=0`
      - `unresolved_unsafe=0`
      - `fallback_windows=0`
    - 实际 live query
      `select count(*) from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents", '2026-04-08 13:33:00.700288+00')`
      当前已完整返回：

- 已完成修复 PG14 / release gate
  `scenario_oa_50t_50000r.documents @ '2026-04-09 06:25:40.377546+00'`
  的 `3/9 build record index` 新 residual blocker：
  - 现场已确认：
    - release gate 首次表层停留仍显示在
      `3/9 30% metadata`
    - 但对同一 backend 做 `gdb` 后，真实热点已落在
      `fb_wal_fill_xact_statuses_serial()`
      的 raw WAL fallback，而不是 metadata seed 本身
    - 独立 `fb_summary_xid_resolution_debug(...)` 结果为：
      - `summary_hits=19979`
      - `summary_exact_hits=9`
      - `unresolved_touched=86`
      - `fallback_windows=23`
    - 同 case 的 `fb_recordref_debug(...)` 结果为：
      - `xact_summary_spool_records=0`
      - `xact_summary_spool_hits=0`
      - `summary_xid_fallback=77`
      - `xact_fallback_windows=23`
      - `xact_fallback_covered_segments=836`
  - 当前已确认根因：
    - 本轮 query `metadata_fallback_windows=0`
      ，因此 metadata 期不会顺手产出 query-local `xact_summary_log`
    - summary-first + exact-fill 后仍剩少量 unresolved xid
    - 现有 raw xact fallback 虽已有 worker 方案，但
      `fb_wal_fill_xact_statuses_parallel()` 入口被硬编码
      `return false;`
    - 结果是 PG14 release gate 现场继续对
      `23` 个 fallback windows / `836` 个 covered segments
      做单 backend 串行 `RM_XACT_ID` 扫描
  - 当前修复已落地：
    - 启用 bounded raw xact fallback 的并行入口
    - worker 侧复用 serial xact visitor 语义，继续覆盖
      `COMMIT/ABORT` 与 `XLOG_XACT_ASSIGNMENT`
    - 调度已改成 leader + workers 共同消费 fallback windows，
      不再由 leader 停在 `WaitForBackgroundWorkerShutdown()`
    - worker 启动已改成 all-or-fallback，
      避免 partial launch 留下未覆盖窗口
  - 当前补充确认：
    - live case 已看到 `pg_flashback wal xact` worker 真正进入
      `fb_wal_serial_xact_fill_visitor()` / `WALRead`
    - 说明昨天那版“metadata 阶段顺带产出 xact spool”的优化这次确实没有命中，
      但 residual xact fallback 的并行入口已经真正打通
    - 同时也暴露出并行入口当前还有两个新问题：
      - leader backend 会停在 `WaitForBackgroundWorkerShutdown()`，
        自己不参与 fallback window 扫描；
        当本轮只抢到 1 个动态 worker 时，整体仍接近单线程
      - worker 启动当前允许 partial launch 后继续，
        存在尾部窗口无人消费的覆盖风险
  - 修复过程中还发现一个独立验证期 crash：
    - `fb_wal_fill_xact_statuses_serial()` 的 cleanup 会在早退路径上 `hash_destroy(state.assigned_xids)`
    - 但旧代码直到后段才 `MemSet(&state, 0, sizeof(state))`
    - 当 unresolved sets 已在 summary 阶段被清空时，会带着未初始化的 `state.assigned_xids` 进入 cleanup，
      触发 backend `SIGSEGV`
    - 当前已将 `FbWalSerialXactVisitorState state` 提前零初始化，live count query 不再崩溃
  - 当前验证结果：
    - `fb_recordref_debug('scenario_oa_50t_50000r.documents', '2026-04-09 06:25:40.377546+00')`
      已在 `54.998 s` 返回：
      - `summary_xid_fallback=77`
      - `xact_fallback_windows=23`
      - `xact_fallback_covered_segments=836`
      - `xact_parallel_workers=4`
    - 对 leader backend 抓栈，已确认主 backend 不再等待 worker，
      而是自己也停在 `fb_wal_visit_window() -> WALRead`
    - 按 release gate 同口径
      `PGOPTIONS='-cpg_flashback.memory_limit=6GB'`
      重跑原 SQL：
      - `3/9 30% metadata` `+1607.081 ms`
      - `3/9 55% xact-status` `+31269.176 ms`
      - `3/9 100% payload` `+19249.242 ms`
      - 已继续进入 `4/9 0% replay discover precomputed`
      - 说明原来卡在 `3/9` 的 blocker 已解除
    - 默认 `1GB` 口径下，同 SQL 现会在通过 `3/9` 后进入 preflight 内存上限报错，
      也进一步说明现场问题已从 `3/9` 卡顿切换为后续资源约束

- 当前对外开源发布面已收口到统一口径：
  - 根目录许可证已固定为 `Apache-2.0`
  - 当前首个公开发布版本已前滚到 `0.2.0`
  - 根目录已新增 `VERSION` 作为仓库发布版本源
  - 扩展升级链已补到 `0.1.1 -> 0.2.0`
  - 根目录 `README.md` 已收口为通用 PostgreSQL 扩展风格
  - `scripts/sync_open_source.sh` 已纳入 `LICENSE` / `VERSION`
  - 开源镜像内公开 Markdown 已切到单文件中英双语
  - 双语切换方式固定为 `中文 | English` 锚点跳转
  - 本轮已刷新并复核 `open_source/pg_flashback/`

- 正在收敛 release gate 产物判读与最终报告可读性：
  - 已确认 `tests/release_gate/bin/nohup.out` 中真正的 correctness 失败
    只有 `4` 条：
    - `random_flashback_1.users`
    - `random_flashback_1.meetings`
    - `random_flashback_4.users`
    - `random_flashback_4.approval_tasks`
  - 当前 `tests/release_gate/output/latest/json/truth_manifest.json`
    未携带 `target_snapshot` 字段，而当前脚本源码已经会写入该字段并在
    flashback 执行时注入 `pg_flashback.target_snapshot`
  - 因此当前 `latest` 产物更像是：
    - 未按最新 release gate truth/flashback 脚本重新采集并复跑
    - 或 `output/latest` 仍残留旧版本 truth manifest
  - 现阶段判断：
    - 这批 `failed` 记录不能直接视为“当前 HEAD 仍有未知新 bug”
    - 更接近“未用最新 capture + replay 口径完成复测”
    - 是否仍残留真实 correctness bug，需在重新采集带
      `target_snapshot` 的 truth manifest 后复跑确认
  - 当前同步推进：
    - 重做 `release_gate_report.md`
    - 报告统一改为中文
    - release gate 各阶段产物解耦
    - 报告中显式拆分：
      - correctness failure
      - performance regression
      - missing golden baseline / 未评估
      - `target_snapshot` 覆盖率
      - 测试过程与场景矩阵
  - 新增执行约束：
    - release gate 脚本各阶段必须做到“单步可独立重跑”
    - 任一阶段单独成功执行后：
      - 不能被旧日志追加污染
      - 不能错误复用历史 run 的中间产物
      - 不能让上一步/上一次执行残留改变最终结论
    - 最终报告必须只反映当前这轮有效产物

- 已建立统一文档
  [docs/reports/flashback-failure-fix-log.md](/root/pg_flashback/docs/reports/flashback-failure-fix-log.md)
  用于汇总所有“已修复的闪回失败”案例：
  - 当前口径固定为：
    - 只收录已经定位根因并已完成修复的问题
    - 每条记录固定写：
      - 时间
      - 报错现象
      - 原因
      - 修复方式
    - 后续约束固定为：
      - 每次修复新的 flashback failure / replay failure / flashback crash
      时，必须同步更新这份台账
    - 同步更新 `STATUS.md` / `TODO.md`

- 已确认 release gate `random_flashback_4.documents` 的
  `FATAL: terminating connection due to administrator command`
  现场不是 `pg_flashback()` 自身把连接打崩：
  - 该报错口径对应 postmaster / backend 收到外部管理员级终止
  - 现场没有新增到“query 结束前 backend 进程自崩”的证据
  - 当前将其与本轮 correctness 修复拆开处理：
    - 本轮代码修改不围绕“documents crash”展开
    - 继续把主线收敛在 release gate truth mismatch 的根因修复

- 已确认并修复 release gate `random_flashback_1.users` /
  `random_flashback_1.meetings` 的 truth mismatch 根因：
  - 现场表现：
    - `users` / `meetings` 两条 query case 均能完整跑到
      `[done] total elapsed ...`
    - 但最终 correctness 对比仍报
      `row_count or sha256 mismatch`
  - 根因已确认：
    - 根因不是页级回放损坏、tuple decode 丢字段、也不是 reverse op
      组装错误
    - release gate truth 是在
      `repeatable read` 事务里导出的目标时刻 MVCC snapshot
    - 旧版 `pg_flashback()` 只按 WAL 中记录到的 commit timestamp
      判断
      `committed_before_target` / `committed_after_target`
    - 当某些事务在 target snapshot 时刻仍“对 truth 不可见”，
      但随后很快提交，且其 WAL commit timestamp 仍早于 `target_ts`
      时，旧逻辑会把这些事务误判成 `before_target`
    - 这会把 truth 中本应保留的旧版本错误回滚成更新后的新版本，
      最终表现为 `users` / `meetings` 的 row hash mismatch
  - 当前修复已落地：
    - `src/fb_guc.c` / `include/fb_guc.h`
      新增用户级 GUC：
      `pg_flashback.target_snapshot`
    - `src/fb_wal.c` / `include/fb_wal.h`
      在 WAL record index 中解析
      `txid_current_snapshot()::text`
      的 active xid 列表
    - 对目标 snapshot 中仍处于 in-progress 的 xid：
      - 即使其 commit timestamp 排在 `target_ts` 之前
      - 也不再判为 `committed_before_target`
      - 若其在 `query_now_ts` 前提交，则按 `committed_after_target`
        处理
    - `tests/release_gate/bin/capture_truth_snapshots.sh`
      现在会把 truth capture 事务的
      `txid_current_snapshot()::text`
      一并写入 manifest
    - `tests/release_gate/bin/run_flashback_matrix.sh`
      现在会把 manifest 中的 `target_snapshot`
      通过
      `PGOPTIONS='-c pg_flashback.target_snapshot=...'`
      注入 flashback 查询 / COPY / CTAS-create 路径
  - 当前验证：
    - 手工对 `users` 失败行注入 target snapshot 后，
      已恢复 truth 中的旧值
    - 手工对 `meetings` 失败行注入 target snapshot 后，
      已恢复 truth 中的旧值
    - `bash -n tests/release_gate/bin/capture_truth_snapshots.sh`
      / `bash -n tests/release_gate/bin/run_flashback_matrix.sh`
      已通过
  - 下一步：
    - 重新采集带 `target_snapshot` 的 truth manifest
    - 再复跑 release gate correctness-only 对比，确认整轮 mismatch 清零

- 已确认并修复 release gate `run_flashback_checks` /
  `scenario_oa_50t_50000r.leave_requests`
  中的
  `failed to replay heap multi insert` 根因：
  - 现场稳定复现：
    - `select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."leave_requests", '2026-04-08 01:37:20.067024+00')`
      曾报
      `WARNING: will not overwrite a used ItemId`
      / `ERROR: failed to replay heap multi insert`
  - 根因已确认：
    - prune lookahead 的 future constraints 旧逻辑没有覆盖
      `HEAP2_MULTI_INSERT`，导致 final replay 在带 prune image 的页上
      漏看“未来目标槽位必须可插入”的约束
    - 同时 data prune 的 future guard 旧逻辑把
      future old tuple 过度从 `nowdead/redirected` 中剔除，
      与当前 `fb_replay_get_old_tuple_from_page()` 已支持
      `LP_DEAD` / `LP_REDIRECT` 的读取语义不一致
    - 两者叠加后，final replay 会在某些块上保留错误页状态，
      最终把后续 multi-insert 的目标 offset 留成“已占用”
  - 当前修复已落地：
    - `src/fb_replay.c`
      为 `FB_WAL_RECORD_HEAP2_MULTI_INSERT`
      补齐 future compose 与 same-block future support
    - `src/fb_replay.c`
      保留 `state->page_lsn > record->end_lsn` 的 warm-state hardening
    - `src/fb_replay.c`
      调整 prune future guard：
      - future old tuple 仅阻止 `nowunused`
      - future new slot 会把 `nowdead/redirected` 收敛到 `nowunused`
    - 新增最小回归：
      `sql/fb_replay_prune_future_state.sql`
      / `expected/fb_replay_prune_future_state.out`
      锁住 prune image 后接 `HEAP2_MULTI_INSERT` 的 preserve 判定
  - 当前验证：
    - `SELECT fb_replay_prune_image_preserve_next_multi_insert_debug();`
      返回
      `prune_image_preserve_next_multi_insert=true`
    - 串行复核：
      `set pg_flashback.parallel_workers=0; select count(*) ...leave_requests...`
      返回 `49887`
    - 默认并行复核：
      `select count(*) ...leave_requests...`
      返回 `49887`
    - `select * ...leave_requests... order by id limit 5`
      已稳定返回结果，不再报 `heap multi insert`

- 已完成调整 release gate `run_flashback_checks` 的 correctness 对比时机：
  - 当前已改为每条 flashback case 在结果 CSV 落盘后立刻对比对应 truth
  - 不再等全部 flashback case 完成后，才由最终总评阶段第一次输出 correctness 结果
  - 当前实现保持：
    - `evaluate_gate` 仍保留最终汇总 verdict
    - `run_flashback_matrix.sh` 负责单 case 即时 accuracy 输出
  - 当前验证：
    - `bash tests/release_gate/bin/selftest.sh`
      `PASS`
  - 下一步：
    - 在真实 `run_release_gate.sh --from run_flashback_checks` 现场继续观察 mismatch case 的即时日志可读性

- 已完成修复 release gate `run_flashback_checks` 在 replay preflight 上
  因 `pg_flashback.memory_limit` 默认 `1GB` 过小而直接失败的问题：
  - 2026-04-08 同机复跑首个 case 现场已确认：
    - `3/9 55% xact-status` 已降到约 `1s`
    - `3/9 100% payload` 约 `19s~21s`
    - 随后 replay preflight 报
      `estimated flashback working set exceeds pg_flashback.memory_limit`
      （现场 `estimated=1946827608 bytes` / `limit=1073741824 bytes`）
  - 当前修复口径已落地：
    - 不改扩展内核默认 `memory_limit`
    - 在 release gate `run_flashback_checks` 脚本层补一次性自动容错
    - 命中该特定 preflight 报错时，自动以
      `pg_flashback.memory_limit='6GB'` 重试同一 flashback case
  - 当前实现：
    - `tests/release_gate/bin/run_flashback_matrix.sh`
      新增针对 flashback psql 命令的 stderr 识别与一次性 6GB 重试
    - query / COPY / CTAS 三条 flashback 路径都走同一重试入口
    - 重试命中时会输出显式日志：
      `flashback case ... hit memory_limit preflight; retrying with pg_flashback.memory_limit=6GB`
  - 当前验证：
    - `bash tests/release_gate/bin/selftest.sh`
      已新增并通过最小 RED：
      首次 preflight 报错后，会以 `PGOPTIONS=-c pg_flashback.memory_limit=6GB`
      自动重试
    - 手工复跑
      `./tests/release_gate/bin/run_release_gate.sh --from run_flashback_checks`
      时，首个 query case 已现场确认：
      - 首次 `1GB` preflight 失败后打印 retry 日志
      - 随后 `6GB` 重试继续跑过 replay / reverse apply
      - 最终跑到 `[done] total elapsed 192305.921 ms`

- 已确认并修复 release gate `run_flashback_checks` 首个 case 中
  `3/9 55% xact-status` 的 residual fallback 根因：
  - 现场 `fb_summary_xid_resolution_debug()` 打出的 `21` 个 unresolved xid
    对照当前 archive / `pg_wal` 后，已确认并不存在于当前 WAL 内容中
  - 根因不是 “summary 索引没命中”，而是 `src/fb_summary.c`
    当前 summary file identity 过弱：
    - 旧实现只把 `source_kind + segment_name + file_size`
      编进 `summary-%hash.meta`
    - 当 archive 中同名 16MB 段被新内容复用时，旧 summary 会被继续当作
      当前 WAL 的合法 summary 命中
    - 于是 query 会从 stale summary 中读到 phantom touched xid，
      导致 `summary_xid_fallback` 非零，并白白退回真实 WAL fallback
  - 2026-04-08 已把 summary identity/hash 升级为同时绑定：
    - `source_kind`
    - `segment_name`
    - `file_size`
    - `st_mtime`
    - `st_ctime`
    - WAL long header `xlp_sysid`
  - 同机复测：
    - 新 hash 生效后，`pg_flashback_summary_progress`
      一度从 `100%` 掉到 `25.44%`，证明旧 summary 已被判失效
    - 重建 summary 后，同一 case
      `scenario_oa_50t_50000r.documents @ '2026-04-08 00:38:25.357868+00'`
      的 `fb_recordref_debug()` 已变成：
      - `summary_xid_fallback=0`
      - `xact_fallback_windows=0`
      - `xact_fallback_covered_segments=0`
      - `summary_payload_locator_fallback_segments=0`
    - 手工复跑
      `./tests/release_gate/bin/run_release_gate.sh --from run_flashback_checks`
      首个 case 现在稳定表现为：
      - `3/9 30% metadata` 约 `1.3s~1.6s`
      - `3/9 55% xact-status` 约 `0.99s~1.14s`
      - 已不再出现此前 `27s+ / 44s+` 的 xact fallback 慢路径
  - 当前 release gate 首个 case 的新 blocker 已转为独立问题：
    - `payload` 约 `17s~19s`
    - 随后在 replay preflight 报
      `estimated flashback working set exceeds pg_flashback.memory_limit`

- 已确认并修复 `random_flashback_1.users` /
  release gate `run_flashback_checks` 中的
  `failed to replay heap update` 根因：
  - 现场稳定复现：
    - `scenario_oa_50t_50000r.users @ '2026-04-08 00:38:25.357868+00'`
      报
      `lsn=BA/4472E390 ... failed to replay heap update`
  - 根因已确认：
    - final replay 会复用 warm pass 产出的 block state
    - `BA/3E107190` 这条带 FPI 的 `HEAP2_PRUNE`
      在 final pass 上被
      `fb_replay_prune_image_should_preserve_page()` 错误 short-circuit
    - 旧逻辑的 prune lookahead 只累计“未来 old tuple / new slot”，
      没有把后续 `PRUNE_VACUUM_CLEANUP` 释放出来的 slot
      反向折算回 lookahead
    - 于是 lookahead 误判 prune image 上的 dead slot `2/3/4`
      “未来仍不可插”，把更早 prune image 误判成 `image_ok=false`
    - final replay 最终保留了更早于 prune image 的 pre-cleanup 页状态
      （`maxoff=7` / `heap_free≈840`），而没有切回 prune image
      （`maxoff=5` / `heap_free≈6608`）
    - 后续 `BA/3F119810` cleanup 与 `BA/4472E390` update
      都建立在错误页基线上，最终在 `PageAddItem()` 报错
  - 当前修复已落地：
    - `src/fb_replay.c`
      为 `FB_WAL_RECORD_HEAP2_PRUNE` 的 future compose
      增加“后续 `nowunused` 会释放 future insert slot”语义
    - `src/fb_replay.c`
      同时补一层额外 hardening：
      若 `state->page_lsn > record->end_lsn`，直接禁止 preserve
      更晚 warm state
    - 新增独立回归 `fb_replay_prune_future_state`
  - 当前验证：
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_replay_prune_future_state'`
      `All 1 tests passed.`
    - 同一 `users` live case
      `scenario_oa_50t_50000r.users @ '2026-04-08 00:38:25.357868+00'`
      复跑已完整通过：
      - `[done] total elapsed 5980.581 ms`
      - `count = 50015`

- 已调整 release gate `run_flashback_checks` 的 flashback 脚本默认口径：
  - `tests/release_gate/bin/run_flashback_matrix.sh`
    现对每条 flashback 命令默认注入：
    - `PGOPTIONS='-c pg_flashback.memory_limit=6GB'`
  - `tests/release_gate/bin/common.sh`
    当前默认已改成：
    - `FB_RELEASE_GATE_WARMUP_RUNS=0`
    - `FB_RELEASE_GATE_MEASURED_RUNS=1`
    - 即同一条 flashback case 默认只执行 `1` 次，不再按旧口径执行 `3` 次
  - 当前不再依赖“先按 1GB 试跑，再命中特定 preflight 报错后重试 6GB”
    才拿到 6GB
  - query / COPY / CTAS-create 三条 flashback 路径现在都会在执行前打印实际 SQL：
    - 日志格式：
      `flashback sql [scenario:table:path:runN] ...`
  - 当前验证：
    - `bash tests/release_gate/bin/selftest.sh`
      已通过，覆盖：
      - 默认 `PGOPTIONS=-c pg_flashback.memory_limit=6GB`
      - query/copy/ctas-create 三类 flashback SQL 日志输出
      - 默认不会再出现 `run2` / `run3`

- 已确认并修复 release gate `users` case /
  `fb_summary_segment_lookup_payload_locators_cached()` 中的
  `ERROR: pfree called with invalid pointer` 根因：
  - 现场调用栈已确认炸点在：
    - `src/fb_summary.c`
      `fb_summary_segment_lookup_payload_locators_cached()`
      尾部的 `pfree(positions)`
  - 根因已确认：
    - 该函数第一轮只用 `match_count` 统计“`slice_count > 0` 的匹配 relation”
    - 但旧第二轮实现会在拿到当前 `slice_count` 之前，
      先把输出位置绑定到
      `matched_slices[slice_index]` /
      `matched_counts[slice_index]`
    - 当“最后一个正样本之后”仍存在
      `slice_count = 0` 的匹配 relation 时，
      会在 `slice_index == match_count` 上发生越界写
    - 被写坏的正是后续 scratch chunk 的 header，
      最终在函数尾部 `pfree(positions)` 报 invalid pointer
  - 当前修复已落地：
    - `src/fb_summary.c`
      改成先取临时 `slice/slice_count`，确认 `slice_count > 0`
      后才写入 scratch arrays
    - 同时删除旧的 `positions` k-way merge，
      改成“安全收集正样本 -> 统一拼接 -> sort + deduplicate”
      的 merge 口径
    - 新增最小回归 `fb_summary_payload_locator_merge`
      锁住“正样本后跟零样本”的 multi-match locator merge 场景
  - 当前验证：
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_summary_payload_locator_merge'`
      `All 1 tests passed.`
    - live 现场复跑：
      `copy (select * from pg_flashback(NULL::scenario_oa_50t_50000r.users, '2026-04-08 00:38:25.357868+00') order by id) to stdout`
      当前已完整跑通，`[done] total elapsed 17181.644 ms`
      且不再报 invalid `pfree`

- 已继续深入追查 release gate `run_flashback_checks` 的 `3/9 xact-status`
  尾部慢路径，当前现场已补充到：
  - 2026-04-08 同机手工复跑
    `./tests/release_gate/bin/run_release_gate.sh --from run_flashback_checks`
    时，首个 case 仍稳定表现为：
    - `3/9 30% metadata` 约 `11.4s`
    - `3/9 55% xact-status` 约 `44.7s`
    - `3/9 70% -> 100% payload` 约 `27.9s`
  - 同一 case
    `scenario_oa_50t_50000r.documents @ '2026-04-08 00:38:25.357868+00'`
    的 `fb_recordref_debug()` 当前已确认：
    - `summary_xid_hits=27366`
    - `summary_xid_fallback=21`
    - `summary_xid_exact_hits=0`
    - `summary_xid_exact_segments_read=2087`
    - `xact_fallback_windows=7`
    - `xact_fallback_covered_segments=860`
    - `xact_summary_spool_records=0`
    - `xact_summary_spool_hits=0`
  - 这说明当前现场不是“summary 完全没用上”，而是：
    - relation span / xid summary 已经命中大头
    - 但仍有 `21` 个 xid 在两轮 summary exact lookup 后依旧 unresolved
    - 于是 backend 继续掉回真实 WAL fallback，拖慢 `3/9 55%`
  - 当前已排除的方向：
    - `pg_flashback_summary_progress` 当前显示
      `missing_segments=0`
      / `last_query_summary_ready=t`
      / `last_query_summary_span_fallback_segments=0`
      / `last_query_metadata_fallback_segments=0`
    - 当前不是“summary service 常规缺覆盖”导致的普遍降级
    - 手工构造的“大量 savepoint / 96 subxact” probe 当前可由
      `xact_summary_log` 解出，未直接复现 release gate 的
      `summary_xid_fallback=21`
  - 当前进一步收敛出的可疑点：
    - `summary` 侧 relation touched xid 与 query 侧 WAL 实扫 touched xid
      的口径不一致：
      - `src/fb_summary.c` 当前对 relation touched xid 先取
        `fb_summary_record_xid()`，优先折叠到 `top_xid`
      - `src/fb_wal.c` 当前 `fb_mark_record_xids_touched()` 仍会把
        `raw xid + top_xid` 都放进 touched 集
    - 该不一致尚未证明就是 release gate `21 xid` 的唯一根因，
      但已经确认是现有 summary/xact 口径中的真实缺口
    - regression-only 诊断函数
      `fb_summary_xid_resolution_debug(...)`
      当前在大现场上还会稳定 backend `SIGSEGV`
      （`dynahash.c:963`），导致 unresolved xid 样本暂时无法直接打印
  - 当前修复主线已调整为：
    - 先修复 xid tracking / debug 诊断口径不一致
    - 再继续追具体 unresolved xid 样本
    - 最终补齐 summary 生成侧可能遗漏的 xid outcome 场景，
      直到 release gate 现场不再为少量 unresolved xid 回退 WAL

- 已完成修复无主键 bag residual 历史行首条丢失的问题：
  - 2026-04-07 本机 PG18 / `alldb` 用户现场已复现：
    - `INSERT(2 rows) -> UPDATE a=1 -> DELETE a=1 -> DELETE all -> VACUUM`
    - 回看 `after_insert / after_update / after_delete_a1` 三个时间点时，
      `pg_flashback()` 会漏掉 residual 链表头对应的历史行
  - root cause 已确认：
    - `src/fb_apply_bag.c` 中 `fb_bag_emit_residual()` 在
      `fb_bag_apply_finish_scan()` 把 `residual_cursor` 指向 `entries_head` 后，
      首次发射 residual 时仍先推进到 `all_next`
    - 结果 bag 模式下 residual 链表头永远不发射
    - 当当前表已空且目标历史结果全部依赖 residual 发射时，
      就会表现为“少一行”或“直接空结果”
  - 当前修复已落地：
    - `fb_bag_emit_residual()` 改为显式维护“当前 entry 剩余待发射次数”
    - 只有当前 entry 的 residual 次数耗尽后才推进到下一个 `all_next`
    - 已补最小 SQL 回归：
      - `sql/fb_flashback_bag.sql`
      - `expected/fb_flashback_bag.out`
    - 新回归锁住：
      - 无主键表
      - update + delete 到空表
      - `VACUUM`
      - 多个 target timestamp 均能正确发射 residual 历史行
  - 当前验证已完成：
    - 手工 PG18 复跑用户同类 case：
      - `after_insert` 返回 `2` 行
      - `after_update` 返回 `2` 行
      - `after_delete_a1` 返回 `1` 行
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_bag'`
      `All 1 tests passed.`

- 已启动 `run_flashback_checks` 现场的 `3/9 build record index` 彻底收敛：
  - 2026-04-07 本机 PG18 / `alldb` 现场已手工复跑：
    - `./run_release_gate.sh --from run_flashback_checks`
  - 当前已确认：
    - 最近几轮 `3/9` 优化主要压缩的是 payload / locator / materializer 主链
    - 这些优化并非无效，但新的主耗时已经前移到 query-side 的
      `summary-span` 与 `xact-status` 路径
  - 现场 root cause 已确认分成两段：
    - `summary-span`
      - `fb_build_summary_span_visit_windows()` 当前仍按 segment 逐次 lookup，
        并让 `fb_summary_segment_lookup_spans_cached()` 在 query 时
        重新 `palloc + copy` relation span 数组
      - `3/9 10% -> 30%` 的大抖动当前对应这一段 query-side 现拼现拷
    - `xact-status`
      - `fb_summary_fill_xact_statuses()` 仍先按 source windows 读取 outcome slice
      - 只要 unresolved xid 仍存在，就会继续走
        `fb_build_xact_fallback_visit_windows()` 的 segment 级 WAL fallback
      - 现场 `perf` / `pg_stat_activity` 已确认热点落在
        `fb_wal_fill_xact_statuses_serial -> fb_wal_visit_window -> WalRead`
  - 当前实施路线已拍板：
    - `summary-span` 改成 cache 期 stable public slice，禁止 query 时重复 copy
    - `summary-span` query 侧增加 segment 去重与更早的 window merge
    - `xact-status` 先消费 metadata 期产出的 query-local xact summary spool
    - `xact-status` 在 summary-first 后增加精确补洞路径，优先避免整段 WAL fallback
    - 若仍需 WAL fallback，继续缩到 unresolved xid 驱动，而不是沿用大窗口 segment 扫描
  - 2026-04-07 当前新增收敛已落地：
    - serial metadata 主路径已正式接通 `xact_summary_log`
    - `RM_XACT_ID` 的 commit/abort outcome 现在会在 metadata 扫描期写入
      query-local spool
    - `fb_wal_fill_xact_statuses_serial()` 现改为：
      - 先消费 `xact_summary_log`
      - 仅对 spool 未解出的 xid 再走 summary outcome
      - 仍未解出时才进入 exact-fill / WAL fallback
    - `fb_recordref_debug()` 已新增：
      - `xact_summary_spool_records`
      - `xact_summary_spool_hits`
    - 最小回归 `fb_recordref` 已锁住：
      - 当前夹具下 `xact_summary_spool_hits > 0`
      - `summary_xid_segments_read = 0`
      - `xact_fallback_windows = 0`
  - 本轮完成标准：
    - 直接用 release gate 现场复跑
    - 若 `./run_release_gate.sh --from run_flashback_checks`
      中 `3/9` 无明显缩短，则继续迭代，不以“已完成”收尾
  - 2026-04-07 晚间同机现场已继续确认新的 root cause：
    - `tests/release_gate/bin/run_release_gate.sh --from run_flashback_checks`
      当前首个 query case 仍稳定落在：
      - `3/9 30% metadata` 约 `57s`
      - `3/9 55% xact-status` 约 `31s`
      - `3/9 70% -> 100% payload` 约 `27s`
    - `pg_flashback_summary_progress` 已确认：
      - `missing_segments = 0`
      - `last_query_summary_ready = t`
      - 说明当前现场不是“summary 未覆盖”导致的常规降级
    - 对 `fb_recordref_debug('scenario_oa_50t_50000r.documents', ...)`
      现场 backend 做 `gdb/perf` 已确认：
      - 当前卡点明确在
        `fb_wal_fill_xact_statuses_serial -> fb_wal_visit_window -> fb_wal_read_page`
      - `frame fb_wal_fill_xact_statuses_serial` 局部变量已看到：
        - `window_count = 119543`
        - `fallback_window_count = 7`
        - `remaining_xids = 2`
      - 说明不是“大量 unresolved xid”本身，而是“极少量 unresolved xid`
        仍把 backend 拉回真实 WAL fallback”
    - 当前进一步收敛结论：
      - `xact-status`
        - 问题不再是 summary 全量 outcome slice copy
        - 而是少量 unresolved xid 在 relation span 覆盖外仍需要 outcome 时，
          现路径没有继续做“全局 summary exact lookup”
        - 结果即使 summary 全覆盖，仍会退回 `WalRead`
    - 2026-04-07 深夜同机继续做 live xid 样本反查后，当前更细 root cause
      已进一步收敛到 `summary builder`：
      - 再次用 `fb_recordref_debug('scenario_oa_50t_50000r.documents',
        '2026-04-07 04:41:28.555065+00')` 现场复跑，
        仍看到：
        - `summary_xid_hits = 44405`
        - `summary_xid_exact_hits = 0`
        - `summary_xid_fallback = 54`
        - `xact_fallback_windows = 7`
      - `gdb` 直接遍历 `fb_wal_fill_xact_statuses_serial()` 的
        `fallback_touched` 已取到一批仍需回退的 sample xid：
        - `13296140`
        - `13295781`
        - `13296138`
        - `13295989`
        - `13295990`
        - `13296881`
        - `13296884`
      - 对这批 sample xid 做 `pg_waldump` 直接 grep，没有任何顶层
        `tx:` 命中，说明它们更像是 relation DML 期的 subxid，不是顶层
        commit xid
      - 当前最可疑的不一致点已定位到：
        - `src/fb_summary.c` 在 summary build 期进入 `RM_XACT_ID`
          后，先把 `xl_info` 掩成 `XLOG_XACT_OPMASK`
        - 然后把这个被掩码后的值传给
          `ParseCommitRecord()` / `ParseAbortRecord()`
        - 但 query-side WAL fallback (`src/fb_wal.c`) 传的是完整
          `XLogRecGetInfo(reader)`
      - 当前判断：
        - 这很可能让 summary build 丢掉 commit/abort record 中的
          `subxact` 信息
        - 结果就是 summary xid outcome 只覆盖顶层 xid，
          query-side 仍要为少量 subxid 退回真实 WAL
    - 2026-04-07 深夜这条 root cause 已确认并完成首轮修复：
      - 已补 `fb_summary_xid_resolution_debug()`，把 live case 直接拆成
        “summary-only 不走 raw WAL fallback” 口径
      - live case 在 summary-only 下确实仍留下 unresolved xid，
        说明问题不在“query 没用 summary”，而在“summary 文件没写全”
      - 已补最小 RED：
        - `fb_recordref` 新增 commit-subxact / abort-subxact 两组场景
        - 其中 abort-subxact 在修复前稳定表现为：
          - `summary_hits = 0`
          - `unresolved_touched = 3`
          - `unresolved_unsafe = 1`
          - `fallback_windows = 1`
      - 当前修复已落地：
        - `src/fb_summary.c` 的 `RM_XACT_ID` build 路径改为把完整
          `XLogRecGetInfo(reader)` 传给
          `ParseCommitRecord()` / `ParseAbortRecord()`
        - `FB_SUMMARY_VERSION` 已前滚到 `10`，强制旧 summary 失效并重建
      - 当前验证已完成：
        - abort-subxact RED 修复后已变为：
          - `summary_hits = 4`
          - `unresolved_touched = 0`
          - `unresolved_unsafe = 0`
          - `fallback_windows = 0`
        - `installcheck REGRESS='fb_recordref'`
          `All 1 tests passed.`
      - 对 release gate 实际 archive source (`/walstorage/18waldata`)
        继续复核后，当前现场进一步收敛为：
        - `xact-status` 已不再依赖 raw WAL fallback：
          - `xact_summary_spool_hits = 30432`
          - `summary_xid_hits = 0`
          - `xact_fallback_windows = 0`
        - 但 `metadata` 仍有残余 summary 缺口：
          - `metadata_fallback_windows = 364`
          - `summary_payload_locator_public_builds = 0`
      - 直接复跑
        `tests/release_gate/bin/run_release_gate.sh --from run_flashback_checks`
        的首个 query case，当前 `3/9` 已缩到：
        - `summary-span` `+29.646 ms`
        - `metadata` `+6412.559 ms`
        - `xact-status` `+525.584 ms`
        - `payload` `+516.234 ms`
      - 同机再次复跑同一首个 query case 后，当前进一步确认：
        - `metadata` 可继续降到 `+3229.323 ms`
        - `xact-status` 稳定在 `+417.550 ms`
        - 但 `payload` 会抬到 `+10951.771 ms`
      - 当前剩余主瓶颈已进一步收敛为：
        - `summary payload locator` 没有真正命中 public slice
        - 现场仍看到：
          - `summary_payload_locator_records = 0`
          - `summary_payload_locator_public_builds = 0`
          - `summary_payload_locator_fallback_segments = 364`
        - 说明“所有 summary 驱动步骤 < 5s”当前只剩 payload 一段尚未达标
      - 当前下一步已拍板：
        - 继续沿 payload locator 数据流逐层对账
        - 先分清：
          - summary build 是否真的写了 locator
          - summary cache/query lookup 是否真的读到了 locator public slice
          - release gate 的 archive source 是否在 source identity 上与 summary 文件失配
      - 2026-04-07 深夜沿 archive source identity 继续对账后，当前已确认更细 root cause：
        - `summary build` 本身没有丢 payload locator：
          - 对样本 segment `00000001000000B40000001E`
            离线扫 `.meta` 已确认旧 summary
            `summary-df414f45d29afb05.meta` 内含目标 relation 的
            payload locator
          - 新增单段 debug 后，
            `fb_summary_build_candidate_debug('/walstorage/18waldata/...1E')`
            也已确认该 segment 可以单独成功重建
        - 真正不一致出在 `summary identity`：
          - 默认 `summary service` / progress 口径走的是 PostgreSQL
            `archive_command` 解析出的 `/home/18pg/wal_arch`
          - release gate query session 显式 `SET pg_flashback.archive_dest`
            后走的是 `/walstorage/18waldata`
          - 当前 `fb_summary_file_identity_hash()` 把完整 `path`
            纳入 hash，导致同一 archive segment 在两条路径下生成两份
            不同 summary identity：
            - `/home/18pg/wal_arch/...1E` -> `df414f45d29afb05`
            - `/walstorage/18waldata/...1E` -> `a6a6d66cf85adf72`
        - 已直接复核：
          - 默认 source 口径下：
            - `fb_summary_service_plan_debug()` 对 `segno=46110`
              返回 `summary_exists = true`
            - `fb_summary_candidate_debug('/walstorage/...1E')`
              返回 `candidate = false`
          - 显式 `SET pg_flashback.archive_dest='/walstorage/18waldata'`
            后：
            - 同一 `segno=46110` 变为 `summary_exists = false`
            - payload locator plan 的 archive source lookup
              因此整段退回 fallback
        - 当前判断：
          - 问题不在 payload locator build/cache 主体
          - 而在 summary identity 仍绑定“路径字符串”，不能复用
            等价 archive source 上的同一 WAL segment summary
      - 当前下一步已拍板为：
        - 先补最小 RED，锁住“等价 archive 路径共享同一 summary identity”
        - 再只改 summary identity/hash 契约，不扩大 payload/query 逻辑面
      - 2026-04-07 深夜这条修复已完成首轮落地并通过 live 复核：
        - 已补最小 RED：
          - 新增 `fb_summary_identity`
          - 锁住：
            - 等价 archive 路径生成相同 `summary_path`
            - 先在 archive path A 建 summary 后，archive path B 可直接复用
        - 当前实现已改为：
          - `src/fb_summary.c` 的 summary identity 不再 hash 完整路径
          - 当前统一按：
            - `source_kind`
            - WAL segment basename
            - file size
            生成 summary identity
          - 因而 `/home/18pg/wal_arch/...` 与 `/walstorage/18waldata/...`
            对同一 archive segment 现在会映射到同一 summary 文件
        - 当前验证已完成：
          - 回归：
            - `fb_summary_identity`
            - `fb_summary_service`
            - `fb_payload_scan_mode`
            - `All 3 tests passed.`
          - live 样本：
            - 先在 `/home/18pg/wal_arch/00000001000000B40000001E`
              口径下命中
              `summary-ebb881a28fb1759d.meta`
            - 再切到 `/walstorage/18waldata/00000001000000B40000001E`
              后，`fb_summary_candidate_debug()` 直接返回
              `summary_exists=true`
          - live payload locator plan：
            - 修复前：
              - `base_segments=363`
              - `success_segments=0`
              - `fallback_segments=363`
            - 按新 identity 契约重建后：
              - `base_segments=356`
              - `success_segments=356`
              - `fallback_segments=0`
              - `failed_segments=[]`
        - 当前结论：
          - 此前 release gate archive source 上的 payload locator 漏命中，
            root cause 已确认并修复为“summary identity 绑定路径字符串”
          - 后续主线应回到：
            - 复跑 release gate 现场
            - 继续确认 `summary_payload_locator_public_builds`
              与最终 `3/9 payload` 时延是否一并收敛
      - `payload`
        - `NOTICE` 中 `70% payload` 只是 payload 规划开始，不代表 payload 已完成
        - 真正耗时落在 `70% -> 100%` 的 payload 物化/record spool 阶段
        - 需要继续区分：
          - summary payload locator stub/locator 命中
          - fallback window 物化
          - replay/final 仍必需的 payload body

- 已推进 release gate `4/9 replay warm` 的 `heap delete redo` blocker：
  - 2026-04-07 现场 `scenario_oa_50t_50000r.documents`
    @ `2026-04-07 04:41:28.555065+00`
    的首个稳定报错此前固定为：
    - `failed to locate tuple for heap delete redo`
    - `lsn=B4/3EDE89D8 blk=488294 off=3`
  - 本轮已补一刀 replay 内核修复：
    - `src/fb_replay.c` 的 cross-block `heap update`
      不再依赖 `record.blocks[]` 过滤后的数组位置来认
      “new block / old block”
    - 当前改为优先按 WAL `block_id` 语义取：
      - `block_id = 0` 作为 new block
      - `block_id = 1` 作为 old block
      - 只有缺失时才回退到旧的数组位置兜底
  - 已补最小 debug contract：
    - `fb_replay_heap_update_block_id_contract_debug()`
    - 锁住“即使 `record.blocks[]` 顺序与 WAL block_id 脱钩，
      replay 仍必须把 `new_offnum` 插到 WAL block 0 对应页上”
  - 当前现场复核状态：
    - 独立 `count(*)` live query 已持续运行数分钟，
      未再在原位置复现 `heap delete redo` 报错
    - `./run_release_gate.sh --from run_flashback_checks`
      当前也已重新推进到原 `4/9` 失败点之后，未再第一时间炸回同一错误
  - 当前仍在继续：
    - 等待 release gate 完整跑出新的首个 blocker
    - 若后续不再出现该错误，则把本项从“进行中”转为“已完成”

- 已完成回退 release gate 的 frozen WAL fixture 路径，恢复直接使用 live archive：
  - 2026-04-07 现场复核已确认：
    - frozen fixture 方案会把 flashback 查询切到复制后的 WAL 路径
    - 这会绕开 live archive 上已生成的默认 summary 身份，导致 release gate 不能直接复用现成 summary 索引
  - 当前口径已改为：
    - live archive 被清理的问题按“release gate 脚本自身不得在 truth capture 后误删 archive”处理
    - 不再继续维护 `output/.../wal/frozen` 复制路径
    - `run_flashback_checks` 改回直接依赖实例当前 live archive / `pg_wal` 解析
  - 当前修复已落地：
    - 删除 `fb_release_gate_sync_frozen_wal()` / `fb_release_gate_seal_current_wal_tail()` 及其调用
    - 删除 `capture_truth_snapshots.sh` 中的 frozen WAL 同步
    - 删除 `run_flashback_matrix.sh` 与 `create_flashback_ctas.sql` 对 frozen `archive_dest` 的强制覆盖
    - `tests/release_gate/bin/selftest.sh` 已改为锁住：
      - `run_flashback_matrix.sh` 不再带 `set pg_flashback.archive_dest = ...`
      - matrix 不再调用 `pg_switch_wal()`
      - CTAS 路径不再传 `archive_dest=...`
  - 当前验证已完成：
    - `bash tests/release_gate/bin/selftest.sh`
    - `bash -n tests/release_gate/bin/common.sh tests/release_gate/bin/capture_truth_snapshots.sh tests/release_gate/bin/run_flashback_matrix.sh tests/release_gate/bin/selftest.sh tests/release_gate/bin/prepare_empty_instance.sh tests/release_gate/bin/run_release_gate.sh`
  - 下一步：
    - 用真实 PG18 release gate 现场复跑，确认 summary 命中恢复到默认 live-archive 路径

- 已完成修复 deep/full 测试脚本自动清理 live archive 导致缺 checkpoint/FPI 的问题：
  - 已确认根因不在 flashback 内核，而在 `tests/deep/bin/` 的 round cleanup 策略
  - `full` 模式把 `/isoTest/18waldata` 直接作为 live `archive_dest` 使用时，
    通用 cleanup 与 full 主入口中的显式 `find ... -delete`
    会把缺页补锚仍需依赖的更早 checkpoint / FPI WAL 提前删掉
  - 当前修复已落地：
    - `fb_deep_cleanup_round_artifacts()` 不再清空 live archive
    - `run_all_deep_tests.sh` 不再在 baseline snapshot prepare / batch restore 后主动清空 `/isoTest/18waldata`
    - 若 live archive 模式下因磁盘水位触发 retry，当前改为停止并提示人工清理，避免“自动删档后继续跑”
    - 已补 shell 级 RED，自测锁住：
      - live archive round cleanup 不得误删归档
      - non-live fixture archive 仍会被正常清理
  - 当前验证已完成：
    - `bash tests/deep/bin/test_full_snapshot_resume.sh`
    - `bash -n tests/deep/bin/common.sh tests/deep/bin/run_all_deep_tests.sh tests/deep/bin/test_full_snapshot_resume.sh`
    - `bash tests/deep/bin/run_all_deep_tests.sh --full --dry-run`
  - 下一步：
    - 用真实 `--full` / batch B 现场复跑，确认 shared backscan 能稳定吃到保留下来的更早 WAL
    - TOAST deep/full 结束后，仍按既定运维动作手工清空 `/isoTest/18waldata`

- 已完成修复 release gate 初始化阶段未在新建 `alldb` 后安装 `pg_flashback` 扩展的问题：
  - 已确认根因在 `tests/release_gate/bin/prepare_empty_instance.sh`
  - 旧脚本只执行 `dropdb/createdb`，不会在新库中补 `CREATE EXTENSION pg_flashback`
  - 当前修复已落地：
    - `prepare_empty_instance.sh` 在 `createdb` 后立即执行
      `CREATE EXTENSION IF NOT EXISTS pg_flashback;`
    - `tests/release_gate/bin/selftest.sh` 已补最小 RED，
      锁住 prepare 阶段对新建 `alldb` 的扩展安装动作
    - `tests/release_gate/README.md` 已同步登记 prepare 阶段的新固定动作
  - 当前验证已完成：
    - `bash tests/release_gate/bin/selftest.sh`
    - `bash -n tests/release_gate/bin/prepare_empty_instance.sh tests/release_gate/bin/selftest.sh`

- 已完成修复 release gate 总入口在失败退出后误删归档现场的问题：
  - 已确认问题点在 `tests/release_gate/bin/run_release_gate.sh` 的 `trap cleanup EXIT`
  - 旧脚本只要本轮执行过 `prepare_instance`，退出时就会清空当前版本归档目录
  - 这不会制造本轮 `missing FPI`，但会在 `run_flashback_checks` 失败后抹掉上一轮 truth/WAL 现场，阻断从现有产物继续复跑和排障
  - 当前修复已落地：
    - release gate 总入口退出时仅停止 `alldbsimulator`
    - 不再自动清理当前版本 archive dir
    - `tests/release_gate/bin/selftest.sh` 已补最小 RED，
      锁住“跑过 `prepare_instance` 后退出也不得自动删归档”
    - `tests/release_gate/README.md` 已同步改成“退出不自动清档”的真实口径
  - 当前验证已完成：
    - `bash tests/release_gate/bin/selftest.sh`
    - `bash -n tests/release_gate/bin/run_release_gate.sh tests/release_gate/bin/selftest.sh`
  - 当前说明：
    - 本轮修复的是“失败退出后误删归档现场”
    - 已经出现的 `missing FPI for block 487920` 仍需继续按内核 root cause 追查，不应再被脚本 cleanup 抹现场

- 已完成 `count(*) FROM pg_flashback(...)` 错误计数的止血修复，并将后续工作收口为“重设计 count-only 优化”：
  - 2026-04-06 本机 PG18 / `alldb` 现场先用实际 `pg_waldump` 复核：
    - `scenario_oa_50t_50000r.documents`
      在 `2026-04-04 23:20:13 -> 23:40:13` 期间真实净变化不是 `0`
    - `scenario_oa_50t_50000r.approval_comments`
      在 `2026-04-04 23:00:13 -> 23:40:13` 期间真实净变化也不是 `0`
    - 旧实现却返回一串相同计数，确认问题在扩展内部 `count(*)` 专用执行链路
  - 已补最小 RED：
    - `fb_custom_scan` 新增 “summary 已命中 + target 后同时存在 INSERT/DELETE” 计数回归
    - 旧实现下该回归会出现：
      - `fast_count = current_count`
      - 与 materialize 出来的真值计数不一致
  - 当前已采取的修复策略：
    - 暂时禁用 `FbCountAggScan` 聚合下推
    - 暂时禁用 `FbApplyScan` 的 `count_only_fast_path`
    - `count(*) FROM pg_flashback(...)` 统一回到已验证正确的常规 flashback 执行链
  - 修复后 live 复核：
    - `documents`
      - `2026-04-04 23:40:13` -> `1949853`
      - `2026-04-04 23:39:13` -> `1949853`
      - `2026-04-04 23:30:13` -> `1949855`
      - `2026-04-04 23:20:13` -> `1949887`
    - `approval_comments`
      - `2026-04-04 23:40:13` -> `199826`
      - `2026-04-04 23:20:13` -> `199823`
      - `2026-04-04 23:00:13` -> `199920`
  - 当前遗留：
    - `FB_WAL_BUILD_COUNT_ONLY` / `FbCountAggScan` 的真正 root cause 还没有完成内核级修复
    - 后续要在 correctness 已锁住的前提下，单独重设计并重新开放 count-only 优化

- 已完成 `documents @ 2026-04-04 23:40:13` 的通用 WAL 物化优化收敛：
  - live 现场 `gdb` 已确认：
    - `record_count = 4530229`
    - `summary_payload_locator_records = 4530229`
    - `precomputed_missing_block_count = 0`
    - `payload_scan_mode = LOCATOR`
    - `discover round = 1`
  - `perf` 已确认当前不是卡在 summary anchor lookup，而是卡在
    locator-only stub 的逐条真实物化：
    - `fb_wal_record_cursor_read`
    - `fb_wal_load_record_by_lsn`
    - `fb_wal_open_segment`
    - `fb_open_file_at_path`
    - `posix_fadvise`
  - 当前根因已确认：
    - `3/9` 的 locator-only payload stub 把 payload body 延后后，
      `4/9` discover 仍顺着整个 `record_log` 消费 stub
    - stub 当前只有 `record_start_lsn`
    - cursor 命中 stub 后会逐条新建 `XLogReader`
    - 并逐条重新 `open/fadvise/close` segment
    - 同类浪费也存在于 deferred payload materialize
    - locator-only 路径还会把 `precomputed_missing_blocks` 清零，
      使 discover shortcut 失效
  - 当前通用修复已落地：
    - 新增 reusable WAL record materializer
    - locator-only / deferred payload / replay discover 共用同一物化层
    - 区分顺扫窗口与稀疏按 LSN 物化的 open hint
    - locator-only 路径恢复 `precomputed_missing_blocks` 预计算
    - query-local WAL bgworker 改为先按实例 worker budget 限流，避免反复 register/kill 不可能启动的 worker
  - 2026-04-06 本机 PG18 live 复核：
    - SQL：
      `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
    - 结果：
      - `3/9 30% metadata` `+1211.347 ms`
      - `3/9 55% xact-status` `+0.362 ms`
      - `3/9 100% payload` `+7.348 ms`
      - `4/9 replay discover precomputed` 进入 `< 1 ms`
      - `/usr/bin/time` 总时长 `1.39s`
  - 当前结论：
    - 本轮目标 `< 50s` 已达成
    - `4/9` 不再是 blocker
    - live case 当前主耗时已转移到 `3/9 metadata`

- 已完成 `documents @ 2026-04-04 23:40:13` 的 `SELECT * ... LIMIT 100` live case
  `3/9` 压降：
  - 当前在 `summary payload locator` 已覆盖且 `parallel_workers > 1` 的路径上，
    `3/9` 新增 `locator-only payload stub` fast path：
    - `3/9` 不再为 locator-first 路径全量物化 payload body
    - build 期仅把 `record_start_lsn` 级 stub 落到 record spool
    - cursor / replay 侧按需按 LSN 回填真实 record
  - 2026-04-06 本机 PG18 live 复核：
    - SQL：
      `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
    - 一轮复核：
      - `3/9 30% metadata` `+1303.071 ms`
      - `3/9 55% xact-status` `+15658.182 ms`
      - `3/9 100% payload` `+950.091 ms`
      - `3/9` 累计约 `17.97s`
    - 前一轮热缓存复核：
      - `3/9 30% metadata` `+1318.998 ms`
      - `3/9 55% xact-status` `+15039.466 ms`
      - `3/9 100% payload` `+1061.092 ms`
      - `3/9` 累计约 `17.47s`
  - 当前确认：
    - `100% payload` 已从此前三十秒级压到约 `1s`
    - `3/9` 当前主瓶颈重新收敛为 `xact-status`
  - 当前残留：
    - `PGHOST=/tmp PGPORT=5832 make installcheck REGRESS='fb_recordref fb_replay'`
      中 `fb_recordref` 已通过
    - `fb_replay` 仍有一处旧契约差异：
      - `skips_discover_rounds` 期望 `t`，现场结果为 `f`
      - 说明 `precomputed_missing_blocks` / discover-round shortcut
        与新 fast path 仍有一处交互待继续收敛

- 已完成修复 `runtime/` 目录 stale `fbspill-*` 在失败查询后不清理的问题：
  - 现场复现已锁定：
    - `Custom Scan (FbWalIndexScan)` / `FbCountAggScan` 在建 WAL index 期间若因
      `storage_change` 等错误提前 `ERROR`
    - 当前 backend 自己创建的 `runtime/fbspill-<pid>-*` 会残留为空目录
  - 当前根因已确认：
    - 失败路径缺少对当前 backend runtime 产物的兜底清扫
    - 单靠正常 `spool` 生命周期释放不足以覆盖这类 abort 路径
  - 当前修复已落地：
    - 为 `fb_runtime` 新增 `fb_runtime_cleanup_current_backend()`
    - `fb_custom_scan.c` 在异常清理时补 current-backend runtime sweep
    - `fb_entry.c` 的 SRF abort callback 也补同一兜底清扫
    - `fb_runtime_cleanup` 新增 RED：
      - 故意触发 `storage_change` 错误后，断言当前 backend `runtime` 残留为 `0`
  - 当前验证已完成：
    - `make ... installcheck REGRESS='fb_runtime_cleanup fb_custom_scan fb_value_per_call'`
      `All 3 tests passed.`
    - 本机手工复现：
      - `count(*) FROM pg_flashback(...)` 捕获 `storage_change` 错误后
      - 同 backend 立即检查 `pg_flashback/runtime`
      - `runtime_entries = 0`

- 已确认并开始修复 `documents @ 2026-04-04 23:40:13` 的 `3/9` 新 blocker：
  - 当前 `summary payload locator-first` 方案方向本身成立：
    - 已明显压低 payload 阶段的无关 WAL decode
  - 但 live case 新热点已经从 “payload decode” 转移成
    “payload locator 查询期规划”
  - 现场证据已经确认：
    - `select count(*) from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13')`
      在 `3/9` 停留期间，backend `gdb/perf` 热点几乎全部落在：
      - `fb_build_summary_payload_locator_plan`
      - `fb_summary_segment_lookup_payload_locators_cached`
      - `pg_qsort`
    - 当场栈顶已抓到：
      - `pg_qsort_swapn`
      - `fb_summary_segment_lookup_payload_locators_cached`
      - `fb_build_summary_payload_locator_plan`
    - 同次现场 `fb_build_summary_payload_locator_plan` 参数已确认：
      - `base_window_count = 164536`
      - 单 segment lookup 现场一次就在排 `6315` 条 locator
  - 当前根因已确认不是“locator-first 方案无效”，而是查询期实现存在复杂度放大：
    - payload plan 仍按高碎片 `payload_base_windows` 逐窗口逐段调用 locator lookup
    - 同一 segment 会被重复 lookup
    - query cache 当前只缓存 summary 文件，不缓存 relation-scoped public locator slice
    - `fb_summary_segment_lookup_payload_locators_cached()` 每次 lookup 仍重新拷贝并 `qsort`
  - 当前已拍板的新修复方向：
    - 保持“一段 WAL 一个 summary sidecar”模型不变
    - 把 payload locator 从“查询时现拼现排”推进为：
      - summary build 期按 relation 预排序 / 去重的 stable slice
      - query cache 期复用 relation-scoped public locator slice
      - payload plan 期按 segment 去重，不再由碎片 windows 重复驱动 lookup
    - 目标不是只优化 `approval_comments` 一类轻 case，而是先把
      `documents @ 2026-04-04 23:40:13` 从 `3/9` 新热点里拉出来
  - 第一轮 locator 规划收敛已完成后，live case 新热点继续右移到
    `payload materialize / record spool`：
    - 现场复核：
      - `select count(*) from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13')`
        当前已能通过 `3/9 70% payload` 前的 locator 规划，不再卡在
        `pg_qsort`
      - 但 `3/9 100% payload` 仍单独耗时约 `178.360s`
      - 紧接着 preflight 直接报：
        - `estimated=8455177953 bytes`
        - `limit=1073741824 bytes`
    - 新一轮 `gdb` 栈顶已确认热点不再是 locator plan，而是：
      - `fb_wal_visit_payload_locators`
      - `fb_copy_xlog_fpi_record_ref`
      - `fb_index_append_record`
      - `fb_spool_log_append`
      - `pwrite64`
    - 当前判断：
      - locator-first 已把“无关 WAL decode”压下去
      - 但 payload 物化阶段仍把大量 page-image 记录完整写入 record spool
      - 当前要继续压的是“对 replay 无贡献或不需要保留完整 image 的 payload 体积”，
        先把 `3/9 payload` 与 preflight working set 一起打下来
  - 针对用户现场固定 SQL
    `select count(*) from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13')`
    已继续落地 `count(*)` 专用快路径：
    - planner `UPPERREL_GROUP_AGG` 现会把纯 `count(*)` 识别成
      `Custom Scan (FbCountAggScan)`，不再走 PostgreSQL `Aggregate -> FbApplyScan`
    - `FbCountAggScan` 内部固定走 `FB_WAL_BUILD_COUNT_ONLY`：
      - 不再捕获 payload record spool
      - 不再构造 reverse/apply
      - metadata 阶段按 xid 累加主表 `insert/delete/update` 计数，
        `xact-status` 结束后直接汇总历史行数
    - 2026-04-05 本机 `alldb` 复核：
      - `3/9 30% metadata` 约 `2.263s`
      - `3/9 55% xact-status` 约 `17.134s`
      - `3/9 100% payload` 约 `0.002s`
      - `3/9` 整段累计约 `19.6s`，已压到 `< 20s`
      - 同次整条 SQL 总耗时约 `22.132s`
    - 当前新尾巴已经从 `3/9 payload` 转移到整体总耗时与
      `SELECT *` 非聚合路径，不再是本次用户要求的 blocker
  - 在 `SELECT *`
    `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
    的 live case 上，新一轮现场已确认 `3/9` 热点继续右移：
    - `xact-status` 的全窗口回退已收窄为 unresolved-only fallback
    - 但当前 payload locator-first 仍保留“逐条 locator 重新 `XLogBeginRead`”
      的访问模式
    - `gdb` / `pg_stat_activity` 现场已确认热点落在：
      - `fb_wal_visit_payload_locators`
      - `XLogReadRecord`
      - `WALRead`
    - 同次现场参数已确认：
      - `locator_count = 4530229`
      - 两条 live backend 都持续停在 payload locator 逐条读 WAL
  - 当前已拍板的新修复方向：
    - 不改变 summary payload locator-first 的 correctness 语义
    - 不允许因为批量顺扫而把不在 locator 集中的 relation record 错误发射进 payload
    - 在此前提下，把 payload locator 物化改成：
      - 高密度 locator case 下按 covered segment run 批量顺扫 WAL
      - 访问期使用精确 locator 迭代器过滤，只在 `record_start_lsn` 命中时才发射 payload
      - 稀疏 locator case 继续保留 direct read，避免把 decode 范围无谓放大
    - 目标是先把用户固定 SQL 的 `3/9` 明确压到 `< 20s`
  - 2026-04-06 本机继续复核：
    - `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
      当前 `3/9` 已降到：
      - `0% prefilter` `+49.683 ms`
      - `10% summary-span` `+24.843 ms`
      - `30% metadata` `+1196.110 ms`
      - `55% xact-status` `+14716.162 ms`
      - `70% payload` `+0.103 ms`
    - `3/9` 累计约 `15.99s`，已经压到 `< 20s`
  - 本轮真正生效的不是单点微调，而是两刀一起落地：
    - payload locator 改成“批量顺扫 + 精确 locator 命中过滤”
      - live case 不再卡在逐条 `XLogBeginRead`
    - xact fallback 保持 unresolved-only，但进一步补了：
      - 只对 unresolved 集计数
      - unresolved 全部补齐后立即早停
      - fallback windows 改成从最新 run 往前扫，先吃掉更接近 commit/abort 的区间

- 已修复 `fb_custom_scan` / `fb_replay_debug` 的
  `failed to replay heap insert` 新现场：
  - 现场症状：
    - `fb_custom_scan.sql` 的 cursor `MOVE`
      会在 `failed to replay heap insert` 上失败
    - `fb_replay_debug('fb_custom_scan_target', target_ts)` 也可独立复现
  - 根因已确认：
    - final replay 的 record cursor 仍会收到“相邻两条 start LSN 完全相同”的重复 WAL record
    - 同一条 `heap insert` 被 replay 第二次后，目标 offset 已占用，于是报
      `will not overwrite a used ItemId`
    - 当前 root cause 不在 `apply_image=false` 页基线，而在 payload/record cursor
      仍可能把同一条 WAL record 喂入 final replay 两次
  - 当前修复：
    - `fb_replay_run_pass()` 对“相邻重复 `record.lsn`”做硬去重
    - 依据是 WAL record start LSN 全局唯一，exact duplicate start 可安全跳过
  - 2026-04-06 本机复核：
    - `sql/fb_custom_scan.sql` 直跑通过
    - `make ... installcheck REGRESS='fb_custom_scan' ...` 通过
    - `fb_replay_debug` no-count 场景恢复：
      - `errors=0`
      - `precomputed_missing_blocks=0`
      - `discover_skipped=1`
    - 同 session `count(*) -> full replay` 实际结果集行数仍为 `20000`

- 已完成 `summary payload locator` 架构升级主链，summary 现已从
  “relation spans 缩窗”推进到“payload 精确 record 入口”：
  - 保持模型不变：
    - 仍是“一段 WAL 一个 summary sidecar”
    - 仍是 query-side 可回退的可再生缓存
    - 不引入跨 segment 全局索引
  - 当前实现：
    - `summary v8` 新增 relation-scoped `payload locator` section
    - locator 记录目标 relation payload record 的精确 `record_start_lsn`
    - query-side payload 改为 locator-first：
      - 优先按 locator 定点读取 record
      - 仅对 locator 缺失 / summary 缺失 / recent tail 未覆盖 segment
        回退现有 window 路径
    - `fb_recordref_debug()` 已新增：
      - `summary_payload_locator_records`
      - `summary_payload_locator_fallback_segments`
      - `payload_scan_mode=locator`
  - 当前验证：
    - 定向回归 `fb_recordref` / `fb_summary_v3` / `fb_payload_scan_mode` /
      `fb_wal_parallel_payload` 已通过
    - live case `scenario_oa_50t_50000r.approval_comments`：
      - `fb_recordref_debug(..., '2026-04-04 23:40:13')` 当前返回：
        - `payload_scan_mode=locator`
        - `summary_payload_locator_records=171185`
        - `summary_payload_locator_fallback_segments=0`
        - `payload_scanned_records=171185`
      - 对比升级前：
        - `payload_scanned_records` 从 `8198479` 降到 `171185`
        - `3/9 payload` 从约 `10.3s` 降到约 `5.6s`
      - `... '2026-04-04 22:40:13'` 同样收敛到：
        - `payload_scanned_records=171185`
        - `3/9 payload` 从约 `7.5s` 降到约 `5.6s`
  - 当前结论：
    - `3/9 payload` 已不再由 span 内海量无关 decode 主导
    - 热点已转移到 metadata / xact-status / replay final 等后续阶段

- 已完成 `summary progress` / `meta/summary` 失活索引清理收敛：
  - `summary service` cleanup 当前会按当前可见 WAL 源集合探活删除 orphan summary
  - 为避免打掉刚构建、尚未消费的 summary，orphan cleanup 继续受 `recent protect` 短窗口保护
  - 已补严格文件名匹配：
    - 只清理正式 `summary-<hash>.meta`
    - 不再把 `.tmp.<pid>` 构建临时文件误判成正式 summary
  - 当前回归补齐：
    - `fb_summary_service` 新增“删除 WAL 后删除对应 summary 文件”断言
  - 当前定点验证：
    - `make ... installcheck REGRESS=fb_summary_service` 通过
    - `make ... installcheck REGRESS=fb_summary_prefilter` 通过
    - `make ... installcheck REGRESS=fb_summary_v3` 通过

- 已完成开源镜像目录设计与实施拆解，当前进入落地与首轮整理：
  - 规格文件：
    - `docs/superpowers/specs/2026-04-04-open-source-mirror-design.md`
  - 计划文件：
    - `docs/superpowers/plans/2026-04-04-open-source-mirror-plan.md`
  - 当前已拍板：
    - 根仓库是唯一权威研发源
    - `open_source/pg_flashback/` 是公开镜像目录
    - 统一使用白名单同步脚本刷新，不做手工长期维护
  - 当前实施目标：
    - 新增 `open_source/README.md` / `open_source/manifest.txt`
    - 新增 `scripts/sync_open_source.sh`
    - 首轮生成并检查 `open_source/pg_flashback/`
    - 将 `open_source/` 加入根仓库 `.gitignore`

- 已完成一份新的发布前 gate 设计规格，准备进入实现拆解：
  - 规格文件：
    - `docs/superpowers/specs/2026-04-03-release-gate-design.md`
  - 目标是建立一套可供 agent 持续复用的 `PG14-18` 发布前功能/性能阻断 gate
  - 场景固定覆盖：
    - 空实例清理与 `alldb` 重建
    - `/root/alldbsimulator` 构造 `50 x 100MB` 表
    - `1h` DML 压测
    - 固定目标表扩容到 `5GB`
    - 五个随机时间点 truth snapshot + flashback 正确性比对
    - `COPY TO` / `CTAS` 落盘 flashback
  - 性能口径固定为：
    - 与仓库内 golden baseline 比较
    - 采用“相对比例 + 绝对增量”的双阈值阻断
  - 归档目录口径固定为：
    - `PG14 -> /walstorage/14waldata`
    - `PG15 -> /walstorage/15waldata`
    - `PG16 -> /walstorage/16waldata`
    - `PG17 -> /walstorage/17waldata`
    - `PG18 -> /walstorage/18waldata`
    - 每轮测试前后都必须清理当前版本对应目录
  - 当前实现已推进到可执行骨架：
    - `tests/release_gate/bin/common.sh`
    - `tests/release_gate/bin/run_release_gate.sh`
    - `tests/release_gate/bin/prepare_empty_instance.sh`
    - `tests/release_gate/bin/load_alldb_seed.sh`
    - `tests/release_gate/bin/run_alldb_dml_pressure.sh`
    - `tests/release_gate/bin/grow_flashback_target.sh`
    - `tests/release_gate/bin/capture_truth_snapshots.sh`
    - `tests/release_gate/bin/run_flashback_matrix.sh`
    - `tests/release_gate/bin/evaluate_gate.sh`
    - `tests/release_gate/bin/render_report.sh`
    - `tests/release_gate/bin/selftest.sh`
    - `tests/release_gate/sql/check_environment.sql`
    - `tests/release_gate/sql/list_large_databases.sql`
    - `tests/release_gate/sql/recreate_alldb.sql`
    - `tests/release_gate/sql/table_size_summary.sql`
    - `tests/release_gate/sql/export_table_csv.sql`
    - `tests/release_gate/sql/export_flashback_csv.sql`
    - `tests/release_gate/sql/create_flashback_ctas.sql`
    - `tests/release_gate/sql/drop_flashback_ctas.sql`
    - `tests/release_gate/config/release_gate.conf`
    - `tests/release_gate/config/scenario_matrix.json`
    - `tests/release_gate/config/thresholds.json`
    - `tests/release_gate/golden/pg14.json`
    - `tests/release_gate/golden/pg15.json`
    - `tests/release_gate/golden/pg16.json`
    - `tests/release_gate/golden/pg17.json`
    - `tests/release_gate/golden/pg18.json`
    - `tests/release_gate/templates/report.md.tpl`
  - 当前已验证：
    - `bash tests/release_gate/bin/selftest.sh`：`PASS`
    - `bash tests/release_gate/bin/run_alldb_dml_pressure.sh --dry-run --start-only`
    - `bash tests/release_gate/bin/capture_truth_snapshots.sh --dry-run --mode random`
    - `bash tests/release_gate/bin/capture_truth_snapshots.sh --dry-run --mode dml`
    - `bash tests/release_gate/bin/run_flashback_matrix.sh --dry-run`
    - `bash tests/release_gate/bin/evaluate_gate.sh`
    - `bash tests/release_gate/bin/render_report.sh`
  - 当前已锁定的环境 blocker：
    - 本机 PG18 现有 `archive_command = cp %p /home/18pg/wal_arch/%f`
    - 与 spec 要求的 `/walstorage/18waldata` 不一致
    - 因此 `bash tests/release_gate/bin/run_release_gate.sh --dry-run`
      当前会被环境 gate 正常拦住，不进入 real flow
  - 2026-04-04 当前补充实现口径已拍板：
    - `tests/release_gate/bin/run_release_gate.sh` 需支持阶段级编排：
      - `--list-stages`
      - `--from <stage>`
      - `--to <stage>`
      - `--only <stage>`
    - 阶段名改为面向用户直观可读：
      - `prepare_instance`
      - `start_alldbsimulator`
      - `load_seed_data`
      - `grow_target_table`
      - `start_dml_pressure`
      - `capture_random_truth_snapshots`
      - `wait_dml_pressure_finish`
      - `capture_dml_truth_snapshots`
      - `run_flashback_checks`
      - `evaluate_gate`
      - `render_gate_report`
    - 从中间阶段启动时不自动补前置产物，缺依赖文件时继续直接报错
    - `cleanup` 仍只由总入口 `trap` 负责，不作为用户可选阶段
    - 只有本次执行实际跑过 `prepare_instance` 时，总入口退出才允许清理当前版本归档目录
      - 避免从 `run_flashback_checks` 等中间阶段起跑时误删上一阶段已生成 WAL
    - release gate 所有统一日志输出都补齐时间戳
    - `1h` DML 压测当前真实口径继续固定为：
      - 只在 `load_seed_data + grow_target_table` 完成后启动
      - schema 级目标
      - `insert/update/delete` 等权重混合
      - 每个事务只执行 `1` 个 DML
      - 默认总限速从 `200 ops/s` 上调到 `2000 ops/s`
      - `bulk 10k` / `mixed dml` 仍在后续定向快照阶段单独构造，不并入这 `1h`
    - `grow_target_table` 期间不采随机 truth snapshot，也不作为 flashback 场景时间窗
  - 2026-04-04 本轮已完成：
    - `tests/release_gate/bin/run_release_gate.sh` 已支持：
      - `--list-stages`
      - `--from <stage>`
      - `--to <stage>`
      - `--only <stage>`
    - 用户可见阶段名已统一改为直观动作语义：
      - `prepare_instance`
      - `start_alldbsimulator`
      - `load_seed_data`
      - `grow_target_table`
      - `start_dml_pressure`
      - `capture_random_truth_snapshots`
      - `wait_dml_pressure_finish`
      - `capture_dml_truth_snapshots`
      - `run_flashback_checks`
      - `evaluate_gate`
      - `render_gate_report`
    - `tests/release_gate/README.md` 已重写为完整操作手册：
      - 覆盖环境要求
      - 覆盖总入口和单脚本入口
      - 覆盖每个阶段的作用、依赖和产物
      - 明确 `1h` DML 压测真实执行规则
    - release gate 默认 DML 总限速已上调到 `2000 ops/s`
  - 2026-04-04 本轮继续完成：
    - release gate 阶段顺序已调整为：
      - `prepare_instance`
      - `start_alldbsimulator`
      - `load_seed_data`
      - `grow_target_table`
      - `start_dml_pressure`
      - `capture_random_truth_snapshots`
      - `wait_dml_pressure_finish`
      - `capture_dml_truth_snapshots`
      - `run_flashback_checks`
      - `evaluate_gate`
      - `render_gate_report`
    - 当前固定口径已明确：
      - 目标大表先扩容到 `5GB`
      - 所有扩容插入完成后才启动 `1h` DML 压测
      - 扩容阶段不采随机 truth snapshot，也不纳入 flashback 时间窗
    - `fb_release_gate_log()` / `fb_release_gate_fail()` 已统一补齐时间戳前缀
    - 已修复总入口 `cleanup` 误删已有 WAL：
      - 当前只在本次执行实际跑过 `prepare_instance` 且非 `dry-run` 时清理归档目录
      - 从 `run_flashback_checks` / `render_gate_report` 等中间阶段起跑，不再清掉前序 real run 生成的 WAL
    - `tests/release_gate/bin/selftest.sh` 已补新约束：
      - 锁住 `grow_target_table` 必须先于 `start_dml_pressure`
      - 锁住统一日志必须带时间戳
      - 锁住中间阶段起跑不得删除已有 archive 文件
  - 2026-04-04 本轮已验证：
    - `bash -n tests/release_gate/bin/common.sh tests/release_gate/bin/run_release_gate.sh tests/release_gate/bin/selftest.sh`
    - `bash tests/release_gate/bin/selftest.sh`：`PASS`
    - `bash tests/release_gate/bin/run_release_gate.sh --list-stages`
- 当前已补齐一个用户面安装/升级缺口：
  - 历史上曾在同一 `extversion = '0.1.0'` 下先后安装过
    `pg_flashback(text, text, text)` 与 `pg_flashback(anyelement, text)`
  - 现已正式前滚到 `0.1.1`，并补齐
    `sql/pg_flashback--0.1.0.sql`
    `sql/pg_flashback--0.1.1.sql`
    `sql/pg_flashback--0.1.0--0.1.1.sql`
  - 新增回归 `fb_extension_upgrade` 后，当前已锁住：
    - `CREATE EXTENSION ... VERSION '0.1.0'`
    - `ALTER EXTENSION pg_flashback UPDATE TO '0.1.1'`
    - 升级后旧三参入口消失，新二参入口可直接调用
  - 本机复核：
    - `make ... installcheck REGRESS='fb_extension_upgrade fb_user_surface pg_flashback'`
      `All 3 tests passed.`
    - `alldb` 中执行 `ALTER EXTENSION pg_flashback UPDATE TO '0.1.1'` 后，
      用户 SQL 已不再报 `function does not exist`
- 当前第一优先级 blocker 已固定为：
  - `87/17F73AB8 / rel=1663/33398/16395737 / blk=38724 / off=1 / failed to locate tuple for heap delete redo`
  - 后续所有“继续修复真实闪回失败”默认先围这条 cold-run first blocker
  - 在该 blocker 清掉前，不将 summary / 回归 / 性能类问题视为主线完成
- 当前并行跟踪一个独立 runtime bug：
  - `pg_flashback summary worker 1/2` 长时间驻留 `7GB+ RSS`
  - 当前现场：
    - `PID 1173478` `postgres: pg_flashback summary worker 2` `RSS ~= 7.8GB`
    - `PID 1173479` `postgres: pg_flashback summary worker 1` `RSS ~= 7.4GB`
  - 当前怀疑不是单次查询内存，而是 worker 长生命周期内的 MemoryContext / mmap / cache 未及时回落
  - 该问题按第二优先级并行修复，但不得打断 `blk=38724` 主线追查
- 当前新增一个已锁定 root cause 的用户 case：
  - `scenario_oa_12t_50000r.roles @ '2026-04-02 22:10:13'`
    在 `4/9 replay discover` 报 `missing FPI for block 9990`
  - 已确认不是 replay discover 不会消费 FPW，而是 query-side payload window
    把跨 segment 的记录头裁掉了：
    - `89/EFFFF9D8 DELETE ... blk=9990 FPW` 在 `pg_waldump` 中存在
    - 但 `fb_recordref_block_debug(..., 9990)` 的首条记录已变成
      `89/F0001908`
  - 当前修复方向已固定为：
    - payload 物化读取必须对“窗口首条可能跨 segment 延续”的场景做向前补读
    - 只放宽 read 范围，不放宽 emit/filter 边界
  - 2026-04-03 本轮已完成修复并现场复核：
    - payload read window 现会为首窗补读前一连续 segment
    - payload emit gate 已从“仅看 `ReadRecPtr >= emit_start`”
      修正为“允许 `EndRecPtr = emit_start` 的紧邻前驱记录进入索引”
    - `fb_recordref_block_debug(..., 9990)` 现已重新看到：
      - `89/EFFFF9D8 DELETE ... image=true:apply=true`
    - 同一 live SQL 不再报 `missing FPI for block 9990`
    - 默认 `memory_limit=1GB` 下已前移为真实 preflight 门限报错
    - `SET pg_flashback.memory_limit = '3GB'` 后，
      `select count(*) ... roles ...` 已返回 `98896`
- 回归面当前已重新收口为绿色基线：
  - `fb_user_surface + fb_replay` 已复现并修复一处测试夹具幂等性问题
  - 根因不是 replay 逻辑回退，而是 `sql/fb_replay.sql` 新增
    `fb_replay_prune_lookahead_snapshot_isolation_debug()` 后，
    清理段漏掉对应 `DROP FUNCTION`
  - 当前最小复现与全量回归都已恢复：
    - `pg_regress ... fb_user_surface fb_replay`：`All 2 tests passed.`
    - `make ... installcheck`：`All 36 tests passed.`
  - 主线开发焦点不变：
    - 继续回到 batch B / deep pilot 的页级 replay root cause
    - 不把这次回归夹具问题误判成产品行为问题
- 正在修复一个新的页级回放 root cause：
  - 现场已稳定复现：
    - `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'`
    - `scenario_oa_12t_50000r.documents @ '2026-03-31 22:50:13'`
    都会报：
    - `lsn=87/6E00D568 rel=1663/33398/16395737 blk=1084494 off=4 ... maxoff=1`
    - `failed to replay heap insert`
  - 当前 root cause 假设已收紧为：
    - 回放内核把 `has_image=true` 错当成“可直接 materialize / 覆盖页面”
    - 但 PostgreSQL WAL 语义里还存在 `has_image=true && apply_image=false`
    - 这类记录不应覆盖已有页状态，也不应被当成页基线锚点
  - 当前修复方向：
    - 先补最小 RED，锁住 `apply_image=false` 合约
    - 再修 replay / precomputed-missing / anchor resolve 对 image 的判定
    - 最后复跑 live case 与相关回归
  - 当前阶段性结论已新增：
    - 已确认 `has_image=true && apply_image=false` 的误用确实存在，已补 RED 并修正：
      - `fb_replay_ensure_block_ready()`
      - `fb_wal_record_block_materializes_page()`
      - missing-anchor resolve / 若干 replay gate
    - 但 live case 的真正主链 blocker 不止这一处
    - 当前继续追到的新现场表明，还存在至少两类独立页态问题：
      - `HEAP2_PRUNE` 的 applicable image 在部分 block 上会把已有页态压得过旧，导致后续 `insert/update` 的 `offnum` 不再可落
      - `INIT_PAGE` 记录当前已确认需要无条件重置已有 block state；此前只在“块未初始化”时 `PageInit()` 的逻辑不符合 redo 语义
    - 当前已新增开发期 trace：
      - `fb_recordref_block_debug()` 现会额外打印 `record_index / apply_image / imgmax`
      - 已借此确认：
        - `blk 1084494` 上 `83/6675CDF8` image `imgmax=4`
        - `86/30212248` image `imgmax=1`
        - `blk 1090557` / `blk 1109792` 也存在 `PRUNE` image 与后续 slot reuse 相互影响的现场
      - 2026-04-03 继续用 final-pass 单块 trace 收敛到更具体现场：
        - `blk 1084485` 在 final replay 中的连续页态会经过：
          - `80/D6079340 PRUNE_ON_ACCESS dead=[1,2]`
          - `83/6674F870 PRUNE_ON_ACCESS dead=[3,4,5] unused=[6]`
          - `86/30211CD8 PRUNE_VACUUM_CLEANUP FPW`
        - 到 `87/6E031A08 LOCK off=1` 前，页态已变成 `lp1..lp5 = dead`
        - 随后 `87/6E031A78 UPDATE old_off=1` 仍要求从同一 block 读取正常旧 tuple
        - 这说明当前 blocker 已进一步收敛为：
          - `PRUNE/FPI` 连续页态与后续 `UPDATE old_off=1` 的 WAL 语义之间仍有未解释矛盾
          - 仅把 final pass 中的 `PRUNE/FPI` 改成“已有页态时只推进 LSN、不覆盖页内容”的保守模式，仍不能消掉该现场
      - 2026-04-03 最新安装态复现 `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'` 后，
        targeted trace 已进一步确认：
        - `blk 1084485 @ 86/30211CD8` 现在确实会进入 preserve 判定
        - 但结论是 `current_ok=false image_ok=false`
        - 因而它“没有 preserve 日志”的旧判断已过时；当前不是漏走 preserve gate
      - 同一轮 live log 继续推进到新的 first blocker：
        - `87/6E098C40 / blk=1084494 / failed to locate tuple for heap delete redo`
        - 结合 `fb_recordref_block_debug(..., 1084494)` 与 `pg_waldump` 已确认该块主链为：
          - `83/6675CDF8` `FPI_FOR_HINT imgmax=4`
          - `83/6675EC30` `PRUNE_ON_ACCESS dead=[1,2,3] unused=[4]`
          - `86/30212248` `PRUNE_VACUUM_CLEANUP FPW`
          - `87/6E00D568` `INSERT off=4`
          - `87/6E062350` `DELETE off=4`
          - `87/6E098C40` `DELETE off=3`
        - 当前最强 root cause 假设已改为：
          - `prune lookahead` 现在只保存“最近一条 future record”的约束
          - 这能保住 `insert off=4`
          - 但会漏掉同一块更后面的 `delete off=3`
          - 因而 `83/6675EC30` 之后页态仍把 `off=3` 变成 dead，直到 `87/6E098C40` 真正报错
      - 2026-04-03 本轮已把上述 root cause 落成最小 RED + 单点修复：
        - 新增 `fb_replay_prune_compose_future_constraints_debug()` 回归
        - `prune lookahead` 已从“单条 future record 覆盖”修成“按 future record 逆向组合前置页态约束”
        - 当前 `fb_replay` 专项回归已通过
      - 修复后同一 live case 已继续推进，不再停在 `1084494`
        - 新的 first blocker 变为：
          - `80/C90B5FB8 / rel=1663/33398/16395804 / blk=17079 / off=42 / failed to replay heap insert`
        - `pg_waldump` 当前已确认该 TOAST block 可见链路至少包含：
          - `80/C81991E0 PRUNE_ON_ACCESS dead=[23]`
          - `80/C8199218 DELETE off=24`
          - `80/C90B5FB8 INSERT off=42`
          - `80/C91A3B90 PRUNE_ON_ACCESS dead=[24]`
          - `80/C91A3BC8 DELETE off=42`
        - 该现场和 `1084494` 不同：
          - 当前报错时 `maxoff=41 off=42 has_item=false`
          - 更像页内可用空间/碎片状态异常，不像 line pointer preserve 缺口
      - 2026-04-03 本轮继续把 `17079` 的 TOAST blocker 收紧成更小 root cause：
        - targeted trace 已确认该块在 `80/C81991E0 PRUNE_ON_ACCESS dead=[23]` 时
          - decode 结果正确：`ndead=1 dead_first=23 cleanup_lock=true`
          - 但修复前的 final replay 里 `prune after` 仍保持 `lp23=normal upper=320`
        - 根因已确认不是 `PageRepairFragmentation` 本身，而是：
          - `fb_replay_build_prune_lookahead()` 把 `entry->future = future_entry->future`
            做成了浅拷贝
          - 同块更老 record 的 `old_off=23` 会回写污染已存下来的 prune lookahead snapshot
          - 从而把本不该保留的 `dead[23]` 错判成 future old tuple requirement
        - 当前修复结果：
          - 新增 `FbReplayFutureBlockRecord` 深拷贝 helper
          - `prune lookahead` 存 entry 时改成 clone，不再共享 `Bitmapset *`
          - 新增 RED `fb_replay_prune_lookahead_snapshot_isolation_debug()`
            锁住“lookahead snapshot 不能被同块更老 record 污染”
          - live trace 已确认 `17079` 现在会走成：
            - `80/C81991E0 prune after: lp23=dead upper=512`
            - `80/C90B5FB8 insert off=42` 成功
            - `80/C91A3B90 prune after: lp24=dead upper=512`
          - `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'`
            已不再停在 `17079`
        - 新的 first blocker 已前移为：
          - `87/6E0B1F10 / rel=1663/33398/16395804 / blk=26273 / off=27 / failed to locate tuple for heap delete redo`
      - 2026-04-03 最新结论已改线：
        - `26273` 不是新的 `prune/lookahead` 语义缺口
        - 真正 root cause 是历史 `summary v6` sidecar 污染：
          - `87/6D` 的旧 summary relation span 只覆盖到 `87/6D604398`
          - 漏掉了同段更晚的
            - `87/6DD491C0` toast insert off=27
            - `87/6DD4B110` toast insert off=28
            - `87/6DD4B230` main insert blk=1084489
          - 从而 query-side payload/materialize window 把整笔 `tx=6343730` 从 record index 中裁掉
        - 当前已确认：
          - 临时旁路 `meta/summary` 后，`fb_recordref_block_debug(..., 1084489)` 会重新看到 `87/6DD4B230`
          - 仅重建 `87/6D` summary 后，summary-on 路径也会重新看到这条记录
        - 当前永久修复已落地：
          - `FB_SUMMARY_VERSION` 已前滚到 `7`
          - 新增回归 helper `fb_summary_v6_rejected_debug()`
          - `fb_summary_v3` 已锁住“旧 `v6` summary 必须失效”
  - 当前未完成：
    - `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'`
      当前已不再停在 `26273`
      - 复跑结果：`count = 4356677`
    - 剩余主线仍需继续回到更早 batch B / deep pilot root cause，
      不能把 `summary v6` 失效修复误当成整条主线已收尾

## 本轮完成

- 已完成开源镜像文档双语化与 README 用户手册重写：
  - 影响目录：
    - `open_source/pg_flashback/README.md`
    - `open_source/pg_flashback/docs/**/*.md`
    - `open_source/pg_flashback/tests/README.md`
  - 当前结果：
    - 开源镜像内全部 Markdown 已统一为“单文件中英双语”
    - 每份文档顶部都提供 `中文` / `English` 跳转按钮
    - `README.md` 已改写为当前开源版本实际可用的用户手册
    - README 只描述已公开安装的接口、配置、查询方式、`CTAS` / `COPY` 承接方式、结果语义、错误边界和基本原理
    - 未对外安装或未完成的能力当前只作为边界说明保留

- 已完成 `fb_replay` 回归夹具幂等性修复：
  - 现象：
    - 全量 `installcheck` 一度只剩 `fb_replay` 红
    - 最小复现已收敛为：
      - `pg_regress ... fb_user_surface fb_replay`
  - 根因：
    - `sql/fb_replay.sql` 新增
      `fb_replay_prune_lookahead_snapshot_isolation_debug()` 后
    - 清理段未同步 `DROP FUNCTION`
    - 在已有脏 `contrib_regression` 数据库里二次执行时，
      会先撞 `CREATE FUNCTION already exists`
  - 修复：
    - 为 `fb_replay_prune_lookahead_snapshot_isolation_debug()` 补齐清理逻辑
    - 同步 `expected/fb_replay.out`
  - 验证：
    - `pg_regress ... fb_user_surface fb_replay`：`All 2 tests passed.`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck`
      ：`All 36 tests passed.`
- 已完成旧 `summary v6` sidecar 失效修复：
  - 根因：
    - 历史 `87/6D` summary 文件 relation span 截断，导致 `tx=6343730`
      的晚段 main/toast 记录不再进入 query-side payload window
  - 修复：
    - `FB_SUMMARY_VERSION` 前滚到 `7`
    - 新增回归 `fb_summary_v6_rejected_debug()`
    - `fb_summary_v3` 已通过
  - live 复核：
    - `scenario_oa_12t_50000r.documents @ '2026-04-01 22:10:13'`
      当前返回 `count = 4356677`
    - `v7` 生效并重启 PG18 后，cold run 已不再回到 `26273`
      - 新的 first blocker 前移为：
        - `87/17F73AB8 / rel=1663/33398/16395737 / blk=38724 / off=1 / failed to locate tuple for heap delete redo`
- 已完成 `pg_flashback_summary_progress` 的两处问题修复：
  - 现象 1：
    - 用户视图里 `completed_segments / missing_segments / frontier` 长时间看起来不推进
  - 当前 root cause 1：
    - 之前不只是 `cold` claim 顺序偏向较新 segno
    - launcher enqueue 也会在固定 queue capacity 内先塞满较新的 cold backlog
    - 结果 oldest 侧 backlog 长时间既不进队，也不被 worker 认领
  - 现象 2：
    - `estimated_completion_at` 有时长期为 `NULL`
    - live 环境中还已复现出错误的 epoch-like 时间：
      - `2000-01-01 08:00:40...`
  - 当前 root cause 2：
    - ETA 当前只接受过窄的 recent throughput sample
    - 样本不足时直接回 `NULL`
    - 同时绝对时间之前直接做原始 `TimestampTz` 算术，live 已暴露错误绝对时间值
  - 当前修复结果：
    - cold queue 已改成：
      - enqueue: `hot` 仍保 newest-first
      - enqueue: `cold` 改成 oldest-first
      - claim: `cold` 也按 oldest-first + 连续 batch 向前推进
    - ETA 已改成：
      - 用 `throughput_window_started_at .. max(last_build_at,last_scan_at)` 取样
      - backlog 存在且已有 recent sample 时返回未来时间
      - 用 `pg_add_s64_overflow` 安全构造绝对时间，避免 epoch-like 错值
    - 已新增一个仅调试/回归使用的调度仿真入口：
      - `fb_summary_service_schedule_debug(integer, integer, integer)`
      - 用于锁住固定 queue capacity 下 `cold` oldest-first 的调度语义
    - 当前定向验证已通过：
      - `fb_summary_service`
      - `fb_progress`

- 已定位一处新的 `recovered_wal` 污染 root cause，当前正在修复：
  - 现象：
    - 某些环境下即使 archive 已能提供查询所需的 retained suffix
    - `DataDir/pg_flashback/recovered_wal` 仍会持续累积真实 segname 文件
  - 当前 root cause：
    - `fb_collect_archive_segments()` 会在 retained suffix 决定之前
    - 先把 `pg_wal` 中所有 mismatch candidate 尝试 convert 到 `recovered_wal`
    - 其中一部分 candidate 虽然最终会因 prefix gap 被裁掉，不参与本次查询
    - 但 convert 已经发生，导致 `recovered_wal` 被不必要地污染
  - 当前修复方向：
    - 先补回归锁住“非保留 suffix 的 mismatch 不应 materialize”
    - 再把 convert 时机后移到真正需要该 segno 时

- 已完成一处新的 WAL resolver 尾部无效段误判修复：
  - 现象：
    - 用户现场 `scenario_oa_12t_50000r.documents @ 2026-03-31 22:10:13/23:10:13`
      报：
      - `WAL not complete: pg_wal contains recycled or mismatched segments...`
    - live 复核后确认真正 root cause 不是“archive exact-hit 被同名 mismatch 压过”
    - 而是 resolver 会把 `pg_wal` 里更高 segno 的 recycled/预分配无效尾段当成必需最新段
    - 从而在 `2/9` 过早报 `pg_wal contains recycled or mismatched`
  - 当前修复结果：
    - `fb_collect_archive_segments()` 不再以“目录里名字最大的 candidate”作为 suffix 起点
    - 当前改为只从真正可用的 direct candidate（archive / valid pg_wal / ckwal）里选最高 segno
    - trailing invalid `pg_wal` tail 现在会被安全忽略，不再挡住 archive/valid suffix
    - 新增回归覆盖：
      - trailing invalid `pg_wal` tail 不应阻断 archive suffix
      - archive exact-hit + 同名 mismatch 仍必须优先取 archive
    - live 现象已收敛：
      - `2026-03-31 22:10:13` 当前报真实 retained suffix 不足
      - `2026-03-31 23:10:13` 当前已能走过 `2/9`，继续进入后续阶段

- 已完成一轮面向历史 backlog 的 summary 预建吞吐收敛：
  - 服务调度已从“纯 recent-first”收敛为：
    - 固定 `1` 个 hot-first worker
    - 其余 worker 默认 cold-first
    - 当首选队列为空时允许回退到另一队列
  - cold backlog 已支持小批量连续 segment claim，减少 shared queue 往返与锁竞争
  - service enqueue 阶段已新增“summary 文件存在”快路径，避免对大量缺失段反复 `open/read header`
  - `pg_flashback_summary_progress` 已新增：
    - `estimated_completion_at`
    - 口径为“最近 build 速率下的 backlog ETA”；样本不足或速率不可用时返回 `NULL`
  - summary builder 已改成边扫边去重：
    - `touched_xids`
    - `unsafe_facts`
    - `block_anchors`
    不再依赖段尾统一 `qsort/dedup`
  - 当前已通过定向验证：
    - `fb_summary_service`
    - `fb_summary_v3`
    - `fb_wal_parallel_payload`
    - `fb_progress`

- 已拍板删除原表闪回后的全表闪回承接面与加速方向：
  - 产品边界：
    - 删除 `pg_flashback_to(regclass, text)` 后
    - 全表闪回若需落地新表，正式推荐 `CTAS AS SELECT * FROM pg_flashback(...)`
    - `COPY (SELECT * FROM pg_flashback(...)) TO ...` 仅作为导出路径
  - 当前调研结论：
    - PostgreSQL 内核原生支持 `CREATE TABLE AS query`
    - PostgreSQL `COPY` 原生只支持：
      - `COPY table FROM ...`
      - `COPY (query) TO ...`
    - 不存在“`COPY FROM SELECT` 直接创建新表”的内核语法
    - 当前 `pg_flashback(...)` 在普通 `SELECT` 与 `CTAS` 下都已实测进入扩展自带 `CustomScan` 链，而不是回退到 PostgreSQL 默认 `FunctionScan`
    - PostgreSQL `CTAS` 末端本身已不是通用慢 receiver：
      - 内核 `IntoRelDestReceiver` 已直接使用 `BulkInsertState + table_tuple_insert()`
      - 因此“替换 CTAS receiver 本身”不是当前最优攻击点
  - 已确认的下一阶段主目标：
    - 不新增新的公开 helper
    - 保持用户继续写标准 `CTAS AS SELECT * FROM pg_flashback(...)`
    - 不尝试替换 PostgreSQL 原生 `CTAS` receiver
    - 改为识别 `full-output` 场景：
      - `SELECT * FROM pg_flashback(...)`
      - `CTAS AS SELECT * FROM pg_flashback(...)`
      - `COPY (SELECT * FROM pg_flashback(...)) TO ...`
    - 将优化重点从“新表写入 sink”转为：
      - `apply` 上游的 tuple 产出与搬运成本
      - full-table materialization 专用 fast path
    - 以“全表闪回结果导出/落地”共享快路径速度最优为第一目标
  - 当前状态：
    - 已修正设计前提，并已开始实现第一阶段 full-output 快路径
  - 当前已落地：
    - `FbApplyScan` 已新增 `full-output` 识别口径：
      - 覆盖 simple full-row output
      - 包括 `SELECT * FROM pg_flashback(...)`
      - 以及 simple `CTAS AS SELECT * FROM pg_flashback(...)`
      - 同时也覆盖“按原列顺序显式列出全部列”的等价写法
    - `EXPLAIN (VERBOSE)` 已新增：
      - `Flashback Full Output Fast Path: true|false`
      - `Flashback Output Dispatch: direct|execscan`
    - 命中 `full-output` 时，`FB_CUSTOM_NODE_APPLY` 已不再走 `ExecScan(...)`
      - 当前改为直接调用 `fb_flashback_apply_next()`
      - 先减少无 qual / 无 projection 场景下的通用 scan/projection 调度开销
    - 已新增定向回归：
      - `fb_flashback_full_output`
      - 断言 simple `SELECT` 与 simple `CTAS` 都命中同一套 full-output explain/debug 口径
      - 并验证 `CTAS` 结果与查询结果一致
    - 当前定向验证已通过：
      - `fb_flashback_full_output`
      - `fb_user_surface`
      - `pg_flashback`

- 已拍板并开始执行 `pg_flashback_to(regclass, text)` 的破坏性删除：
  - 删除目标：
    - 公开安装面
    - `fb_export` 原表回退实现
    - 相关回归与文档
  - 删除后产品边界固定为：
    - 仅保留查询式 `pg_flashback(anyelement, text)`
  - 兼容策略：
    - 不保留别名、stub、NOTICE 或迁移提示
    - 升级后旧 SQL 直接因函数不存在失败
  - 当前状态：
    - 正在执行

- 已开始收敛 `sparse payload` 冷态随机读放大问题，并串行跟进新的 `missing FPI` 现场 bug：
  - 背景：
    - 当前 live case `scenario_oa_12t_50000r.documents @ '2026-03-31 22:40:13+08'`
      虽已把 `payload_scanned_records` 从约 `1960万` 降到约 `180万`
    - 但 `fb_recordref_debug()` 仍显示：
      - `payload_windows=259687`
      - `payload_covered_segments=718`
      - `payload_scan_mode=sparse`
    - 这说明当前瓶颈已从“decode 总量过大”收口为：
      - 同一批 covered segments 被切成大量稀疏小 window
      - `fb_wal_visit_sparse_windows()` 仍按 window 级别反复重置 reader / 关开 segment / 重新找记录起点
      - 重启后的第一次查询因此出现明显冷态随机读放大
    - 用户新增现场同时暴露：
      - `documents @ '2026-04-01 23:15:13'`
      - `4/9 replay discover precomputed`
      - 报错 `missing FPI for block 216136`
  - 当前拍板方向：
    - 先做一轮不改 payload 语义的安全优化：
      - 优先复用 sparse path 内同一 segment slice 的 reader / open file 状态
      - 先减少 window 级重定位与随机读，不先扩大 payload decode 覆盖面
    - 优化完成后立即复现并修正新的 `missing FPI` 现场问题
  - 当前最新进展：
    - 已把 `reader/file` 复用逻辑同时接到串行 sparse path 与 payload worker path
    - live debug `documents @ '2026-03-31 22:40:13+08'` 已能稳定看到
      - `payload_sparse_reader_reuses=272253`
    - 已给 `fb_recordref_debug()` 补出 `anchor_redo`
    - 已新增一个仅调试期手工创建的 `fb_recordref_block_debug()` C 入口，便于排查单 block 是否真的进入 record index
  - 当前 bug 收敛结论：
    - `documents @ '2026-04-01 23:15:13'` 已不再停在 `missing FPI for block 216136`
    - 本轮继续追到的真实 root cause 不是 replay 内核本身，而是：
      - 查询侧仍在信任旧版本 `meta/summary` sidecar
      - 其中部分 segment summary 只覆盖了 segment 前半段 relation spans
      - 例如 `0000000100000084000000AD` 的旧 summary 只到
        `84/AD836320`
      - 导致同块更早 WAL：
        - `84/ADDC9090 INSERT off=5`
        - `84/ADDC91B0 INSERT off=6`
        未进入当前 payload record index
      - 最终在 discover/final replay 上表现成：
        - `84/AE079278 INSERT off=7`
        - `failed to replay heap insert`
        - `maxoff=4`
    - 已验证：
      - 手工重建该 segment summary 后，`off=5/6/7/8` 四条记录都会重新进入 record index
      - 再继续复现时，旧 `heap insert / blk=216125` 错误消失
      - 将 `FB_SUMMARY_VERSION` 前滚后，原始现场
        `SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents,'2026-04-01 23:15:13')`
        已返回：
        - `count = 4356675`
        - `total elapsed 108734.386 ms`
      - 在本轮回归收口后的再次冷启动复测中，同一 SQL 继续稳定返回：
        - `count = 4356675`
        - `total elapsed 160227.813 ms`
      - 定向回归当前已通过：
        - `fb_summary_v3`
        - `fb_wal_parallel_payload`
    - 当前正式修复口径：
      - 将 summary sidecar 版本前滚，强制旧语义/旧内容的 summary 文件失效
      - 查询侧对这些旧 summary 自动回退为安全 WAL 扫描或等待新版本 summary 重建
  - 验证目标：
    - 冷启动下 `3/9 payload` 明显下降
    - `documents @ '2026-04-01 23:15:13'` 不再报 `missing FPI for block 216136`
  - 当前状态：
    - 正在执行

- 已开始回收 `WAL payload dynamic bgworker/DSM` 路径：
  - 背景：
    - 当前代码仍在 `fb_wal_materialize_payload_parallel()` 顶部硬返回 `false`
    - `fb_wal_parallel_payload` 现状也只断言“并行请求下仍显示 `payload_parallel_workers=0`”
    - 用户现场已明确要求重新放开 payload 并行，不再接受长期停留在串行 fallback
  - 本轮目标：
    - 先把自动化回归改回“真实并行 worker 生效”口径
    - 再重新启用 payload bgworker/DSM 主路径
    - 最后用用户提供的 `documents @ '2026-03-31 22:40:13'` 现场 SQL 连跑复核
  - 当前状态：
    - 正在执行

- 已拍板进入 `3/9` payload 的 `B` 档架构收敛：
  - 背景：
    - 当前 live case `documents @ '2026-04-01 01:40:13'` 在保守 narrowing 后，`3/9 payload` 已从约 `97s` 降到约 `16.403s`
    - 但 `fb_recordref_debug()` 仍显示：
      - `payload_scanned_records=19603174`
      - `payload_kept_records=690785`
      - 保留率仍只有约 `3.5%`
    - 这说明剩余热点已不在 window 数量，而在 payload window 内部仍按顺序 decode 了大量无关 record
  - 当前拍板方向：
    - 不再继续单纯扩大/合并 payload windows
    - 改为引入 `summary-span` 驱动的 sparse candidate-stream 读路径
    - 对 summary 已提供的细粒度 relation spans，payload 侧尽量直接跳到 candidate span 起点，而不是把多个 span 重新并成粗窗口后顺扫整段
    - 缺 summary 的 fallback segment 仍保留现有安全全扫语义
  - 目标：
    - 继续压低 `payload_scanned_records`
    - 在不改 replay/apply 接口语义的前提下，把 `3/9` 的剩余主耗时从“粗窗口内无关 decode”转成“candidate span 命中式 decode”
  - 当前实现结论：
    - 直接吃 raw summary spans 的 sparse path 在 live case 上会把 replay 打坏：
      - `payload_refs` 从安全口径 `690785` 漂到 `762265`
      - `SELECT count(*) ...` 现场会报
        `failed to replay heap insert`
    - root cause 已收敛为：
      - raw summary spans 自身仍需要先做一轮全局 overlap-normalize
      - 不能直接把“原始 span 列表”当成最终 payload candidate stream
    - 当前稳定版本改为：
      - 先对 payload candidate spans 做一轮 `fb_merge_visit_windows()` 级别的 overlap merge
      - 只消重叠/接壤，不做同 slice 激进扩窗
      - 再把这层 merge-normalized spans 作为 sparse payload read path 的输入
      - `fb_recordref_debug()` 额外暴露 `payload_scan_mode=sparse|windowed`
  - 当前 live debug：
    - `fb_recordref_debug('scenario_oa_12t_50000r.documents', '2026-04-01 01:40:13+08')`
    - 当前返回：
      - `payload_scan_mode=sparse`
      - `payload_windows=259685`
      - `payload_covered_segments=714`
      - `payload_scanned_records=1808411`
      - `payload_kept_records=690785`
    - 与上一版安全 windowed 口径相比：
      - `payload_refs` 保持回到 `690785`
      - `payload_scanned_records` 从约 `19603174` 继续降到约 `1808411`
  - 当前 live query：
    - `SET pg_flashback.memory_limit='8GB'`
    - `SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 01:40:13')`
    - 当前返回：
      - `count = 4356191`
      - `3/9 metadata` 约 `1.337s`
      - `3/9 xact-status` 约 `1.786s`
      - `3/9 payload` 约 `17.262s`
      - `NOTICE [done] total elapsed 48872.288 ms`
      - shell 侧总耗时约 `49.12s`
    - 相比用户现场最初看到的：
      - `3/9 payload` 约 `73.198s`
      - 当前已降到约 `17.262s`
      - 且 replay/apply 主链保持正确返回，不再报 `failed to replay heap insert`

- 已将 `3/9 build record index` 的用户观测收口到固定子相位：
  - root cause：
    - live case 上原先存在 `3/9 100%` 后到 `4/9 0%` 之间的长空白
    - 结合 `fb_replay_debug()` 已确认：
      - `4/9 replay discover` 在该类 case 上已是 precomputed/近空转
      - 真正被隐藏的是 build-index 尾段工作，旧 `NOTICE` 会误导用户把热点看成 `4/9`
  - 当前修复口径：
    - 仍保持公开进度总段数固定为 `9`
    - `3/9` 改为固定子相位发射：
      - `prefilter`
      - `summary-span`
      - `metadata`
      - `xact-status`
      - `payload`
    - `fb_progress` 当前会拒绝未进入该 stage 的越级 percent 更新，避免 prepare 阶段提前打出伪 `3/9 100%`
  - 新增回归：
    - `fb_progress` 当前会稳定断言新的 `3/9` 子相位流
  - 当前验证：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_progress fb_recordref'`
      当前结果：`All 2 tests passed.`

- 已为 `fb_recordref_debug()` 增加 build-index payload work counters：
  - 当前新增 debug 字段：
    - `payload_covered_segments`
    - `payload_scanned_records`
    - `payload_kept_records`
  - 当前口径：
    - 与现有 `payload_windows` / `payload_parallel_workers` 一起输出
    - 先服务回归与 live case 定位，不额外进入用户 SQL 视图
  - 新增回归：
    - `fb_recordref` 已断言 serial / parallel 调试字符串都包含上述 payload counters
  - 当前验证：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_progress fb_recordref'`
      当前结果：`All 2 tests passed.`

- 已对 build-index payload phase 做一处最小 narrowing，并完成 live 复核：
  - 当前测量结论：
    - 原 live case `documents @ '2026-04-01 01:40:13'` 上，旧 build-index payload 主问题不是 payload refs 数量本身，而是 payload visit windows 过度碎片化
    - 在新增 counters 后，旧口径现场显示：
      - `payload_windows=259685`
      - `payload_refs=690785`
      - `3/9 70% -> 100% payload` 约 `97.270s`
  - 本轮收口：
    - 不再跨 segment 激进扩窗
    - 仅把“已经落在同一 covered segment slice 内”的 payload windows 合并
    - 同时修正 payload counters 生命周期，避免被 `fb_wal_finalize_record_stats()` 清零
  - 当前 live debug：
    - `fb_recordref_debug('scenario_oa_12t_50000r.documents', '2026-04-01 01:40:13+08')`
    - 当前返回：
      - `payload_windows=681`
      - `payload_covered_segments=714`
      - `payload_scanned_records=19603174`
      - `payload_kept_records=690785`
      - `elapsed=1:03.56`
  - 当前 live query：
    - `SET pg_flashback.memory_limit='8GB'`
    - `SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 01:40:13')`
    - 当前返回：
      - `count = 4356191`
      - `3/9 payload` 从约 `97.270s` 降到约 `16.403s`
      - `NOTICE [done] total elapsed 130953.442 ms`
      - shell 侧总耗时约 `2:11.24`
  - 当前 bundle 验证：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_progress fb_recordref'`
      结果：`All 2 tests passed.`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_v3 fb_summary_overlap_toast fb_replay'`
      结果：`All 3 tests passed.`
  - 2026-04-05 新增 live 现场：
    - `scenario_oa_50t_50000r.approval_comments @ '2026-04-04 23:40:13'`
    - 用户查询表面停在：
      - `3/9 70% payload`
    - 当前 root cause 已定位为新的 scan-mode 误判，不是 replay/apply 卡死：
      - gdb 栈稳定停在 `fb_wal_build_record_index() -> fb_wal_visit_sparse_windows() -> WALRead/pread64`
      - 当前现场变量已抓到：
        - `summary_span_windows=38666`
        - `payload_base_window_count=38666`
        - `payload_sparse_count=32184`
        - `payload_windowed_count=383`
        - `payload_parallel_count=383`
        - `payload_scan_mode=sparse`
        - `payload_covered_segments=407`
        - `summary_span_fallback_segments=0`
        - `metadata_fallback_windows=0`
      - 这说明当前不是 summary 缺失导致的 fallback 全扫，而是：
        - summary relation spans 本身极碎
        - query-side heuristic 仍选择 `sparse`
        - sparse path 对每个 emit window 都把 decode 起点拉回同一 segment slice 的段首
        - 同一批 covered segments 被重复 `XLogFindNextRecord()/XLogReadRecord()`，形成新的 `WalRead` 放大
  - 当前修复口径：
    - 不改 replay 语义，不重新扩大 payload decode 覆盖面
    - 第一优先级先修 query-side payload scan-mode 选择：
      - 对“`payload_sparse_count` 远大于 `payload_windowed_count`，且 `windowed` 已收敛到少量 covered slices”的 case，不再误选 `sparse`
      - 先优先回到 `windowed` 顺扫同一 covered slice
    - 同步补一条最小 RED，锁住这类碎 summary-span 现场不再回归为 `payload_scan_mode=sparse`
  - 当前已完成：
    - query-side heuristic 当前已改为同时参考 `payload_sparse_count`、`effective windowed window count` 与 `payload_covered_segments`
    - 对 `covered_segments <= 512` 且 `sparse_window_count < covered_segments * 128` 的场景，不再误选 `sparse`
    - 新增回归：
      - `fb_payload_scan_mode`
      - 锁住“碎 summary-span synthetic case 仍使用 summary spans，但最终必须选择 `payload_scan_mode=windowed`”
    - 当前定向验证：
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_payload_scan_mode fb_wal_parallel_payload fb_recordref fb_summary_v3'`
      - 结果：`All 4 tests passed.`
    - 当前 live spot-check：
      - `scenario_oa_50t_50000r.approval_comments @ '2026-04-04 23:40:13'`
      - `fb_recordref_debug(...)` 当前返回：
        - `payload_windows=383`
        - `payload_scan_mode=windowed`
        - `payload_covered_segments=407`
        - `summary_span_windows=38666`

- 已补 `pg_flashback_summary_progress` 的“最近查询 summary 降级”观测：
  - root cause：
    - 旧视图的 `progress_pct` 只代表 stable candidate 对应的 summary 文件是否齐全
    - 这会把“summary 文件覆盖率 `100%`”与“最近一次 flashback 查询没有回退到原始 WAL 扫描”混为一谈
    - live 复核也确认：即使 `progress_pct = 100`，查询侧慢点也可能来自别的阶段；因此用户需要单独看到“最近一次查询是否真的发生了 summary fallback”
  - 当前修复口径：
    - `fb_wal_build_record_index()` 结束时会上报最近一次查询的 summary fallback 结果到 summary service shared state
    - `pg_flashback_summary_progress` 新增 4 列：
      - `last_query_observed_at`
      - `last_query_summary_ready`
      - `last_query_summary_span_fallback_segments`
      - `last_query_metadata_fallback_segments`
    - 其中 `last_query_summary_ready=true` 表示最近一次 query-side build-index 没有发生 summary span / metadata fallback
    - 因而用户现在可以区分：
      - “summary 文件覆盖率是否完整”
      - “最近一次实际查询有没有回退到原始 WAL 扫描”
  - 当前验证：
    - 回归 `fb_summary_service` 已新增：
      - 删空 `meta/summary` 后跑一条真实 `pg_flashback(...)`
      - 断言视图立刻显示：
        - `last_query_summary_ready = false`
        - `last_query_summary_span_fallback_segments > 0`
        - `last_query_metadata_fallback_segments > 0`
      - 重建 summary 后再次查询，断言上述字段恢复到 ready/0
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_service fb_replay'`
      当前结果：`All 2 tests passed.`

- 已将“缺 checkpoint anchor”失败从慢路径收口到 build-index 入口附近：
  - root cause：
    - 旧实现只有在 `3/9 build record index` 完成 metadata / fallback 扫描后，才统一检查 `ctx->anchor_found`
    - 当 target 早于 retained WAL 可覆盖的最早 checkpoint 时，查询会先扫完整个大时间窗，最后才报
      `WAL not complete: no checkpoint before target timestamp`
  - 当前修复口径：
    - 在 `prepare wal` 后立即做一次 lightweight anchor probe
    - 该 probe 只为确认“retained WAL 内是否存在任何 checkpoint anchor”，不再等完整 metadata / payload 主扫描结束
    - 最终缺锚点报错统一收口为：
      - `WAL not complete: target timestamp predates earliest checkpoint in retained WAL`
      - 或 `WAL not complete: retained WAL contains no checkpoint record`
  - 当前现场复核：
    - `scenario_oa_12t_50000r.documents @ '2026-03-31 08:10:13'`
    - 原先需要约 `50s` 后才失败；当前表现为：
      - `[1/9]`
      - `[2/9]`
      - `[3/9 100%] (+59.976 ms)`
      - 随即报 `target timestamp predates earliest checkpoint in retained WAL`
    - 当前错误详情同时带出：
      - `target=2026-03-31 08:10:13+08`
      - `earliest_checkpoint=2026-03-31 22:31:30+08`
      - `first_retained_segment=00000001000000800000009F`
  - 新增回归：
    - `fb_wal_error_surface`
    - 已通过 `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg REGRESS=fb_wal_error_surface installcheck`

- 已收口 `fb WAL record spool session is not initialized` 的用户可读性：
  - root cause：
    - 旧实现直接把 `fb_index_ensure_record_log()` 内部的 spool/session 缺失文案抛给用户
    - 该报错暴露的是扩展内部临时工作区状态，而不是 WAL 完整性或用户输入问题
  - 当前修复口径：
    - 用户可见报错改为：
      - `pg_flashback internal error: temporary WAL workspace was not initialized for this query`
    - 同时补充 detail / hint，明确这是 query 级临时工作区初始化异常，而不是用户 SQL 写法问题
  - 新增回归：
    - `fb_wal_error_surface` 内新增 `fb_recordref_missing_spool_debug(regclass, timestamptz)` 专项覆盖
    - 当前已验证新文案稳定输出

- 已修复“连续删掉较早一段 WAL 后，prepare 阶段仍因更早 prefix gap 直接报 `missing segment`”的问题：
  - root cause：
    - resolver 在 [fb_wal.c](/root/pg_flashback/src/fb_wal.c) 原先对整条候选 segment 集做全局连续性校验
    - 只要较早位置存在一个不可恢复 gap，就会在 `2/9 preparing wal scan context` 直接报
      `WAL not complete: missing segment between ...`
    - 即使较新的连续 suffix 仍然完整，且 target 实际只需要这段 suffix，也会被前缀缺口误杀
  - 当前修复口径：
    - 选择逻辑改成“保留最新端连续 suffix”
    - 遇到不可恢复 gap 时，不再整条失败，而是丢弃 gap 之前的旧 prefix
    - 只有最新 suffix 自身仍不连续时，才继续视为真正的 WAL 不完整
  - 新增回归：
    - `fb_wal_prefix_suffix`
    - 该回归构造 `A,C,D` 三段 archive fixture，验证 prepare 会丢弃旧 `A`，保留最新连续 `C,D`
  - 当前验证：
    - `fb_wal_prefix_suffix fb_wal_sidecar` 已全部通过
    - 现场 `alldb` 上，原先在 `2/9` 报 `missing segment between 000000010000007F000000FF and 00000001000000800000009F`
      的查询，现在已能进入 `3/9`
    - 同一条 query 当前改为报 `WAL not complete: no checkpoint before target timestamp`，说明“前缀 gap 误杀”已消失，剩余失败原因已切到真实 anchor 覆盖不足

- 已修复 `pg_flashback_summary_progress` 对 archive 删洞场景的误报：
  - root cause：
    - progress 统计原先只看“当前 candidate 是否已有 summary 文件”，没有把当前 WAL 连续性缺口折算进 `missing_segments` / `first_gap_*`
    - 同时 progress 复用了 `skip_unstable_tail=true` 的 candidate 口径，连续 run 的尾段会被天然裁掉，进一步掩盖真实 gap
    - 当用户清理一部分较老归档 WAL 后，查询路径会先因 `WAL not complete` 失败，但 progress 仍可能给出一段看似正常的 stable 时间窗
  - 当前修复口径：
    - `fb_summary_service_collect_progress()` 改为基于完整 candidate 集统计，不再沿用 `skip_unstable_tail=true`
    - 将相邻 segno 间的 WAL 洞折算进 `missing_segments`
    - `first_gap_from_newest_*` / `first_gap_from_oldest_*` 现可直接指向真实 WAL 缺口，而不只指向“缺 summary 的现存 segment”
    - 当 shared snapshot 与当前会话可见 candidate 完全错位时，progress 会自动回退到“直接按当前 candidate 统计”，避免 session 级 `archive_dest/debug_pg_wal_dir` 下整张视图失明
  - 当前验证：
    - `fb_summary_service` 单测已通过
    - `fb_summary_service fb_summary_v3 fb_wal_sidecar` bundle 已通过
    - `alldb` 现场当前显示：
      - `missing_segments = 793`
      - `first_gap_from_newest_segno = 32926`
      - `first_gap_from_oldest_segno = 31488`
      - `progress_pct = 46.20081411126187`
    - 同时原用户 SQL 仍会快速报 `WAL not complete: missing segment ...`，说明观测面已与 query-side 事实对齐

- 已修复 serial payload path 在 split materialize windows 下漏掉跨 segment 记录头的问题：
  - root cause：
    - `fb_split_parallel_materialize_windows()` 只借 trailing decode-tail，不借 leading decode-head
    - 在 payload bgworker 已硬关闭后，串行 fallback 仍沿用这套 split windows
    - 当目标 heap record 从前一段起始、在当前段结束时，后一个 split chunk 会从段边界起扫并直接跳过该 record
    - 现场最小复现为 `fb_flashback_toast_storage_boundary`，失败点为跨 `80/5C` -> `80/5D` 的 heap insert，后续 replay 在同页 `off=5` 时报 `failed to replay heap insert`
  - 当前修复口径：
    - 在 payload bgworker 重新启用前，串行 payload 路径不再调用 `fb_split_parallel_materialize_windows()`
    - 保持 merged payload windows 原样读取，避免 split chunk 裁掉跨段记录头
  - 当前验证：
    - `fb_flashback_toast_storage_boundary` 已恢复通过
    - `fb_wal_parallel_payload fb_wal_sidecar fb_summary_overlap_toast fb_summary_v3 fb_flashback_toast_storage_boundary fb_toast_flashback fb_recordref` 已全部通过
    - live case `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'` 在 `memory_limit='8GB'` 下仍可完整返回 `count = 4356409`

- 已止血 `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'` 的 postmaster 级 crash：
  - 现场证据：
    - 崩溃时 PostgreSQL 日志报 `PANIC: stuck spinlock detected at get_hash_entry, dynahash.c:1268`
    - core 落在 autovacuum `LockAcquire -> SetupLockInTable -> get_hash_entry`
    - 崩溃窗口内用户查询正在走 dynamic payload bgworker/DSM 路径
  - 当前处置：
    - 先硬关闭 `fb_wal_materialize_payload_parallel()`，强制回落串行 payload materialize
    - 保留原有 WAL payload 串行路径，避免 meta 未及时生成或并行路径不稳时影响 correctness
    - 追加 full-range anchor fallback，避免关闭 bgworker 后 `fb_wal_sidecar` 因预过滤窗口裁掉 checkpoint 而误报 `no checkpoint before target timestamp`
  - 当前验证：
    - `fb_wal_parallel_payload` 已同步为“并行请求下仍走串行 payload worker=0”口径
    - `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'`：
      - `memory_limit='6GB'` 现返回真实 preflight 内存报错，不再 crash
      - `memory_limit='8GB'` 已完整跑通并返回 `count = 4356409`
    - 最新 `postgresql-2026-03-31_214335.log` 已无新的 `PANIC` / `stuck spinlock` / postmaster recovery 记录

- 已修复 summary-first payload 窗口残留重叠导致的 replay 重放错误：
  - root cause：
    - `fb_summary_segment_lookup_spans_cached()` 会把 main/toast/reltag 命中的多套 relation spans 直接拼接返回
    - 查询侧旧实现只做 append/clip，不对 summary span windows / payload windows 做全局 merge
    - live case 与最小 toast-heavy case 都会把同一段 WAL 重复 materialize，先后触发：
      - `failed to replay heap insert`
      - 消掉重复 insert 后继续暴露的 `failed to locate tuple for heap delete redo`
  - 当前修复口径：
    - 新增 summary/payload visit window 的全局排序 + merge
    - 串行 payload materialize 增加 `payload_emit_start_lsn` 单调推进，物理读范围保留，逻辑发射不再重复 append 同一条 record
    - 原有 summary 缺失时的 WAL fallback 路径保持不变
  - 新增回归：
    - `fb_summary_overlap_toast`
  - 当前验证：
    - `fb_summary_overlap_toast fb_summary_v3 fb_flashback_toast_storage_boundary fb_toast_flashback fb_recordref fb_wal_sidecar` 已全部通过
    - 清空 `/isoTest/18pgdata/pg_flashback/meta/summary` 后复跑 `fb_recordref fb_flashback_toast_storage_boundary` 通过，确认 meta 未及时生成时旧路径仍可正确回退
    - 用户 live case `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'`：
      - `memory_limit='6GB'` 当前不再报 replay 错误，改为真实 preflight 内存报错
      - `memory_limit='8GB'` 已完整跑通并返回 `count = 4356409`

- 已修复 `3/9 build record index` 阶段的一处 summary-prefilter pthread 崩溃：
  - root cause：
    - segment prefilter 的 pthread worker 在 `fb_summary_load_file()` 路径内调用了 `fb_runtime_meta_summary_dir() / psprintf() / palloc()`
    - PostgreSQL backend 内存上下文分配器不支持该 pthread 用法，导致 backend `SIGSEGV`
  - 当前修复口径：
    - `fb_summary_load_file()` 改为走线程安全的栈上 `snprintf()` summary path 拼接
    - prefilter worker 不再在 pthread 内触碰 PostgreSQL `palloc/psprintf`
  - 用户 case `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'`：
    - 原先在 `NOTICE [3/9 0%]` 后直接把 backend 打挂
    - 当前已可稳定走完整条 flashback 主链，不再触发 server crash

- 已修复 WAL payload 并行阶段的一处失败退化问题：
  - root cause：
    - live case 上 payload bgworker 不能稳定保留时，leader 会主动 `TerminateBackgroundWorker()`
    - 旧实现随后仍走“worker 必须 `DONE`”的正常 wait 校验，把回退路径再次抬成 `ERROR`
  - 当前修复口径：
    - payload worker 注册/启动失败时回退串行 payload 基线
    - 主动终止已启动 worker 后，只等待关停，不再要求这些 worker 回报 `DONE`
  - 当前 live 验证：
    - `fb_wal_parallel_payload` 回归已通过
    - `scenario_oa_12t_50000r.documents @ '2026-03-29 14:10:13'` 默认 `memory_limit=1GB` 下当前返回真实 preflight 报错，不再 crash

## 进行中

- 已完成 `block-anchor summary v1` 首版接线：
  - root cause：
    - 现有 `summary v3` 已能压缩 `3/9 build record index` 的 segment/range/xid 工作
    - 但 `4/9 replay discover` 的缺页锚点解析仍主要依赖 `record_log` 反向扫
    - summary 当前天然产出的是 `LSN` 级事实，而 replay backtrack gate 仍以 query-local `record_index` 为主
  - 当前实现口径：
    - `summary-*.meta` 已新增 relation-scoped block anchor section
    - summary build 已记录主表/TOAST relation 的 `FPI/INIT_PAGE` block anchor facts
    - 查询侧已优先用 block anchor summary 解析 missing-block 的最近可用 anchor
    - replay backtrack gate 已从 `record_index` 收敛到 `anchor_lsn`
    - `warm` pass 已按最早 `anchor_lsn` 定位回放起点
    - 现有 relation/xid summary section、summary cache 与 WAL fallback 语义保持兼容
  - 新增调试/回归口径：
    - 回归 `fb_replay` 已改为显式构建 summary，并校验 block-anchor section 可读且 flashback 结果正确
    - 新增 regression-only helper `fb_summary_block_anchor_debug(regclass)` 用于核对 summary 中是否存在目标 relation 的 block anchors
  - 本轮验证：
    - 手工 `sql/fb_replay.sql`：通过
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_replay fb_summary_v3 fb_recordref'`：`All 3 tests passed.`

## 下一步

- 在 correctness 已锁住的前提下，继续定位 `FB_WAL_BUILD_COUNT_ONLY` / `FbCountAggScan` 的真实 root cause，并准备重新开放 count-only 优化
- 将开源镜像双语文档模板收口到 `scripts/sync_open_source.sh` 的长期同步流程中，避免后续刷新时回退到旧单语文档
- 将 `docs/superpowers/specs/2026-04-03-release-gate-design.md` 细化为正式实现计划
- 为 `tests/release_gate/` 建立独立脚本与配置骨架
- 固化 `PG14-18` golden baseline 文件结构、双阈值阻断规则与 Markdown 报告模板
- 继续回到 batch B / deep pilot 的真实 replay blocker：
  - 当前回归面已绿，优先处理 live case 的页态语义根因
  - 先围住当前文档里挂着的 cold-run first blocker：
    - `87/17F73AB8 / rel=1663/33398/16395737 / blk=38724 / off=1 / failed to locate tuple for heap delete redo`
- 继续补 deep / 冷缓存验证：
  - `installcheck` 已恢复 `All 36 tests passed.`
  - 当前尚未完成的是：
    - `tests/deep/` 同步到当前内嵌恢复模型后的复跑
    - 冷缓存/更长时间窗下的 batch B 现场复核
- 已完成 `replay discover` 静态 missing-block 预计算：
  - 当前实现：
    - 在 `fb_wal_finalize_record_stats()` 里顺序扫描 `RecordRef`
    - 按 block 做 lightweight initialized/no-op/missing 模拟
    - 预先生成 `precomputed_missing_blocks`
  - 当前 replay 口径：
    - 若 `precomputed_missing_blocks = 0`，则 `4/9` 不再跑 page-level discover round
    - `5/9 replay warm` 也随之直接快跳
    - 只有真正缺页基线的 block 才继续走 `anchor resolve + warm`
  - 当前回归：
    - `fb_replay` 已新增 `fb_replay_debug()` 断言：
      - `precomputed_missing_blocks=0`
      - `discover_rounds=0`
    - `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_replay fb_summary_v3 fb_recordref'`
      当前结果：`All 3 tests passed.`
  - 当前 live 验证：
    - 旧用户时间点 `2026-04-01 08:10:13/09:10:13/10:10:13/11:10:13 +08` 现已超出 retained WAL，无法继续原样复跑
    - 在同一张 `scenario_oa_12t_50000r.documents` 上复测近端有效时间窗：
      - `fb_replay_debug(..., '2026-04-01 17:07:22+08')`
      - `fb_replay_debug(..., '2026-04-01 17:11:14+08')`
      - `fb_replay_debug(..., '2026-04-01 17:13:14+08')`
      - 三次都返回：
        - `precomputed_missing_blocks=0`
        - `discover_rounds=0`
        - `discover_skipped=1`
    - 同表实际查询：
      - `SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 17:13:14+08')`
      - 当前阶段表现为：
        - `[4/9 0%] replay discover precomputed`
        - `[4/9 100%] ... (+0.073 ms)`
        - `[5/9 0%] replay warm (+0.022 ms)`
        - `[5/9 100%] ... (+0.022 ms)`
      - 证明 `4/9` / `5/9` 在原表 live case 上已被压到零头
- 在 live case 上继续验证 `block-anchor summary v1` 对 `4/9 replay discover` / `5/9 replay warm` 的真实收益
- 评估是否继续扩展到 block span-driven replay 窄化
- 保持 `summary` 缺失/损坏时继续安全回退到旧 WAL 路径

## 进行中

- 已完成 `scenario_oa_12t_50000r.documents @ '2026-04-01 08:10:13'` 现场
  `ERROR: failed to replay heap insert` 的复核与止血
  - 当前复核结论：
    - 最新 build 安装并重启 PG18 后，同一条 live query 已不再复现该错误
    - `SET pg_flashback.memory_limit='8GB'` 后：
      - `SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 08:10:13')`
      - 当前返回 `4356191`
    - `SET pg_flashback.memory_limit='1GB'` 后，同一条 SQL 当前只会命中真实 preflight：
      - `ERROR: estimated flashback working set exceeds pg_flashback.memory_limit`
      - `estimated=5097596604 bytes`
      - `limit=1073741824 bytes (1 GB)`
  - 当前定位结论：
    - 旧现场失败块为 TOAST relation `1663/33398/16395804 blk 213310`
    - 原报错点为 `lsn=81/71035B40 off=27 maxoff=20`
    - 但补查 record spool 后，已确认同块关键 WAL 记录均已进入索引：
      - `INSERT off 21/22` at `81/70829D68` / `81/70829E88`
      - `INSERT off 23/24` at `81/70905EE0` / `81/70906018`
      - `INSERT off 25/26` at `81/70A32D68` / `81/70A32EC0`
      - `DELETE off 21/22` at `81/70BFCAD8` / `81/70BFCB10`
      - `INSERT off 27/28` at `81/71035B40` / `81/71035C60`
    - 追加 replay trace 也已确认该页在最新 build 上按顺序推进到：
      - `off 21 -> maxoff 21`
      - `off 22 -> maxoff 22`
      - `off 23 -> maxoff 23`
      - `off 24 -> maxoff 24`
      - `off 25 -> maxoff 25`
      - `off 26 -> maxoff 26`
      - delete `21/22` 后保持 `maxoff 26`
      - `off 27 -> maxoff 27`
      - `off 28 -> maxoff 28`
    - 因此当前可以确认：
      - 这条旧错误不是 record spool 漏收该段 WAL
      - 在最新安装态下，该 live case 已不再卡在 heap insert replay
  - 当前后续口径：
    - 该用户 case 现阶段已从 replay blocker 转为正常资源约束问题
    - 若继续追根因，应针对“旧 build / 旧 backend 安装态下为何出现一次性脏现场”单独做版本/部署侧复盘，而不是继续按当前代码逻辑缺陷处理
  - 本轮内核调试新增结论：
    - 已对 `summary -> payload window -> replay` 做对照实验，确认 `failed to replay heap insert` 不是 `fb_replay_heap_insert()` 原语本身的缺陷
    - live case 的 raw summary spans 确实天然乱序；临时 trace 直接打出了同一查询中的大量逆序样本，例如：
      - `prev=80/C7FB7FC0 curr=80/C714B588`
      - `prev=80/C8F85B68 curr=80/C8026FA0`
    - 在临时禁用：
      - summary span 全局 merge
      - payload window 全局 merge
      - `payload_emit_floor` 单调推进
      后，同一条 live SQL 会重新退化为 replay correctness 错误：
      - `ERROR: failed to locate tuple for heap delete redo`
      - `lsn=80/CC0CB580 rel=1663/33398/16395804 blk=18225 off=21`
    - 这说明当前 correctness 真正依赖的是：
      - summary-derived visit windows 必须先全局排序/merge
      - payload materialize 还必须保留 `emit_floor` 去重/单调推进
    - 因而原先 `blk 213310 @ 81/71035B40 off=27 maxoff=20` 的 `failed to replay heap insert`
      应归因于“payload 输入序列出现非单调/重叠导致的页状态落后”，而不是 heap insert redo 自身坏掉
    - 当前代码里这条根因已经由：
      - summary/payload window 全局 merge
      - `payload_emit_start_lsn` 单调推进
      两层修复共同兜住

- 已修复两处 `pg_flashback()` 查询面 correctness / stability 问题：
  - `FbApplyScan` 已改为把 apply 内部结果 copy 到 `ss_ScanTupleSlot` 后再返回
  - 修复了“历史 residual 行参与上层表达式投影/排序时，CustomScan 直接返回内部 slot 导致 executor 读错 scan tuple 并崩溃”的问题
  - 最小复现 `fb_delete_fpw_target`：
    - 删除后查询 `SELECT id, length(pad) ... FROM pg_flashback(...) ORDER BY 1;` 不再触发 `SIGSEGV`
    - 已恢复正确返回历史行
  - keyed 单列 typed-key fast path 的单 key hash 现已与通用 keyed-entry hash 保持一致
  - 修复了 `WHERE id = const` / `WHERE id IN (...)` 快路径漏命中 changed-key entry、误返回当前行或漏发已删除历史行的问题
  - 用户 case `scenario_oa_12t_50000r.documents` 已确认：
    - `target_ts='2026-03-29 20:00:13' AND id=10` 现已返回 `1` 行
    - 反向 delete 对应 commit 时间为 `2026-03-29 22:42:32.744216+08`
    - `target_ts='2026-03-29 22:40:13' AND id=10` 返回 `1` 行，`target_ts='2026-03-29 22:45:00' AND id=10` 返回 `0` 行，边界符合 WAL 事实
  - 已补强回归：
    - `fb_keyed_fast_path` 追加“deleted residual row + 表达式投影”检查
    - `fb_flashback_hot_update_fpw` / `fb_custom_scan` 复跑通过

- 已继续收敛 `FbApplyScan` 串行热路径：
  - keyed 单列 typed key 的当前表扫描命中路径已改成 direct probe，不再每行都走 `Datum[]/bool[] + hash_combine + 通用 typed lookup`
  - 单列 typed key 的 miss 负判定已升级为双 hash bloom 风格过滤，进一步减少“未变化 key”误入精确索引
  - `FbApplyScan` 的 zero-copy 输出已改成“直接把真实结果 slot 交给 `ExecScan`”：
    - 当前行原样返回时直接复用 scan slot
    - replacement / residual 行回落到 apply 自有 relation slot
    - 避免把 buffer heap tuple 强塞进 executor 的 virtual scan slot
  - 当前在 `scenario_oa_12t_50000r.documents`、`parallel_workers=0`、`memory_limit='6GB'` 的 fresh 单 backend 观测中：
    - stage `8 apply` 从上一轮观测约 `49.4s` 压到约 `29.2s`
    - 当前已先拿到 apply 串行热路径上的显著收益

- 已新增主链并行改造实施计划：
  - 计划文件：`docs/superpowers/plans/2026-03-29-flashback-parallel-main-pipeline-plan.md`
  - 固定分阶段顺序：
    - Phase 1：`parallel_workers=0` 仍保留串行 prefilter 基线
    - Phase 2：WAL payload / materialize pass 并行
    - Phase 3：WAL metadata 两段式并行
    - Phase 4：replay / reverse-source 分片并行
    - Phase 5：apply 通用并行
  - 当前所有阶段都必须保持：
    - 相同的 `anchor`
    - 相同的 `unsafe`
    - 相同的 `xid_statuses`
    - 相同的 `RecordRef` 语义
    - 相同的最终历史结果

- 已拍板将 flashback 主链并行控制统一收口为：
  - 新参数固定为 `pg_flashback.parallel_workers`
  - 语义固定为：
    - `0`：flashback 主链强制串行
    - `> 0`：允许 flashback 主链进入并行实现，参数值即并行 worker 上限
    - `< 0`：非法
  - 旧的 `pg_flashback.parallel_segment_scan` 将删除，不再保留兼容
  - 当前不改变 `pg_flashback.export_parallel_workers` 的 keyed 导出 worker 语义
  - 当前并行改造目标固定为 flashback 主链中“可在不改变语义前提下并行”的阶段：
    - resolver / sidecar
    - WAL segment prefilter
    - WAL metadata scan
    - WAL payload / materialize
  - 当前明确不做：
    - 单条 `XLogReader` 顺序流内部并行
  - 决策记录：`docs/decisions/ADR-0017-unified-flashback-parallel-switch.md`

- 已完成 flashback 并行控制第一阶段落地：
  - `pg_flashback.parallel_workers` 已进入代码与回归
  - `pg_flashback.parallel_segment_scan` 已从正式 GUC 面删除
  - 现有 WAL segment prefilter 已切到统一 worker 参数
  - `pg_wal` 段身份校验已增加安全的文件级并行
  - checkpoint sidecar anchor hint 扫描已增加安全的文件级并行
  - 当前仍未完成：
    - replay / reverse-source 的主路径并行
    - apply 的主路径并行

- 已确认当前缓存对重复 flashback 查询有实际加速作用：
  - 当前缓存层包含：
    - backend 内 `prefilter cache`
    - `DataDir/pg_flashback/meta` 下的 `prefilter/checkpoint` sidecar
    - `DataDir/pg_flashback/recovered_wal` 下的恢复段复用缓存
  - live 诊断中，对 `scenario_oa_12t_50000r.documents @ 2026-03-29 19:30:50+08` 连续执行 `fb_recordref_debug(...)`：
    - 同一 backend 约 `58.5s -> 28.7s`
    - 随后新 backend 再跑一轮约 `33.1s`
  - 同次诊断后，`/isoTest/18pgdata/pg_flashback/meta` 中出现了 `1624` 个 `prefilter-*.meta`
  - 当前判断：
    - 缓存对重复查询的加速已成立
    - 收益主要集中在 WAL prefilter / sidecar / index 构建前置阶段
    - replay / apply 阶段仍会继续受 CPU、内存访问、worker 调度和 PostgreSQL buffer 命中率影响而波动

- 已完成主链并行 Phase 1：
  - `parallel_workers=0` 不再关闭 prefilter，而是保留串行 prefilter 基线
  - 当前 `parallel_workers` 只决定 worker fanout 数，不再决定 prefilter 是否启用
  - `fb_recordref_debug` 已追加：
    - `parallel=on|off`
    - `prefilter=on|off`
    - `visited_segments=x/y`
  - 活跃回归 `fb_recordref` / `fb_wal_sidecar` 已覆盖默认 `parallel=off + prefilter=on`

- 已拍板取消 `DataDir/pg_flashback/*` 的自动删除逻辑：
  - 删除 `pg_flashback.runtime_retention`
  - 删除 `pg_flashback.recovered_wal_retention`
  - 删除 `pg_flashback.meta_retention`
  - 删除 runtime 初始化时的 cleanup
  - `recovered_wal/`、`meta/` 继续不做查询结束 cleanup
  - `runtime/` 后续单独恢复为“查询结束安全 sweep”，不回摆 retention GUC

- 已修复 `recovered_wal` 的一处不必要 materialize 问题：
  - 真实用户 case：
    - `SELECT * FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-03-29 19:30:50') WHERE id BETWEEN 1000 AND 2000`
    - `parallel_workers=8`
  - root cause：
    - resolver 会先验证 `pg_wal` 里的 recycled / mismatched segment
    - 旧实现会在“按 segno 做 archive 优先选择”之前，提前把所有 mismatch `pg_wal` 一律 convert 到 `recovered_wal`
    - 即使 archive 已经有对应真实 segment，也会白白把旧段 materialize 一遍
  - 当前修复口径：
    - 若 mismatch `pg_wal` 文件头指向的真实 segno 已有 archive / 可用候选覆盖，则不再 convert
    - 同时把该错名 `pg_wal` candidate 标记为 ignored，避免后续按错误 segno 再触发缺段报错
  - 当前已补自动化回归：
    - `fb_recovered_wal_policy`
  - 当前 live 验证结果：
    - 清空 `/isoTest/18pgdata/pg_flashback/recovered_wal` 后复跑上述用户 case
    - 查询完成返回 `1001`
    - `recovered_wal` 保持为空，没有再生成 `7C/45..89`

- 已完成 `recovered_wal` 语义继续收紧：
  - 用户要求彻底删除“archive exact-hit 也复制到 `recovered_wal`”这条路径
  - 最终口径：
    - archive 有目标 segment 时，resolver 直接消费 archive
    - `recovered_wal` 只复用“已按真实 segname 物化好的恢复段”
    - 只有 `pg_wal` 已被覆盖/错配，且 archive 中不存在对应真实 segment 或 archive 未开启时，才允许把 WAL 物化到 `recovered_wal`
    - archive 中即使存在错名文件，也不再通过 `fb_ckwal_restore_segment()` 物化到 `recovered_wal`
  - 代码收口：
    - `src/fb_ckwal.c` 新增“只找错名 `pg_wal`”的 helper
    - `fb_ckwal_restore_segment()` 不再扫描 archive 做 exact-hit copy，也不再从 archive 错名文件恢复
    - `fb_ckwal_convert_mismatched_segment()` 保留为“resolver 已确认错配 `pg_wal`”时的显式真实名物化入口
  - 自动化验证：
    - `fb_recovered_wal_policy`
      - archive exact-hit + 错名 `pg_wal` 时，`recovered_wal` 保持为空
      - archive 错名文件时，`recovered_wal` 保持为空
      - archive 缺失且仅有错名 `pg_wal` 时，按真实 segname 生成一份 `recovered_wal`
    - `fb_wal_source_policy`
    - `fb_wal_prefix_suffix`
    - `fb_wal_error_surface`
  - 本轮验证命令：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS=fb_recovered_wal_policy`
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS="fb_wal_source_policy fb_wal_prefix_suffix fb_wal_error_surface"`
  - 本轮验证结果：
    - 以上 4 条回归全部通过

- 已完成 summary 服务 recent-first 调度与进度可观测面：
  - launcher / worker 调度已从“隐含倒序入队”收口为显式冷热双队列
  - worker 当前固定为：
    - 先领取 hot 任务
    - 同类任务内按更近的 segno 优先
  - 新增旧版 `fb_summary_progress` 视图：
    - 展示 stable candidate 总数
    - 已完成 / 未完成 summary 数
    - hot/cold missing 数
    - `covered_through_ts`，表示从最新稳定 segment 往回连续已完成 summary 的最老事务时间点
    - `snapshot_timeline_id/snapshot_oldest_segno/snapshot_newest_segno`
    - `snapshot_hot_candidates/snapshot_cold_candidates`
    - `cold_covered_until_ts`，表示从最老稳定 segment 往新方向连续已完成 summary 的最晚事务时间点
    - queue 的 `pending/running` hot/cold 状态
    - launcher/worker/counters 与 summary 文件大小
  - 视图口径已收口为：
    - 以 `last_scan_at` 对应的 stable segment snapshot 为准
    - 不直接拿“当前文件系统最新 tail”做分母
    - recent tail 持续增长时，进度仍按上一轮 snapshot 稳定展示
  - 已补自动化回归：
    - `fb_summary_service`
  - 已补服务级恢复：
    - worker 重启后遗留的 stale `RUNNING` task 会回收为 `PENDING`
    - 进度视图不再把 stale task 误算成活跃运行中

- 已拍板重做 summary 进度用户视图：
  - 旧 `fb_summary_progress` 语义对用户不直观
  - 当前已改为：
    - 用户主视图 `pg_flashback_summary_progress`
    - 调试视图 `pg_flashback_summary_service_debug`
  - 用户主口径当前直接表达：
    - stable 时间窗
    - 近端连续覆盖前沿
    - 远端连续覆盖前沿
    - 从新端/旧端观察到的第一个 gap
  - launcher / worker / queue / cleanup 等内部字段已迁到 `pg_flashback_summary_service_debug`
  - 新的用户可见 summary 观测面当前统一使用 `pg_flashback_` 前缀，不再新增 `fb_*` 用户对象
  - 当前专项验证结果：
    - `PGPORT=5832 PGUSER=18pg make ... installcheck REGRESS='fb_summary_service fb_summary_v3 fb_wal_sidecar'`
    - `All 3 tests passed.`
  - 决策记录：
    - `docs/decisions/ADR-0022-summary-progress-surface-redesign.md`

- 已完成 summary v3 紧凑 segment 索引第一版落地：
  - `summary-*.meta` 已升级为 versioned 多 section 格式
  - 在原有 bloom gate 之上新增：
    - relation dictionary
    - relation spans
    - xid outcomes
  - 查询侧当前口径：
    - metadata / anchor 仍走 prefilter window，保持 checkpoint / unsafe 语义不丢
    - payload materialize 已改为“summary spans 优先，缺失 segment 回退 coarse window”
    - xid status 补齐已改为“summary outcome 优先，WAL 回扫兜底”
    - unsafe-only xid（如 truncate / rewrite / storage change）已纳入 summary outcome / fallback 统一补齐
  - 当前已补自动化回归：
    - `fb_summary_v3`
    - `fb_recordref`
    - `fb_wal_sidecar`
    - `fb_wal_parallel_payload`
    - 联合 `fb_runtime_cleanup` / `fb_runtime_gate` / `fb_spill`
  - 当前专项验证结果：
    - `PGPORT=5832 PGUSER=18pg make ... installcheck REGRESS='fb_summary_v3 fb_recordref fb_wal_sidecar fb_wal_parallel_payload fb_runtime_cleanup fb_runtime_gate fb_spill'`
    - `All 7 tests passed.`

- 已拍板启动 `3/9 build record index` 的 summary-first 第二阶段收敛：
  - 当前 live case 复盘已确认：
    - summary 预建覆盖本身已完整，不是“没建 summary”
    - 当前 `3/9` 的主要剩余热点之一是 `summary xid outcome` 回填仍按 `resolved_segment_count` 全段顺序打开并读取 `summary-*.meta`
    - metadata 主循环当前仍以 WAL 顺序扫描为主，summary 只承担 prefilter/span 缩窗，不足以把 `3/9` 真正打下来
  - 本轮已拍板的新口径：
    - 查询侧把 summary 从“辅助过滤”升级为 `3/9` 的主索引
    - `xid fill` 只允许读取命中 window 覆盖到的 summary segment，不再按全量 resolved segment 扫描
    - 查询期新增 backend-local summary section cache，避免同一查询内反复 `open/read/close` 相同 summary 文件
    - 在保持 `meta/summary` 低存储原则下，新增 relation-scoped `touched xids` 与紧凑 `unsafe facts` section
    - metadata 主路径改为：
      - summary-first 先收敛 `touched xids` / `unsafe facts`
      - xid status 继续优先吃 summary outcomes
      - 仅在 summary 缺失、损坏或覆盖不足的 window 上回退 WAL 扫描
      - 原有 metadata/xact WAL 路径继续保留，承接 meta 未及时生成场景
    - checkpoint / anchor 继续复用现有 checkpoint sidecar，不并入 summary
  - 本轮验证要求固定为：
    - 修改后必须手动删除现有 `DataDir/pg_flashback/meta/summary`
    - 重新预建 summary
    - 再复跑回归与 live 调试 case，确认不是吃旧 meta 假收益
  - 决策记录：
    - `docs/decisions/ADR-0023-summary-first-record-index.md`

## 当前进行中

- 已确认并开始修复 `3/9 xact-status` 的全量回退放大问题：
  - 当前 live case 现场已确认：
    - `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
      的主要长尾已不再是 `payload locator / pg_qsort`
    - backend 当前热点落在：
      - `fb_wal_fill_xact_statuses_serial`
      - `fb_wal_visit_window`
      - `WALRead`
    - 同次 `gdb` 现场已确认：
      - `window_count = 97705`
      - 当前 `3/9 30% metadata` 后长时间空白实际属于
        `xact-status`，不是 metadata 主循环本身
  - 当前根因已确认有两层：
    - `fb_summary_fill_xact_statuses()` 当前仍是
      “all-or-nothing” 语义：
      - 只要还有任意 `touched/unsafe xid` 未由 summary outcome 解出
      - 就返回 `false`
      - 调用方随即回退到旧的串行 WAL xact 扫描
    - 当前串行回退仍直接复用高碎片 relation/span windows：
      - 即使 summary 已经解出大部分 xid
      - 也会按整批碎窗口重扫 `RM_XACT_ID`
      - 还会重复触达已由 summary 命中的 xid
  - 当前已拍板的新修复口径：
    - 保持 correctness 优先，不接受“少量 unresolved 直接忽略”
    - 但把 fallback 从“整批 touched/unsafe xid + 整批碎窗口”
      收敛为：
      - summary 先写入已命中的 xid status
      - 仅为 unresolved xid 构建 fallback 集
      - xact fallback 使用专用 coalesced windows，不再逐个 relation span
        细碎读取 WAL
    - 同时补 query/debug 观测，至少暴露：
      - xact fallback unresolved xid 数
      - xact fallback windows / covered segments
  - 当前验收目标固定为：
    - 用户现场 SQL
      `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
      的 `3/9` 阶段耗时压到 `< 20s`
    - 同时不能破坏现有 summary-first / WAL fallback 正确性

- summary 预建服务第一版已落地：
  - 查询侧已切到 summary-first prefilter：
    - 优先读取 `meta/summary`
    - 缺失时回退旧 `mmap + memmem`
    - 不再持续写入 relation-pattern 级 `prefilter-*.meta`
  - `2026-04-02` 当前新增收敛目标：
    - 优先提升历史 cold backlog 的 summary 补齐速度，而不是继续把大部分 worker 预算放在 recent frontiers
    - 服务调度准备改成：
      - 固定 `1` 个 hot-first worker
      - 其余 worker 默认 cold-first
      - cold worker 支持小批量连续 segment claim
    - `pg_flashback_summary_progress` 准备新增：
      - `estimated_completion_at`
      - 口径固定为“最近 build 速率下的 backlog ETA”，样本不足时返回 `NULL`

- `3/9 build record index` 当前主线收敛中：
  - 第一优先级不再是 metadata 并行 fanout
  - 当前改为先把 summary-first 主索引打完整：
    - 压掉全量 summary xid scan
    - 压掉 metadata 串行 WAL 主循环中的可由 summary 直接回答的工作
    - 同时保持 `meta/summary` 继续走紧凑低存储口径，并保留原有 WAL fallback
  - `2026-04-01` 当前新增收敛目标：
    - live case 上 `3/9 100% -> 4/9 0%` 之间仍有明显空白，已确认这段时间不应再归因给 `4/9 replay discover`
    - 当前要先把 `3/9` 拆成固定子相位：
      - `prefilter`
      - `summary-span`
      - `metadata`
      - `xact-status`
      - `payload`
    - 再继续收窄 build-index 尾段 payload work，避免用户被旧进度面误导
  - `2026-04-05` 当前新增现场结论：
    - `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'` 复测显示：
      - `3/9 xact-status` 约 `16.7s`
      - `3/9 payload` 约 `43.3s`
      - 总耗时约 `62.9s`
    - 当前已新增一轮 query-local summary xid outcome 零拷贝收口：
      - `fb_summary` 现已在 query cache 内保留 `xid outcomes` public slice
      - `xact-status` 不再为每个命中 segment 额外复制整段 outcome 数组
      - 现阶段 live case 上 `xact-status` 已从约 `19s` 档位压到约 `16-17s`
    - 已验证一条错误方向：
      - 直接按“xid 已知 before-target / aborted”裁掉 payload spool 会破坏 replay 正确性
      - 这说明部分此类 record 仍承担页状态/块初始化约束，不能按事务语义直接删除
    - 当前稳定结论：
      - `xact-status` 还能继续往 summary 侧压，但必须建立新的 per-segment xid presence/index，不能再只靠 query 期语义剪枝
      - 当前 live case 的头号热点仍然是 payload materialize/spool，而不是 `xact-status`

- runtime 自动清理口径已重新拍板：
  - 恢复“查询结束触发的 runtime 安全 sweep”
  - 只清理 `DataDir/pg_flashback/runtime`
  - 不恢复 `runtime_retention` 一类保留期 GUC
  - 不恢复 `recovered_wal/meta` 的通用 cleanup
  - 目录扫描时仅删除 owner backend 已失活的：
    - `fbspill-<pid>-<serial>/`
    - `toast-retired-<pid>-<serial>.bin`
  - 当前实现已完成：
    - 本查询自己的 `fbspill-*` 会在 `FbSpoolSession` destroy 时直接删目录
    - 本查询自己的 `toast-retired-*` 会在 toast spill release 时直接删文件
    - 查询结束时会再扫一轮整个 `runtime/`，清理 dead owner 残留
  - `fb_runtime_cleanup` / `fb_spill` / `fb_runtime_gate` 回归已通过
  - 已新增 segment 通用 summary：
    - `locator_bloom + reltag_bloom`
    - 文件落在 `DataDir/pg_flashback/meta/summary`
    - 文件 identity 已升级到纳秒级 `mtime/ctime`，避免 `pg_wal` 活动段内容变化时误复用旧 summary
  - 已新增 `shared_preload_libraries` 下的 summary 服务：
    - 1 个 launcher
    - shared queue
    - 多 worker pool
    - worker 只读 WAL 文件并写扩展私有 `meta/summary`
  - 已实现自动清理：
    - 只清理 `meta/summary`
    - 使用容量上限 + low watermark
    - 保护近期文件与队列中的活跃任务
  - 已完成本机 PG18 preload 手工验证：
    - `shared_preload_libraries = 'pg_flashback'` 后 launcher/worker 可见
    - 清空 `meta/summary` 后，无查询介入即可自动重新生成 summary
    - 人工制造超限 summary 文件后，launcher 会自动清理回阈值以下
  - 当前并发预算已收口：
    - summary worker 默认值改为 `2`
    - 注册时会按 `max_worker_processes` 自动预留查询 worker 槽位，避免挤占 WAL payload 动态 bgworker
  - 当前服务调度与可观测面：
    - launcher 默认按“最近时间 -> 更早时间”推进 summary 预建
    - shared queue 已显式区分 hot / cold 两类任务
    - 用户主视图已重做为 `pg_flashback_summary_progress`
    - 服务调试视图已拆分为 `pg_flashback_summary_service_debug`
  - 决策记录：
    - `docs/decisions/ADR-0019-summary-prebuild-service.md`

- 目标回归当前状态：
  - `fb_keyed_fast_path`
  - `fb_flashback_hot_update_fpw`
  - `fb_custom_scan`
  - 逻辑输出已对齐；当前 `installcheck` 仅剩 `expected/fb_keyed_fast_path.out` 文件末尾空白尾行格式差异，非结果内容差异
- flashback 主链并行改造当前处于：
  - Phase 2 已进入稳定增益阶段
  - Phase 3 prototype 已回收，等待更低开销实现
  - Phase 5 prototype 继续关闭
  - apply 串行热路径继续收敛中，下一步是在无干扰环境下补稳态复测，并继续压 residual / stage 9 尾段
  - Phase 2 当前正式主路径：
    - WAL payload / materialize 并行已经进入稳定主路径
    - 当前实现固定为：
  - 本轮新增性能收敛任务已入场：
    - keyed fast path 从 `=` / `IN` / `ORDER BY key LIMIT` 扩展到单列稳定键的 range 谓词
    - 去掉 `FbApplyScan` 末尾 `ExecCopySlot/tts_virtual_materialize` 输出拷贝链
    - 完成后回看 `3/9` 当前架构中仍可精简的重复性 work
    - 进程级 payload worker
  - 2026-04-08 准确率复核继续推进后，当前新增一个 correctness blocker：
    - `run_flashback_checks` 改成 correctness-only 口径
      (`FB_RELEASE_GATE_WARMUP_RUNS=0 FB_RELEASE_GATE_MEASURED_RUNS=1`)
      后，首条
      `random_flashback_1.documents @ 2026-04-07 04:41:28.555065+00`
      已完成 truth 对比：
      - flashback 导出 `sha256 =
        4edde6e0b1e1ee94e1f9e2de12856bb1a50a85e73431dcbceae16a8e18117e0c`
      - `row_count = 1949969`
      - 与 `truth_manifest` 完全一致
    - 但第二条
      `random_flashback_1.users @ 2026-04-07 04:41:28.555065+00`
      当前稳定失败：
      - 使用现有 `meta/summary` 时：
        - `ERROR: pfree called with invalid pointer ...`
      - 临时移空 `meta/summary` 后：
        - 不再报 invalid `pfree`
        - 改为 `ERROR: too many shared backtracking rounds while resolving missing FPI`
    - 当前结论：
      - release gate 整套 truth compare 还不能宣称跑完
      - `documents` 首条大表 truth 已过
      - `users` 的 summary payload/cached lookup 路径与无-summary backtracking/FPI
        都还存在独立 blocker
  - 2026-04-10 下午 release gate `18pg` 新增下一条 correctness blocker：
    - 现场：
      - `random_flashback_1.documents @ 2026-04-10 04:05:44.660105+00`
      - `3/9 55% xact-status` 约 `+37349.947 ms`
      - `3/9 100% payload` 约 `+69069.254 ms`
      - 随后在 `4/9 replay discover precomputed`
        直接报
        `ERROR: too many shared backtracking rounds while resolving missing FPI`
    - 当前结论：
      - 这条 case 同时暴露两层问题：
        - `xact-status` 仍存在 summary/exact-fill 未命中的 residual tail
        - residual tail 后续又把 replay discover 推进到 shared backtracking 上限
      - 本轮必须把
        `xact-status 未命中`
        与
        `shared backtracking rounds` 这两层一起对齐，
        不能只改表层报错或单独放大回溯轮次
  - 2026-04-10 下午已对齐上述 `documents` 双重 blocker 的主根因：
    - 根因已确认：
      - release gate 已注入 `pg_flashback.target_snapshot`
      - 但 `xact-status` 旧逻辑对“summary/WAL 都拿不到 outcome、
        但 clog 已能确认 committed/aborted”的 xid
        仍直接留成 unresolved
      - 这些 xid 在 target snapshot 口径下本可直接判定
        `before target` / `after target`
      - unresolved xid 会把大量本不该继续保留的 records 留到 replay 输入，
        最终把 `discover precomputed`
        推到 `shared backtracking rounds` 上限
    - 当前修复已落地：
      - `src/fb_wal.c`
        已新增 target snapshot + clog fallback：
        - 当 xid 在 WAL outcome 中缺失时，
          改为直接用 `TransactionIdDidCommit/Abort`
          + snapshot 可见性补齐状态
        - 同时把该补位接入
          `fb_wal_xid_committed_after_target()`
          / `fb_wal_set_record_status()`
          与 serial unresolved fallback 收口
      - 新增 debug contract
        `fb_target_snapshot_clog_status_debug`
      - `sql/fb_target_snapshot.sql`
        / `expected/fb_target_snapshot.out`
        已补最小回归锁住：
        - committed xid 且 snapshot 可见
          => `committed_before_target`
        - committed xid 且 snapshot 不可见
          => `committed_after_target`
    - 当前验证已完成：
      - `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg PGHOST=/tmp installcheck REGRESS='fb_target_snapshot'`
      - 18pg `alldb` 现场复跑同一条 release gate query：
        - `3/9 55% xact-status`
          已从约 `+37349.947 ms`
          降到约 `+1409.804 ms`
      - 原先在 `4/9 replay discover precomputed`
          立即报出的
          `ERROR: too many shared backtracking rounds while resolving missing FPI`
          在本轮观察窗口内未再复现
    - 2026-04-12 当天继续收口了 `4/9 replay discover` 两个“已改过但仍劣化”的热点：
      - discover / shared warm backtrack 已不再创建 `discover_toast_store`
      - discover pass 已改为 skeleton read + on-demand payload materialize
      - applied discover record 已复用同一 materializer reader，
        不再对同一条 WAL record 做第二次 `XLogReadRecord()`
    - 当前新增 contract 回归：
      - `fb_replay_discover_toast`
      - `fb_replay_discover_skip_payload`
      - `fb_replay_discover_materialize_reuse`
    - 当前验证已完成：
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config`
      - `make install PG_CONFIG=/home/14pg/local/bin/pg_config`
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS='fb_replay_discover_toast' PGUSER=14pg`
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS='fb_replay_discover_skip_payload' PGUSER=14pg`
      - `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS='fb_replay_discover_materialize_reuse' PGUSER=14pg`
    - 14pg `alldb` 同一条 live query 再抓 `perf` 后，
      discover 的重复 payload reread 已退出头部热点；
      当前前两位重新收口为：
      - `ValidXLogRecord/XLogReadRecord -> fb_wal_load_record_by_lsn`
      - `fb_replay_get_block/hash_search`
    - 同一条 live query 在
      `spill_mode='disk', memory_limit='4GB'`
      下仍会在 `4/9 replay discover`
      命中
      `pg_flashback memory limit exceeded while tracking BlockReplayStore`
      ，说明下一刀应继续打首读 WAL 冷读和 block hash/store，
      而不是再回头处理 discover reread

## 下一步

- 继续补 summary 服务的细节验证与策略收敛：
  - cleanup 与 queue/progress 的专项回归是否再细分
  - 补齐 cold backlog 吞吐优先调度、批量 claim 与 `estimated_completion_at` 的专项回归
- 回到主线性能目标：
  - 优先完成 `3/9` summary-first 第二阶段：
    - window-scoped xid outcome reduce
    - query-local summary section cache
    - 紧凑 unsafe facts section
    - metadata summary-first + uncovered-window WAL fallback
  - 并行补 `3/9` 观测面收口：
    - `NOTICE` 直接显示 build-index 当前卡在哪个子相位
    - 不新增第 `10` 段，也不做 `count(*)` 特化
    - 先用回归和 debug counter 把 payload 尾段工作量直接暴露出来
  - 基于新 payload counters 继续做真实 work narrowing：
    - 目标是压掉对 replay/final 无贡献的 payload decode/materialize
    - 不是只停留在 `NOTICE` 观测修正
  - 当前 live case 上，payload 尾段已经明显收下来了；后续主热点已转向：
    - `6/9 replay final`
    - `8/9 applying reverse ops`
    - 以及 `3/9 metadata/xact-status` 的剩余串行 work
  - 完成后清空并重建 `meta/summary`，复跑 live case 与回归
  - 再继续压 `3/9` 后半段 payload/materialize
  - 先把 `random_flashback_1.users @ 2026-04-07 04:41:28.555065+00`
    的 correctness blocker 拆开并修掉：
    - 有 summary 时为什么会触发 invalid `pfree`
    - 无 summary 时为什么 shared backtracking/FPI 仍过不去
  - 紧接着处理
    `random_flashback_1.documents @ 2026-04-10 04:05:44.660105+00`
    的新 blocker：
    - 先追清 `3/9 xact-status` residual unresolved xid
    - 再追清为什么 discover/precomputed 会把相同 case 推到
      `too many shared backtracking rounds while resolving missing FPI`
  - 修完后再继续 correctness-only 口径把整套 truth compare 跑完
  - 继续推进 metadata 并行第二版实现
    - leader 共享 resolved segment snapshot，去掉 worker 侧重复 resolver/prepare
    - worker 本地 spool，leader 按 window/LSN 顺序 merge
  - 当前 safety bundle 复核补充：
    - `fb_summary_overlap_toast`
    - `fb_replay`
    - 当前均通过
    - `fb_summary_v3` 仍有一条既有断言漂移：
      - `uses_summary_unsafe_facts` 当前返回 `false`
      - 旧 expected 仍写成 `true`
    - 该差异落在现有 summary/unsafe facts 主线，不由本轮 `3/9 NOTICE + debug counter` 改动引入

- 本轮已完成 keyed range fast path 扩展：
  - planner/apply 当前支持单列稳定 keyed 的：
    - `BETWEEN`
    - `< <= > >=`
    - 两侧 bound 的开闭区间组合
    - 与 `ORDER BY key` / `LIMIT` 的组合
  - 对跨类型常量比较（例如 `bigint key` + `int4 const`）已改为按目标 btree opfamily 做 operator interpretation，不再把 `>=` / `<=` 误判为 `=`
  - `FbApplyScan` 当前已直接绑定输出 slot，去掉末尾 `ExecCopySlot/tts_virtual_materialize` 拷贝链
  - 为避免 range merge 时 slot 生命周期问题，ordered current candidate 当前改为 materialize 成 owned heap tuple 后再交给输出 slot
  - 新增/更新回归：
    - `fb_keyed_fast_path`
    - 覆盖 `BETWEEN`、混合上下界、`ORDER BY DESC LIMIT`、带 projection 的 range fast path

- 对 `3/9` 当前重复性 work 的本轮复盘：
  - 仍值得继续压的主热点不在 prefilter 落盘，而在：
    - `XLogDecodeNextRecord`
    - CRC 校验
    - touched-xid / xid-status 的重复 hash bookkeeping
  - 当前最值得继续看的重复路径：
    - 已进入 decode 但最后不会留下 payload / reverse-op 的 record 仍然完整走了解码与记账
    - touched-xid 与事务状态路径上存在重复 hash lookup / insert
    - segment / window 级筛选信息在 prefilter、visit window、materialize window 之间还有重复折返
  - 结论：
    - `3/9` 下一步更偏架构型收敛，应优先减少“需要进入 decode 的 record 数”和“decode 后仍重复做的 xid bookkeeping”
    - raw spool file merge + anchor rebuild，减少 leader 二次解帧开销
    - 对连续大 payload window 再按 segment 切分，提高 worker 利用率
    - 切分窗口采用“reader overlap + logical emit boundary”模型，避免跨 segment record 丢失
  - 已新增自动化回归 `fb_wal_parallel_payload`
  - 合同固定为：
    - 多 payload window 下串行/并行核心摘要一致
    - `payload_windows` 与 `payload_parallel_workers` 可观测
    - `pg_flashback(...)` 串行/并行结果集 diff 为 `0`
  - 当前 live case 结果：
    - `scenario_oa_12t_50000r.documents`
    - `parallel_workers=8` 时 `payload_windows=10 payload_parallel_workers=5`
    - `FbWalIndexScan` 从约 `41.0s` 压到约 `19.8s`
    - 同环境 `parallel_workers=0` 对照约 `28.8s`
    - 总时长从约 `75.5s` 压到约 `66.0s`
- Phase 3 当前状态：
  - WAL metadata 两段式并行 prototype 仍保留在代码里，但当前保持关闭
  - 关闭原因固定为：
    - live case 上额外 metadata worker 启停 + 二次 `RM_XACT_ID` fill 开销未打赢串行基线
  - 已保留的工作成果：
    - `fb_recordref` safe/unsafe 串并行合同回归
    - touched/unsafe/xid_status 收敛路径验证
  - 当前正式主路径仍然使用 metadata 串行扫描
- Phase 5 当前状态：
  - 已做过 keyed query-side apply 并行 prototype
  - 方案是 shared reverse-source + parallel table scan + per-worker tuple spool + leader residual merge
  - 当前已补齐并固定：
    - SRF / CustomScan 两条 query 路径都会在 apply 并行候选下构建 shareable reverse-source
    - worker 不再强制 `force_string_keys=true`
    - leader 已支持 typed seen-key merge
    - apply worker 数已收口到 `parallel_workers / max_parallel_workers_per_gather / max_worker_processes` 的安全上限，并在 worker 申请失败时回退串行
    - 当前已新增大表保护阈值，超出阈值的 relation 暂不进入 apply 并行，避免 live case 被未成熟模型拖慢
  - 当前 correctness 路径已打通，并新增 `fb_apply_parallel` 回归覆盖：
    - `fb_apply_debug(...)` 可观测 `apply_parallel=on`
    - keyed + 单列 typed key 串并行结果集 diff 为 `0`
  - 但 live case 仍未打赢稳定基线
  - 当前正式主路径也仍保持关闭，不进入默认稳定路径
  - 当前已确定下一轮收敛方向：
    - 第一阶段只继续收敛 keyed + 单列 typed key 的 apply 并行
    - 串行 apply 热路径已先补：
      - direct single-typed probe
      - bloom 风格 negative filter
      - CustomScan raw slot return
    - 当前 live case 的主要瓶颈已确认是“per-worker tuple spool + leader 回读”的文件传输模型
    - 下一轮优先把 worker -> leader tuple 结果传输改成基于 `shm_mq` 的流式模型，seen-key 先继续保留小 spool
    - 再评估是否继续保留 tuple spool 兜底
  - 当前仍待补：
    - 更广 live/deep 对比，确认 Phase 2 收益稳定
    - 重新设计 Phase 3，避免 worker 启停和额外 WAL pass 抵消收益
    - 重新设计低开销 Phase 5，消除“全量物化当前历史结果再回读”这类会拖慢通用 SQL 的方案
  - 当前本机回归还存在一个与本轮无关的已观测 blocker：
    - 某些 `pg_flashback()` 查询会在 `fb_apply_next_output_slot()` 上因 `slot=NULL` 崩溃
    - coredump 栈落点：
      - `src/fb_apply.c`
      - `src/fb_custom_scan.c`
    - 该问题会干扰 `fb_runtime_gate` / `fb_user_surface` 这类会实际执行 `pg_flashback()` 的回归

- 已完成将 `FROM pg_flashback(...)` 从单个 `Custom Scan (FbFlashbackScan)` 拆成可观测的多节点算子树：
  - 当前目标计划树固定为：
    - `FbApplyScan`
    - `FbReverseSourceScan`
    - `FbReplayFinalScan`
    - `FbReplayWarmScan`
    - `FbReplayDiscoverScan`
    - `FbWalIndexScan`
  - 目标是让用户在 `EXPLAIN ANALYZE` 下直接看到 WAL / replay / reverse / apply 各段耗时
  - 不改变公开 SQL 入口，只重构 `FROM pg_flashback(...)` 的 planner / executor 内部结构
  - 当前 `EXPLAIN (VERBOSE, COSTS OFF)` 已可稳定展开整棵多节点树
  - 当前手工 `EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, SUMMARY OFF)` 已确认每个节点都有独立 `actual time`
  - `fb_custom_scan` / `fb_keyed_fast_path` 回归已同步切到新的 plan 基线
  - 设计稿：`docs/superpowers/specs/2026-03-29-multi-custom-scan-explain-design.md`
  - 实施计划：`docs/superpowers/plans/2026-03-29-multi-custom-scan-explain-plan.md`
  - 决策记录：`docs/decisions/ADR-0014-custom-operator-tree-for-explain-analyze.md`

- 已完成 keyed fast path：
  - 目标统一覆盖三类高收益查询：
    - `WHERE 主键 = const`
    - `WHERE 主键 IN (const, ...)`
    - `ORDER BY 主键/稳定唯一键 ASC|DESC LIMIT N`
  - 第一阶段范围固定为：
    - 仅 keyed relation
    - 仅单列稳定主键/唯一键
    - 不安全或无法证明正确的场景自动回退到现有全量 flashback 路径
  - planner / `CustomScan` / apply 当前共享一份内部 `FbFastPathSpec`
  - `WHERE 主键 = const` 与 `WHERE 主键 IN (...)` 已切到主键索引点查，不再扫当前整表
  - `ORDER BY 主键 ... LIMIT N` 已切到主键索引有序扫描 + residual merge + 早停
  - `EXPLAIN` 会在 `Custom Scan (FbFlashbackScan)` 下直接显示 `Fast Path: key_eq|key_in|key_topn`
  - 已修复 live case `ORDER BY id DESC LIMIT N` 在 `key_topn` 路径上的 backend SIGSEGV：
    - 根因是 `index_beginscan(..., 0, 0)` 后漏掉 `index_rescan(..., NULL, 0, NULL, 0)`
    - btree no-key backward scan 会带着未初始化扫描态进入 `index_getnext_slot()`，最终崩在 `_bt_start_array_keys()`
    - 现已补齐初始化；`scenario_oa_12t_50000r.documents ORDER BY id DESC LIMIT 50` live 复测可稳定返回
  - 新增回归 `fb_keyed_fast_path`
  - 设计稿：`docs/superpowers/specs/2026-03-29-keyed-fast-path-design.md`
  - 实施计划：`docs/superpowers/plans/2026-03-29-keyed-fast-path-plan.md`
  - 决策记录：`docs/decisions/ADR-0012-keyed-fast-path-for-point-set-topn.md`

- 已完成 `CustomScan` 输出内存模型修复：
  - `FROM pg_flashback(...)` 现有大内存 live case 已确认不是 `pgsql_tmp`，而是 `CustomScan` 输出链路经由 `Datum -> ExecStoreHeapTupleDatum()` 造成 `ExecutorState` 线性膨胀
  - 当前 `fb_apply` 新增 slot-native 输出路径，`fb_entry` 暴露 `fb_flashback_query_next_slot()`，`fb_custom_scan` 不再经过 `ExecStoreHeapTupleDatum()`
  - 新增 `fb_custom_scan` 游标回归，直接用 `pg_get_backend_memory_contexts()` 校验继续 `FETCH` 时 `ExecutorState` 不再线性增长
  - live 复测 `scenario_oa_12t_50000r.documents order by id desc limit 10` 时，backend RSS 约在 `2.1GB` 左右封顶，不再继续爬升到 `5GB+ / 10GB+`
  - 本轮仍未实现 `ORDER BY/LIMIT` 下推，剩余内存主要反映 flashback 工作集与上层必须等待完整输入的排序语义
  - 设计稿：`docs/superpowers/specs/2026-03-29-customscan-slot-native-output-design.md`
  - 实施计划：`docs/superpowers/plans/2026-03-29-customscan-slot-native-output-plan.md`

- 已完成 `FROM pg_flashback(...)` 的 temp-file 根修：
  - 新增 `fb_custom_scan` 模块，通过 planner hook + `CustomScan` 接管 `RTE_FUNCTION`
  - 新增 `fb_pg_flashback_support(internal)` 作为 `pg_flashback` 的 planner support function，确保新会话在规划阶段就加载模块并注册 hook
  - `CustomScan` executor 直接复用 `fb_flashback_query_begin/next/finish` 主链，不再经过 PostgreSQL 默认 `FunctionScan -> ExecMakeTableFunctionResult() -> tuplestore`
  - live 复测 `scenario_oa_12t_50000r.documents` 时，`stage 8` 期间 `base/pgsql_tmp` 持续保持目录本身 `6 bytes`，不再出现多 GB 临时文件膨胀
  - 新增回归 `fb_custom_scan`，覆盖：
    - `EXPLAIN` 计划节点为 `Custom Scan (FbFlashbackScan)`
    - 低 `work_mem` 下 `count(*) FROM pg_flashback(...)` 的 `pg_stat_database.temp_bytes` 增量为 `0`
- 已修复 `CustomScan` 落地过程中的两个配套问题：
  - `custom_scan_tlist` 现按目标表完整列布局构造，避免聚合场景列映射错位（如 `sum(amount)` 误读成 `sum(id)`）
  - `_PG_init()` 中 `MarkGUCPrefixReserved("pg_flashback")` 现后移到 `DefineCustom*` 之后，保留用户在模块加载前设置的 `pg_flashback.memory_limit` / `spill_mode` 占位值

- 已移除 `pg_flashback()` 内部 materialized SRF 分支：
  - `pg_flashback()` 不再因执行器允许 `SFRM_Materialize` 而优先走 `tuplestore`
  - 内部结果发射统一收口为 `ValuePerCall`
  - 新增回归 `fb_value_per_call`，覆盖入口模式与低 `work_mem` 下不再由函数自身制造 temp spill
- `pg_flashback.memory_limit` 已完成收敛：
  - 用户侧参数名从 `pg_flashback.memory_limit_kb` 改为 `pg_flashback.memory_limit`
  - 最大允许值从 `4TB` 收紧到 `32GB`
  - 已补回归覆盖 `8GB`、`32GB` 与超限 `33GB`
- `pg_flashback.spill_mode` 与 preflight 选择已落地：
  - 新增 `auto|memory|disk` 三态策略 GUC
  - 保持 `pg_flashback.memory_limit` 作为唯一内存预算参数
  - 在 `WAL scan` 后、`replay` 前增加 working-set estimate
  - `auto/memory` 在 estimate 超预算时提前报错，`disk` 允许继续
- `pg_flashback.memory_limit='8GB'` 被误拒绝的问题已修复：
  - `memory_limit` 的单位解析与内部存储已统一改为 `uint64 kB`
  - 实际允许范围与文案一致为 `1kB .. 32GB`
- `pg_flashback.memory_limit` 已支持单位输入：
  - 参数支持用户直接写 `kb/mb/gb`
  - 内部改为大小写不敏感解析并规范化回显
  - 保留旧的裸数字 `kB` 语义兼容
- 内存超限报错已增强可读性：
  - 统一内存超限报错入口追加 `pg_flashback.memory_limit` 调参提示
  - `tracked/limit/requested` 保留原始 bytes
  - 对能被 `1024` 整除的值追加 `KB/MB/GB/...` 可读单位
- `show_progress` 已增强为耗时可见输出：
  - 开启时每条 `NOTICE` 追加“相对上一条输出的增量耗时”
  - 查询结束时额外输出总耗时
  - 关闭时继续保持完全不输出进度与耗时
  - 新增回归专用确定性时钟注入钩子，用于稳定覆盖耗时格式与最终总耗时 NOTICE
- 细化 `docs/architecture/` 全部架构文档，并按当前代码现状重写三份中文手册：
  - `核心入口源码导读.md`
  - `源码级维护手册.md`
  - `调试与验证手册.md`
  - 同步补齐 `error-model` / `reverse-op-stream` / `row-identity` / `wal-decode` / `wal-source-resolution` / `overview` 的代码级说明
- WAL 来源解析新增 PostgreSQL 内核归档配置自动发现：
  - `pg_flashback.archive_dest` 仍保持最高优先级显式覆盖
  - `pg_flashback.archive_dir` 继续作为兼容回退
  - 两者都未设置时，可从 PostgreSQL `archive_command` 自动推断本地归档目录
  - 本地 `pg_probackup archive-push -B ... --instance ...` 可自动映射到 `backup_dir/wal/instance_name`
  - `archive_library` 非空或复杂/远程 `archive_command` 不自动猜测，继续要求显式设置 `pg_flashback.archive_dest`
- 修复已加载扩展后的 `pg_flashback.*` GUC typo 被静默接受：
  - `_PG_init()` 现在保留 `pg_flashback` 前缀
  - 扩展已加载后，未定义的 `pg_flashback.show_process` 会直接报错
  - 新增/更新回归 `fb_guc_defaults` 覆盖该行为
- 修复 `PG18` `same-block HOT_UPDATE + FPW-only` 被误报 `heap update record missing new tuple data`：
  - `fb_replay_heap_update()` 现在尊重 `apply_image`
  - 对“new page 镜像已经是最终页态”的 update/hot update 不再错误要求 tuple payload
- 修复页级重放错误跳过 aborted heap record 导致的页槽位漂移：
  - `fb_replay` 主循环不再跳过 `record.aborted`
  - 避免回滚事务留下的物理 tuple/slot 丢失，进而触发后续 `invalid new offnum during heap update redo`
- 新增回归 `fb_flashback_hot_update_fpw`，覆盖：
  - `VACUUM FREEZE + CHECKPOINT + same-block HOT_UPDATE + FPW-only`
  - 修复前稳定报 `heap update record missing new tuple data`
  - 修复后稳定返回历史旧行像
- 新增仓库维护脚本 `scripts/cron_daily_update.sh`：
  - 仅在仓库存在变更时执行 `git add -A`
  - 固定提交信息 `update`
  - 推送目标固定为 `origin/main`
- 已为当前用户安装本机 `cron`：
  - 每天 `01:00` 在 `/root/pg_flashback` 执行自动提交/推送
  - 日志输出到仓库根目录 `cron_daily_update.log`
- 将 `README.md` 重写为面向客户的使用文档
- 抽取 `fb_compat` 跨版本兼容层
- 去掉 `Makefile` 中默认绑定的 `PG18` `PG_CONFIG`
- 收敛 `PG14-18` 本机编译矩阵
- 同步 `STATUS.md` / `TODO.md` / `PROJECT.md` / `docs/architecture/overview.md` 到当前版本支持口径
- 将公开入口彻底迁移到 `pg_flashback(anyelement, text)`
- 删除结果表物化与并行结果表写入代码
- 删除 `include/fb_parallel.h` / `src/fb_parallel.c`
- 删除 `pg_flashback.parallel_apply_workers` GUC
- 将 apply 主链改为流式执行：
  - 新增 `src/fb_apply.c`
  - 重写 `src/fb_apply_keyed.c`
  - 重写 `src/fb_apply_bag.c`
- 将 `pg_flashback()` 改为 value-per-call SRF
- 修复 SRF cleanup 生命周期问题
- 修复“普通 heap tuple 直接作为复合 Datum 返回”导致的崩溃问题
- 回归 SQL 全量迁移到 `NULL::schema.table` 调用形态
- deep SQL 主线迁移到直接查询口径，不再依赖结果表
- 重写当前维护文档，移除旧的结果表/并行写表描述
- 已从本机会话日志中恢复出一版可编译的 bounded spill Stage A 代码基线：
  - 新增 `fb_spool`
  - 新增 `sql/fb_spill.sql`
  - `fb_wal` / `fb_replay` / `fb_reverse_ops` / `fb_apply` 已切到 `FbReverseOpSource`
  - 该基线来自 `2026-03-26 18:22`、`2026-03-26 20:03`、`2026-03-26 21:10` 三段会话的前半段稳定 patch
- 已继续从本机会话日志恢复出 `2026-03-27` 后续 spill follow-up：
  - `fb_apply` 新增 emit / materialize 双路径
  - `fb_entry` 新增内部 materialized SRF 分支
  - `fb_wal` 新增 recent tail inline / sidecar 相关恢复代码与调试 SQL
  - `sql/fb_recordref.sql` / `sql/fb_toast_flashback.sql` / `sql/fb_wal_sidecar.sql` 已同步回恢复结果
  - 当前稳定继续恢复树为：`/tmp/pgfb_exact_probe`
- 已补回本轮会话内新增的设计 / ADR / 报告文档：
  - `docs/specs/2026-03-26-bounded-spill-design.md`
  - `docs/decisions/ADR-0006-bounded-spill-main-pipeline.md`
  - `docs/decisions/ADR-0007-toast-heavy-materialized-srf.md`
  - 若干 `docs/superpowers/specs` / `plans` 与冷缓存报告
- 已复盘用户 live 案例 `scenario_oa_12t_50000r.notices`：
  - 主表 `relfilenode = 16396047`
  - TOAST `relfilenode = 16396114`
  - 建表事务 `xid = 204237`，提交时间 `2026-03-27 22:40:37.885578+08`
  - 阻塞 flashback 的真实原因不是建表，而是 TOAST relation 在事务 `xid = 294470` 中发生 `SMGR TRUNCATE`
  - 该 TOAST truncate 提交时间为 `2026-03-27 22:51:42.387556+08`
- 已将 `TOAST SMGR TRUNCATE` 从 relation 级直接拒绝改为“允许 replay，最终缺 chunk 时再报错”
- 已修复 standalone `standby_lock` 被误判为 `storage_change` blocker：
  - 该记录只表示 Hot Standby 的 `AccessExclusiveLock`
  - 若没有后续更具体的 `truncate/create/rewrite`，当前不再单独阻塞 flashback
- 已新增回归 `fb_flashback_standby_lock`，覆盖“standalone standby lock 不应阻塞 flashback”
- 已将回归 `fb_flashback_toast_storage_boundary` 改写为 truth 对比口径，覆盖“TOAST truncate 后仍可正确 flashback”的用户案例边界
- 已增强 `storage_change` 诊断信息，当前会直接暴露 relation scope / storage op / xid / commit time
- 已确认用户现场案例 `scenario_oa_12t_50000r.notices` 的默认内存瓶颈不只在 replay：
  - `memory_limit=65536/131072` 时卡在 `BlockReplayStore`
  - `memory_limit=262144/524288` 时 replay 能通过，但会卡在 `forward row tuple`
  - 当前稳定通过阈值落在 `786432KB` 以上
- 已决定将 `pg_flashback.memory_limit` 默认值提升到 `1048576KB`（1GB）：
  - 原因是 bounded spill 尚未接入 WAL 索引 / replay 主链
  - 在当前实现阶段，用 64MB 作为默认值会让已验证可恢复的 live case 直接失败
- 已定位新的 live blocker：`scenario_oa_12t_50000r.notices` 在 `target_ts = '2026-03-28 11:06:13'/'2026-03-28 11:07:13'`
  会在 discover round=1 命中 `lsn=69/8E372BF0` 的 `heap insert off=4 blk=892`
- 当前根因判断为：
  - 该 insert 之前存在一条跨页 `UPDATE(old blk 2761 -> new blk 892, new_off=3)`
  - discover 第一轮因旧页缺锚点跳过该 update
  - 但新页 `blk 892` 没有被标记为“依赖缺页、当前轮状态不可信”
  - 导致后续 `INSERT off=4` 仍在落后页态上执行，最终报 `specified item offset is too large`
- 已继续定位并修复同一 live case 的第二层 blocker：
  - `blk 12119` 曾报 `missing FPI`
  - 实际并非无锚点，而是 `69/8D0D5538` 的 `PRUNE_VACUUM_SCAN FPW` 落在 `redo_lsn..checkpoint_lsn` 区间
  - sidecar 恢复 anchor 后，起扫点被错误裁到 `checkpoint_lsn`，导致这段必要 WAL 被漏扫
- 已完成两处修复：
  - replay discover 新增“缺页依赖传播”：跨页记录任一 block 缺锚点时，将同记录所有 block 标记为当前轮不可信；后续同块记录延后到 warm/backtrack 后再重放
  - checkpoint sidecar 恢复 anchor 时，`start_lsn` 与 `anchor_hint_segment_index` 统一对齐 `redo_lsn`，不再对齐 checkpoint record 自身
- 已补齐仓库当前 `REGRESS` 缺失的 13 份 `expected/*.out` 基线，恢复全量 installcheck 可执行状态

## 当前验证结果

- `make PG_CONFIG=/home/18pg/local/bin/pg_config clean`：通过
- `make PG_CONFIG=/home/18pg/local/bin/pg_config install`：通过
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_to pg_flashback fb_user_surface fb_flashback_keyed'`：`All 4 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_keyed_fast_path fb_custom_scan fb_flashback_keyed pg_flashback'`：`All 4 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_custom_scan fb_value_per_call fb_guc_defaults pg_flashback fb_user_surface fb_progress fb_preflight fb_memory_limit fb_spill fb_toast_flashback'`：`All 10 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_custom_scan fb_keyed_fast_path fb_value_per_call pg_flashback'`：`All 4 tests passed.`
- `psql -h /tmp -p 5832 -U 18pg postgres` 手工 `EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, SUMMARY OFF)`：
  - 已确认 `FbApplyScan -> FbReverseSourceScan -> FbReplayFinalScan -> FbReplayWarmScan -> FbReplayDiscoverScan -> FbWalIndexScan`
  - 已确认每个节点都显示独立 `actual time`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_value_per_call pg_flashback fb_user_surface fb_progress'`：`All 4 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_keyed fb_flashback_bag fb_toast_flashback fb_memory_limit fb_spill fb_preflight'`：`All 6 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_memory_limit fb_spill'`：`All 3 tests passed.`
- `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_preflight fb_memory_limit fb_spill'`：`All 4 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_memory_limit'`：`All 2 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_memory_limit'`：`All 1 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_progress'`：`All 1 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_progress fb_user_surface'`：`All 2 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_wal_source_policy fb_guc_defaults fb_progress pg_flashback'`：`All 4 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_hot_update_fpw'`：`All 1 tests passed.`
- 用户现场案例 `scenario_oa_12t_50000r.notices`：
  - 默认 `memory_limit=65536`：`BlockReplayStore` 超限
  - `memory_limit=131072`：`BlockReplayStore` 超限
  - `memory_limit=262144`：`forward row tuple` 超限
  - `memory_limit=524288`：`forward row tuple` 超限
  - `memory_limit=786432`：`count(*) = 39902`
  - `SET pg_flashback.memory_limit = '1048576'` 后：
    - `target_ts = '2026-03-28 07:42:13'`，`count(*) = 39902`
    - `target_ts = '2026-03-28 09:42:13'`，`count(*) = 39902`
  - 本轮继续验证：
    - `target_ts = '2026-03-28 11:06:13'`，`count(*) = 10091`
    - `target_ts = '2026-03-28 11:07:13'`，`count(*) = 5242`
- 新增 root cause 调试结论（`scenario_oa_12t_50000r.documents`）：
  - 在扩展未于规划前加载时，`EXPLAIN (VERBOSE)` 会退回 `Aggregate -> Function Scan on public.pg_flashback`
  - 当前已通过 `pg_flashback` 的 planner support function 解决该问题；新会话复测已稳定变为 `Aggregate -> Custom Scan (FbFlashbackScan)`
  - `stage 8/9` 期间 backend 会生成 `base/pgsql_tmp/pgsql_tmp<backend>.*`
  - `log_temp_files=0` 与 `lsof` 现场都证明 temp 文件会涨到多 GB
  - 查询结束或 backend 退出后，`base/pgsql_tmp` 立即回到 `0`
  - 根因已锁定为 PostgreSQL `FunctionScan` 对 table SRF 的默认 tuplestore materialize
- `scripts/cron_daily_update.sh` 幂等校验：通过
- 当前用户 `crontab` 安装校验：通过
- `PG14-18` 本机编译矩阵：通过
- `make PG_CONFIG=/home/18pg/local/bin/pg_config install`：通过
- `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`：`All 12 tests passed.`
- `2026-03-27` 当前恢复工作区 `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`：通过
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_toast_storage_boundary'`：`All 1 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_toast_storage_boundary'`（新 truth 口径）：`All 1 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_standby_lock fb_flashback_toast_storage_boundary'`：`All 2 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_flashback_hot_update_fpw fb_flashback_main_truncate fb_flashback_standby_lock fb_flashback_toast_storage_boundary'`：`All 5 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`：`All 1 tests passed.`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck`：`All 20 tests passed.`
- 用户现场案例 `scenario_oa_12t_50000r.notices`：
  - `target_ts = '2026-03-28 09:42:13'` 不再被 `standby_lock` 拦截
  - 当前新的 first blocker 是主表 `SMGR TRUNCATE`
  - 命中事务 `xid = 501101`，提交时间 `2026-03-28 11:29:08.778971+08`
- `/tmp/pgfb_exact_probe` 顺序回放成功后 `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`：通过
- 当前回归覆盖：
  - `fb_smoke`
  - `fb_relation_gate`
  - `fb_relation_unsupported`
  - `fb_runtime_gate`
  - `fb_value_per_call`
  - `fb_flashback_keyed`
  - `fb_flashback_bag`
  - `fb_flashback_storage_boundary`
  - `fb_flashback_toast_storage_boundary`
  - `pg_flashback`
  - `fb_user_surface`
  - `fb_memory_limit`
  - `fb_toast_flashback`
  - `fb_progress`

## 仍在进行

- [x] `pg_flashback.memory_limit` 用户侧 rename 与 `32GB` 上限收敛
- [x] `pg_flashback.spill_mode` 与 preflight 内存/磁盘选择
- [x] 全量 `installcheck` 已按当前工作区重跑通过
- [x] PG14 基础加载兼容已打通
  - 已补 `XLogFindNextRecord` / `pg_mkdir_p` / GUC free / `index_beginscan` / `shm_mq_send` 等兼容层
  - `make PG_CONFIG=/home/14pg/local/bin/pg_config clean && make ... install` 当前通过
- [x] PG14 summary service 已改回真实启动链
  - 不再对 `PG_VERSION_NUM < 180000` 直接 stub `fb_summary_service.c`
  - PG14 现改为 preload 阶段直接 `RequestAddinShmemSpace()` + `RequestNamedLWLockTranche()`，继续复用现有 `shmem_startup_hook + bgworker` 主体
  - `/home/14pg/data/postgresql.conf` 已设置 `shared_preload_libraries = 'pg_flashback'`
  - 重启后已看到 `pg_flashback summary launcher` / `pg_flashback summary worker` 进程
- [x] PG14 summary 开启后的 `xman` 用例最终闭环验证
  - 已补 `io_workers` 缺失 GUC 默认值回退，避免 PG14 执行期直接报 `unrecognized configuration parameter "io_workers"`
  - 已补 `invalid xid + smgr_create` 的 pre-target relation birth 兼容，避免 target 前建表在 anchor / summary 回灌阶段被误判为 `storage_change`
  - 本机 PG14 已启用 `track_commit_timestamp = on`，用于判定 relation catalog 创建事务是否早于 `target_ts`
  - `pgfb_pg14_xman_verify7` 已确认：
    - `track_commit_timestamp = on`
    - `shared_preload_libraries = 'pg_flashback'`
    - `pg_flashback_summary_progress.service_enabled = true`
    - `last_query_summary_ready = true`
    - `last_query_summary_span_fallback_segments = 0`
    - `last_query_metadata_fallback_segments = 0`
    - `ts1_ok .. ts6_ok` 全部为 `true`
- batch B / residual `missing FPI` 收敛
- `scenario_oa_50t_50000r.documents @ '2026-04-11 07:40:40.223683+00'`
  的 `count(*)` 现场继续收敛
  - 已修掉 prune/lookahead precompute 前移后把 `3/9` 压爆的回退
  - 当前最新 live 结果：
    - `3/9 payload = 29.225s`
    - `6/9 final = 32.183s`
    - `8/9 apply = 79.135s`
    - total `164.847s`
  - 现场 `fb_apply_debug(...)` 已确认：
    - `mode=keyed`
    - `apply_parallel=on`
    - `parallel_logs=2`
    - `fast_path=off`
  - 当前 blocker 已转成 SRF 主链里的 `8/9` apply，不再是 `3/9` precompute 前移
- `fb_export_undo`
- WAL 索引 / replay 主链继续向 bounded spill 演进
- 更多 PG18 heap WAL 与主键变化正确性补齐
- deep 级别验证尚未按本轮接口收口后重新补跑
- 当前可继续恢复 / 对照的稳定临时树为：
  - `/tmp/pgfb_stageA_clean`
  - `/tmp/pgfb_exact_probe`

## 下一步

- `pg_flashback-summaryd` 已落地到顶层构建、安装与开源镜像
  - `make` / `make install` 会同时生成并安装 daemon
  - daemon 已支持 `--config` / `--pgdata` / `--archive-dest` /
    `--once` / `--foreground`
  - 已增加单实例 `lock`、`state.json` / `debug.json` 发布、
    runtime hint 写入与 SQL 视图适配
  - 已确认：
    - `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
    - `make ... installcheck REGRESS='fb_summary_daemon_state fb_summary_service fb_summary_prefilter fb_summary_v3'`
    - `su - 18pg -c '/home/18pg/local/bin/pg_flashback-summaryd ... --once --foreground'`
      均通过
- 继续收敛 `count(*) FROM pg_flashback(...)` 当前 SRF 主链上的 `8/9 apply`
  - 默认会话 `EXPLAIN (COSTS OFF) SELECT count(*) FROM pg_flashback(...)`
    仍是 `Aggregate -> Function Scan on pg_flashback`
  - 当前现场 SQL 没有吃到 `CustomScan/FbCountAggScan`
  - 下一轮需要二选一：
    - 让 planner/custom-scan 路径在默认会话口径下真正接管
    - 或者给 SRF 主链补一条正确的 count-only 执行链
- 继续逐步从 preload/shared memory summary 服务迁移到 daemon 状态快照
- 在 PG14 上补一条最小 summary service 回归，锁住“preload 生效后 launcher/worker 真注册”的行为
- 评估是否要把 relation birth 判定从当前 `track_commit_timestamp` 路径继续下沉为不依赖实例配置的实现
- 继续评估是否要把 `validate` / `prepare wal` 进一步拆成更细的 explain 节点
- 观察多节点 `CustomScan` 树在更大时间窗和 deep case 下的 `EXPLAIN ANALYZE` 可读性与稳定性
- 观察首次 `2026-03-28 01:00 CST` 自动提交结果，确认日志与推送行为符合预期
- 继续补更多 `archive_command` 本地模式的自动识别与诊断输出
- 继续处理 deep pilot batch B blocker
- 评估大 live case 在默认 `memory_limit=65536` 下的 bounded spill / 内存收敛路径
- 为 `fb_export_undo` 开始独立实现
- 继续压缩 WAL 索引 / replay 路径的高水位内存
- 补齐主键变化与更多 TOAST / 宽表场景验证
- 按当前恢复后的工作区补跑 install / installcheck / deep 验证，确认恢复结果不仅“可编译”
