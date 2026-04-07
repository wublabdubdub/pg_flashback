# 深度生产化测试

本目录用于 `pg_flashback` 的深度生产化测试，不属于 `make installcheck` 的常规回归集合。

特征：

- 使用独立数据库
- `full` 现在不再追求超长时间单轮跑完整套超大 workload
- 会产生大量 WAL
- 会生成较大的输出与日志
- 以真实场景正确性和边界能力验证为主，而不是小规模单元回归

当前入口：

- 环境准备：`tests/deep/bin/bootstrap_env.sh`
- 基线装载：`tests/deep/bin/load_baseline.sh`
- 真值捕获：`tests/deep/bin/capture_truth.sh`
- 分批运行：`tests/deep/bin/run_batch_*.sh`
- TOAST 深测脚本：`tests/deep/sql/80_toast_scale.sql`
- TOAST 深测入口：`tests/deep/bin/run_toast_scale.sh`
- 总入口：`tests/deep/bin/run_all_deep_tests.sh`

常用模式：

- dry-run：
  - `bash tests/deep/bin/bootstrap_env.sh --dry-run`
- pilot：
  - `bash tests/deep/bin/run_all_deep_tests.sh --pilot`
- full：
  - `bash tests/deep/bin/run_all_deep_tests.sh --full`

当前 `full` 语义：

- 旧的“超长时间 / 超大 WAL / 尽量一次跑完整套 workload”的 `full` 语义已废弃
- 新的 `full` 固定为：
  - 单轮深测
  - WAL 预算约 `10GB`
  - 保持主表 `keyed/bag` 双表模型与 TOAST 表模型不变
  - baseline 只导入一次；完成后保存一份 PGDATA baseline 快照
  - 各 batch 在同一 baseline 快照上恢复执行，不再重复导入 baseline
  - batch 失败时优先回滚到 baseline 快照并重跑当前 batch
  - 终端断连后可基于状态文件从未完成 batch 续跑
  - 运行期间实时监测 `/isoTest`、`/`、`/tmp` 使用率
  - deep round cleanup 现在只自动清理 deep 临时产物与 fake `pg_wal`
  - `full` 模式不再自动清理 live 归档目录 `/isoTest/18waldata`
  - 原因是 batch 验证与缺页补锚可能需要回看更早 checkpoint / FPI WAL；自动清理会把仍需的段提前删掉
  - 任一文件系统使用率超过 `85%` 时，当前轮次会先中断；若 live archive 占满空间，需要人工在合适边界清理后再续跑

快照与空间约束：

- baseline 快照当前保存为单份，不做多代
- 创建快照前会检查磁盘水位和可用空间
- 若可用空间不足以容纳一份 PGDATA 快照，则当前 full 轮次直接失败，不进入 workload
- full 结束后会清理 deep 临时产物；live archive 保留给排障与续跑使用
- baseline 快照仅在当前 full 会话内保留

当前 pilot 结果：

- `batch_a`：通过
- `batch_c`：通过
- `batch_d`：通过
- `batch_e`：通过
- `batch_b`：失败

`batch_b` 当前失败原因已经收敛：

- 当前内核仍固定依赖 `target_ts` 前最近 checkpoint
- 已确认存在真实块在 checkpoint 之后首条相关记录为 `HEAP2 PRUNE_VACUUM_CLEANUP` / `VISIBLE`
- 随后才进入 `UPDATE/HOT_UPDATE`
- 这类块当前仍可能报 `missing FPI`

详细报告：

- `docs/reports/2026-03-23-deep-pilot-report.md`
- `docs/reports/2026-03-23-toast-scale-report.md`
- `docs/reports/2026-03-23-toast-full-report.md`

注意：

- 默认连接方式基于本机 PG18：
  - `su - 18pg`
  - `psql postgres`
- 当前 deep test 仍聚焦已支持能力：
  - 普通 heap
  - `INSERT / DELETE / UPDATE / HOT UPDATE`
  - 基础 TOAST 历史值重建
  - `keyed`
  - `bag`
  - `archive_dest + pg_wal + ckwal` 来源解析
- 当前不把以下内容作为 deep test 通过前提：
  - `multi_insert`
  - `fb_export_undo`
  - 全局更早锚点搜索

当前 TOAST deep 状态：

- pilot：通过
- 历史 full：旧语义下曾连续两次 fresh 复跑通过，`truth_count = result_count = 25000` 且 `diff_count = 0`
- residual risk：flashback 查询 backend RSS 仍可到约 `7.4GB`，后续还要继续做内存模型收敛
