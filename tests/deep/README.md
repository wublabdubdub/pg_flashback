# 深度生产化测试

本目录用于 `pg_flashback` 的深度生产化测试，不属于 `make installcheck` 的常规回归集合。

特征：

- 使用独立数据库
- 可能持续数十分钟到数小时
- 会产生大量 WAL
- 会生成较大的输出与日志
- 以真实场景正确性和边界能力验证为主，而不是小规模单元回归

当前入口：

- 环境准备：`tests/deep/bin/bootstrap_env.sh`
- 基线装载：`tests/deep/bin/load_baseline.sh`
- 真值捕获：`tests/deep/bin/capture_truth.sh`
- 分批运行：`tests/deep/bin/run_batch_*.sh`
- 总入口：`tests/deep/bin/run_all_deep_tests.sh`

常用模式：

- dry-run：
  - `bash tests/deep/bin/bootstrap_env.sh --dry-run`
- pilot：
  - `bash tests/deep/bin/run_all_deep_tests.sh --pilot`
- full：
  - `bash tests/deep/bin/run_all_deep_tests.sh --full`

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

注意：

- 默认连接方式基于本机 PG18：
  - `su - 18pg`
  - `psql postgres`
- 当前 deep test 仍聚焦已支持能力：
  - 普通 heap
  - `INSERT / DELETE / UPDATE / HOT UPDATE`
  - `keyed`
  - `bag`
  - `archive_dest + pg_wal + ckwal` 来源解析
- 当前不把以下内容作为 deep test 通过前提：
  - TOAST
  - `multi_insert`
  - `fb_export_undo`
  - 更早的可恢复锚点搜索
