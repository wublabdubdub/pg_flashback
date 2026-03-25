# 2026-03-23 Deep Pilot Report

## 目标

- 在独立库中执行生产化 pilot 深测
- 覆盖：
  - 多字段大表
  - 多 DML
  - `keyed`
  - `bag`
  - `archive_dest + pg_wal` 来源解析
  - 长时间窗压力

## Pilot 环境

- 数据库：`fb_deep_test`
- 模式：`pilot`
- 字段数：`64`
- 行数：`50,000`
- `bag` distinct bucket：`2,500`
- `pg_flashback.memory_limit_kb`：`1,048,576`
- archive fixture：最近 `48` 个 WAL segment
- 登录方式：
  - `su - 18pg`
  - `PGPORT=5832 psql fb_deep_test`

## 执行命令

注意：本报告记录的是 pilot 当次执行时的脚本与环境。后续 `tests/deep/` 仍需同步到当前“内嵌 `fb_ckwal`、不再暴露用户侧 `ckwal` GUC”的模型；因此下面命令只代表当次 pilot 的执行记录，不代表当前最新脚本约束。

批次 A：

```bash
bash tests/deep/bin/bootstrap_env.sh --pilot
bash tests/deep/bin/load_baseline.sh --pilot
bash tests/deep/bin/run_batch_a.sh --pilot
```

批次 B：

```bash
bash tests/deep/bin/run_batch_b.sh --pilot
```

批次 C：

```bash
bash tests/deep/bin/bootstrap_env.sh --pilot
bash tests/deep/bin/load_baseline.sh --pilot
bash tests/deep/bin/run_batch_c.sh --pilot
```

批次 D：

```bash
bash tests/deep/bin/bootstrap_env.sh --pilot
bash tests/deep/bin/load_baseline.sh --pilot
bash tests/deep/bin/run_batch_d.sh --pilot
```

批次 E：

```bash
bash tests/deep/bin/bootstrap_env.sh --pilot
bash tests/deep/bin/load_baseline.sh --pilot
bash tests/deep/bin/run_batch_e.sh --pilot
```

## 结果

- 批次 A `keyed`：通过
  - `flashback_rows=50000`
  - `truth_rows=50000`
  - `extra_count=0`
  - `missing_count=0`
- 批次 B `事务边界与回滚`：失败
- 批次 C `bag`：通过
  - `flashback_rows=50000`
  - `truth_rows=50000`
  - `extra_count=0`
  - `missing_count=0`
- 批次 D `WAL 来源跨目录`：通过
  - `fb_wal_source_debug()`：
    - `mode=archive_dest total=13 pg_wal=0 archive=13 ckwal=0 invoked=false`
  - `extra_count=0`
  - `missing_count=0`
- 批次 E `长时间窗/多轮 DML`：通过
  - `flashback_rows=50000`
  - `truth_rows=50000`
  - `extra_count=0`
  - `missing_count=0`

## Pilot 中修复的实现问题

- deep SQL 中 `psql` 变量未正确进入 `DO $$ ... $$`
- batch E 插入列不完整
- deep fixture 目录权限导致 `18pg` 无法读取
- deep batch D 的 archive/fake pg_wal fixture 构建顺序错误
- replay 热路径补齐：
  - `HEAP_LOCK` 最小 no-op replay
  - `HEAP2_PRUNE_*` 最小 no-op replay
  - 跨页 `UPDATE` 在新页已有 FPI 时不再机械二次插入

## 当前 blocker

批次 B 的失败已经收敛为当前锚点模型不足，而不是测试脚本问题。

现象：

- `pg_flashback()` 在 pilot 批次 B 中报 `missing FPI`

已确认的真实 WAL 形态：

- 同一 block 在 `target_ts` 前最近 checkpoint 之后，首条相关记录可能是：
  - `HEAP2 PRUNE_VACUUM_CLEANUP`
  - `HEAP2 VISIBLE`
- 这些记录不保证带 `FPI/INIT`
- 随后才出现 `UPDATE/HOT_UPDATE`

已抓到的实际样例：

- `blk 873`
- 顺序：
  - `PRUNE_VACUUM_CLEANUP`
  - `VISIBLE`
  - 多条 `UPDATE/HOT_UPDATE`

结论：

- 当前“`target_ts` 前最近 checkpoint 作为唯一锚点”的策略并不充分
- 即使 `full_page_writes=on`，也不能保证所有后续需要的 block 都能在 anchor 后立即拿到 `FPI/INIT`

## 下一步建议

- 不直接进入 full 模式
- 先解决批次 B blocker，再继续 full 深测
- 方向优先级：
  1. 先补 `missing FPI` 的细粒度诊断
  2. 再确认是否必须进入“更早的可恢复锚点搜索”
  3. 同步 `tests/deep/` 到当前内嵌恢复模型
  4. 再重新跑 pilot 全批次
