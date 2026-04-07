# Release Gate

`tests/release_gate/` 用于 `pg_flashback` 的发布前阻断式功能/性能验证，不属于 `make installcheck` 常规回归。

当前固定范围：

- PostgreSQL：`PG14-18`
- 测试库：`alldb`
- 归档根目录：`/walstorage`
- 按版本归档子目录：
  - `PG14 -> /walstorage/14waldata`
  - `PG15 -> /walstorage/15waldata`
  - `PG16 -> /walstorage/16waldata`
  - `PG17 -> /walstorage/17waldata`
  - `PG18 -> /walstorage/18waldata`

## 目录与入口

- 总入口：`tests/release_gate/bin/run_release_gate.sh`
- 空实例准备：`tests/release_gate/bin/prepare_empty_instance.sh`
- `alldbsimulator` 启停：`tests/release_gate/bin/start_alldbsim.sh`
- 基础建数：`tests/release_gate/bin/load_alldb_seed.sh`
- `1h` DML 压测：`tests/release_gate/bin/run_alldb_dml_pressure.sh`
- 大表扩容：`tests/release_gate/bin/grow_flashback_target.sh`
- truth snapshot：`tests/release_gate/bin/capture_truth_snapshots.sh`
- flashback 检查：`tests/release_gate/bin/run_flashback_matrix.sh`
- gate 判定：`tests/release_gate/bin/evaluate_gate.sh`
- 报告生成：`tests/release_gate/bin/render_report.sh`
- 自检：`tests/release_gate/bin/selftest.sh`

## 前置条件

必须满足：

- 可用 PostgreSQL 实例已启动
- 当前实例 `archive_mode` 已开启
- 当前实例 `archive_command` 实际落点就是本版本对应的 `/walstorage/<major>waldata`
- `/root/alldbsimulator/bin/alldbsim` 可执行
- `jq`、`curl`、`sha256sum`、`awk` 可用
- `FB_RELEASE_GATE_PSQL`、`createdb`、`dropdb` 可连到目标实例

当前默认连接口径来自 [release_gate.conf](/root/pg_flashback/tests/release_gate/config/release_gate.conf) 和 [common.sh](/root/pg_flashback/tests/release_gate/bin/common.sh)：

- `FB_RELEASE_GATE_PSQL=/home/18pg/local/bin/psql`
- `FB_RELEASE_GATE_CREATEDB=/home/18pg/local/bin/createdb`
- `FB_RELEASE_GATE_DROPDB=/home/18pg/local/bin/dropdb`
- `FB_RELEASE_GATE_PGPORT=5832`
- `FB_RELEASE_GATE_PGUSER=18pg`
- `FB_RELEASE_GATE_DBNAME=alldb`
- `FB_RELEASE_GATE_OS_USER=18pg`
- `FB_RELEASE_GATE_TARGET_TABLE_NAME=documents`
- `FB_RELEASE_GATE_DML_TABLE_NAME=leave_requests`
- `FB_RELEASE_GATE_SIM_DML_DURATION_SEC=3600`
- `FB_RELEASE_GATE_SIM_DML_WORKERS=20`
- `FB_RELEASE_GATE_SIM_DML_RATE_LIMIT_OPS=2000`

需要临时覆盖时，直接在命令前加环境变量，例如：

```bash
FB_RELEASE_GATE_OUTPUT_DIR=/tmp/rg-run \
FB_RELEASE_GATE_SIM_DML_RATE_LIMIT_OPS=3000 \
bash tests/release_gate/bin/run_release_gate.sh --only start_dml_pressure
```

## 快速开始

全流程执行：

```bash
bash tests/release_gate/bin/run_release_gate.sh
```

只看阶段清单：

```bash
bash tests/release_gate/bin/run_release_gate.sh --list-stages
```

整条链路 dry-run：

```bash
bash tests/release_gate/bin/run_release_gate.sh --dry-run
```

从某阶段开始一直跑到结束：

```bash
bash tests/release_gate/bin/run_release_gate.sh --from start_dml_pressure
```

只跑某一个阶段：

```bash
bash tests/release_gate/bin/run_release_gate.sh --only render_gate_report
```

跑指定区间：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --from capture_dml_truth_snapshots \
  --to render_gate_report
```

## 阶段清单

总入口当前固定按下面顺序编排。

### `prepare_instance`

作用：

- 检查 `archive_mode`
- 校验 `archive_command` 是否指向当前版本对应的归档目录
- 清空当前版本归档目录
- 删除大于 `100MB` 的非模板数据库
- 重建 `alldb`
- 在新建 `alldb` 后立即安装 `pg_flashback` 扩展
- 生成环境摘要 `json/environment.json`

常见用途：

- 全流程起点
- 测试前做一次实例清场

### `start_alldbsimulator`

作用：

- 启动 `/root/alldbsimulator/bin/alldbsim`
- 等待健康检查通过
- 写入 `logs/alldbsim.pid` 和 `logs/alldbsim.log`

常见用途：

- 只重启 simulator，不重做数据库

### `load_seed_data`

作用：

- 调 simulator 建数接口
- 在目标 schema 中构造约 `50 x 100MB` 的初始数据集

主要产物：

- `json/load_seed_request.json`
- `json/load_seed_response.json`
- `json/load_seed_final.json`

### `grow_target_table`

作用：

- 在 `1h` DML 压测开始前，反复向固定目标表补数据
- 直到 `pg_total_relation_size` 达到 `5GB`

边界：

- 这个阶段只负责把目标表扩到固定规模
- 扩容期间不采 random truth snapshot
- 扩容完成后才进入 `start_dml_pressure`

主要产物：

- `json/grow_target_*.json`
- `json/grow_target_final.json`

### `start_dml_pressure`

作用：

- 在基础建数与目标大表扩容都完成后启动
- 启动 `1h` DML 压测任务
- 只启动，不等待完成
- 写入运行时记录供随机快照阶段使用

主要产物：

- `json/dml_pressure_request.json`
- `json/dml_pressure_response.json`
- `json/dml_pressure_runtime.json`

### `capture_random_truth_snapshots`

作用：

- 在正在运行的 `1h` DML 压测窗口内抓 `5` 个随机时间点
- 导出 target 大表和 2-3 张中表的 truth CSV
- 生成随机时间点调度表
- 仅记录 truth snapshot 产物，后续 flashback 校验继续直接使用实例当前 live archive

主要产物：

- `json/random_snapshot_schedule.json`
- `json/truth_random_manifest.json`
- `csv/truth/*.csv`

依赖：

- 必须已有 `json/dml_pressure_runtime.json`
- 对 real run 来说，DML 任务必须仍处于运行中

### `wait_dml_pressure_finish`

作用：

- 等待 `start_dml_pressure` 启动的任务结束
- 记录最终 job 状态

主要产物：

- `json/dml_pressure_final.json`

依赖：

- 必须已有 `json/dml_pressure_runtime.json`

### `capture_dml_truth_snapshots`

作用：

- 在 `FB_RELEASE_GATE_DML_TABLE_NAME` 上执行定向 DML
- 对每个场景立即抓 truth snapshot
- 不再复制额外 WAL fixture；deterministic DML 的复核继续依赖同一套 live archive

当前固定覆盖：

- `single_insert_flashback`
- `single_update_flashback`
- `single_delete_flashback`
- `bulk_insert_10k_flashback`
- `bulk_update_10k_flashback`
- `bulk_delete_10k_flashback`
- `mixed_dml_flashback`

主要产物：

- `json/truth_dml_manifest.json`
- `json/truth_manifest.json`
- `csv/truth/*.csv`

### `run_flashback_checks`

作用：

- 基于 `truth_manifest` 执行 flashback 验证
- 覆盖三条用户路径：
  - `SELECT * FROM pg_flashback(...)`
  - `COPY (SELECT * FROM pg_flashback(...)) TO ...`
  - `CTAS AS SELECT * FROM pg_flashback(...)`

主要产物：

- `json/flashback_results.json`
- `csv/flashback/*.csv`
- `csv/materialized/*.csv`

依赖：

- 必须已有 `json/truth_manifest.json`

### `evaluate_gate`

作用：

- 对比 flashback 输出和 truth snapshot
- 对比当前耗时和 golden baseline
- 生成最终 gate verdict

主要产物：

- `json/gate_evaluation.json`
- 失败时附带 `logs/diff_*.diff`

依赖：

- 必须已有 `json/flashback_results.json`
- 必须已有 `json/truth_manifest.json`
- 必须有对应版本的 `golden/pg<major>.json`

### `render_gate_report`

作用：

- 根据环境摘要和 gate 结果渲染 Markdown 报告

主要产物：

- `reports/release_gate_report.md`

依赖：

- 必须已有 `json/environment.json`
- 必须已有 `json/gate_evaluation.json`

## 阶段操控规则

总入口支持：

- `--list-stages`
- `--from <stage>`
- `--to <stage>`
- `--only <stage>`
- `--dry-run`

语义：

- 不带参数时，按固定顺序跑完整链路
- `--from a` 表示从 `a` 开始，跑到最后
- `--to b` 表示从头跑到 `b` 结束
- `--from a --to b` 表示只跑闭区间 `[a, b]`
- `--only s` 等价于 `--from s --to s`
- `--only` 不能和 `--from/--to` 混用

重要约束：

- 从中间阶段启动时，不会自动补前置产物
- 依赖缺失就直接报错，这是刻意设计
- `cleanup` 不属于用户可选阶段
- 总入口退出时仍会：
  - 停掉 `alldbsimulator`
  - 不再自动清理当前版本归档目录

这意味着：

- 如果你想从 `capture_random_truth_snapshots` 开始跑，必须先有 `start_dml_pressure` 产出的 `dml_pressure_runtime.json`
- 如果你想只跑 `render_gate_report`，必须先有 `environment.json` 和 `gate_evaluation.json`
- 如果你想从 `run_flashback_checks` 开始复用上一轮 truth/WAL，总入口退出不会再把上一轮已有 WAL 误删

## `1h` DML 压测的真实规则

`start_dml_pressure` 阶段当前真实行为如下，README 以代码现状为准，不以设计稿推断。

目标范围：

- 作用在整个场景 `schema`
- 不是单表压测

当前启用的 DML：

- `insert`
- `update`
- `delete`

权重规则：

- 三种操作都启用
- 三者权重都为 `1`
- 因此当前是等权随机混合压测

执行模型：

1. 每个 worker 开启一个事务
2. 按权重随机选择一次 `insert/update/delete`
3. 在目标 `schema` 中随机选择一张支持该操作的表
4. 该事务只执行 `1` 个 DML
5. 提交后继续下一轮

速率与并发：

- 默认时长：`3600s`
- 默认并发：`20 workers`
- 默认总限速：`2000 ops/s`

注意：

- 这里的 `2000 ops/s` 是整个任务共享的总速率，不是每个 worker 各 `2000`
- 这 `1h` 压测开始前，`grow_target_table` 已先把目标大表扩到 `5GB`
- `bulk_insert_10k` / `bulk_update_10k` / `bulk_delete_10k` / `mixed_dml`
  不属于这 `1h` 压测本体
- 上述批量与混合场景是在 `capture_dml_truth_snapshots` 阶段单独构造

## 日志口径

- release gate 统一日志前缀固定带时间戳
- 便于串联阶段编排、DML 窗口、快照采集和异常退出现场

## 产物目录

默认输出目录：

- `tests/release_gate/output/latest/`

主要结构：

- `json/`
  - 机器可读的阶段产物、manifest、gate 结果
- `csv/truth/`
  - truth snapshot 导出
- `csv/flashback/`
  - `SELECT * FROM pg_flashback(...)` 结果
- `csv/materialized/`
  - `COPY TO` / `CTAS` 结果
- `logs/`
  - 运行日志、simulator 日志、diff 文件
- `reports/`
  - 最终 Markdown 报告

如果要保留多轮结果，建议每次显式指定：

```bash
FB_RELEASE_GATE_OUTPUT_DIR=tests/release_gate/output/2026-04-04-run1 \
bash tests/release_gate/bin/run_release_gate.sh
```

## 常用操作示例

只重跑 flashback 检查和 gate 判定：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --from run_flashback_checks \
  --to render_gate_report
```

已经有运行中 DML 任务，只补抓随机 truth：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --only capture_random_truth_snapshots
```

只补报告：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --only render_gate_report
```

只启动 DML 压测，不等待：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --only start_dml_pressure
```

启动后单独等待结束：

```bash
bash tests/release_gate/bin/run_release_gate.sh \
  --only wait_dml_pressure_finish
```

## 自检

脚本级自检：

```bash
bash tests/release_gate/bin/selftest.sh
```

这个自检会检查：

- 脚本文件存在且可执行
- shell 语法有效
- JSON 配置有效
- `--list-stages`
- `--only <stage>`
- `--from <stage> --to <stage>`

## 当前已知限制

- 若本机 `archive_command` 不指向 `/walstorage/<major>waldata`，总入口 real run 会被环境 gate 拦住
- 从中间阶段启动时不会自动恢复缺失上下文
- 当前 `1h` 压测本体只覆盖基础三类 DML；批量 `10k` 和 mixed case 仍在定向快照阶段单独构造
