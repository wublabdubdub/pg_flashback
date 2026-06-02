# PG14-18 Release Gate Design

## 背景

当前仓库缺少一套可重复、可脚本化、可供 AI agent 持续复用的发布前功能测试标准流程。

现有回归与 deep 测试可以覆盖部分功能，但还不能稳定回答以下发布前问题：

- 在空实例上重新构造大体量数据库后，`pg_flashback` 是否仍能正确工作
- 在大 WAL 压力与 5GB 大表场景下，随机时间点闪回是否仍与 truth snapshot 一致
- `COPY (SELECT * FROM pg_flashback(...)) TO ...` 与 `CTAS AS SELECT * FROM pg_flashback(...)` 是否仍正确
- 当前版本相对仓库维护的 golden baseline 是否出现可阻断发布的性能回归

本设计将上述流程固化为一套“文档 + 可执行脚本规范”的发布前 gate。

## 目标

建立一套固定面向 `PG14-18` 的发布前 gate，满足：

1. 可在空 PostgreSQL 实例上独立执行
2. 可调用 `/root/alldbsimulator` 自动构造 `alldb` 测试数据库
3. 可生成足够大的 WAL 与 5GB 目标表
4. 可对随机时间点与定向 DML 场景执行 flashback 正确性验证
5. 可对查询、`COPY TO`、`CTAS` 三类用户路径统一验证
6. 可与仓库内 golden baseline 比较并做阻断式性能判定
7. 可输出单独的 Markdown 报告，供发布记录和复盘使用
8. 可被后续 agent 直接复用，而不需要重新发明测试流程

## 非目标

本轮不做：

- 不把该发布前 gate 并入 `installcheck`
- 不把该 gate 作为日常开发必跑用例
- 不覆盖 `PG13` 及以下版本
- 不纳入“无主键表 bag/multiset 正确性”作为首版主场景
- 不依赖“上一发布版本”做性能基线
- 不把 truth source 建立在离线操作日志重算上

## 总体方案

采用三层结构：

1. 规范文档
   - 固定环境要求、步骤、场景矩阵、通过口径、报告格式、golden baseline 更新规则
2. 可执行脚本
   - 负责空实例准备、`alldbsimulator` 造数与压测、truth snapshot 采集、flashback 场景执行、性能判定、报告生成
3. golden baseline
   - 仓库内按 `PG14-18` 分别维护，作为发布阻断基线

推荐主入口为 shell orchestrator，不把控制逻辑塞进数据库元数据表。

## 版本范围

发布前 gate 固定覆盖：

- `PG14`
- `PG15`
- `PG16`
- `PG17`
- `PG18`

每个版本都执行：

- 功能正确性 gate
- 性能回归 gate

## 环境与归档约束

### 空实例要求

gate 必须在“空实例”口径上启动，并在测试前执行清理：

- 枚举非模板数据库
- 若数据库大小大于 `100MB`，则删除
- 确认测试库 `alldb` 不存在或已被清理
- 重新创建 `alldb`

### 归档要求

归档根目录固定为：

- `/walstorage`

各 PostgreSQL 主版本固定使用对应子目录：

- `PG14 -> /walstorage/14waldata`
- `PG15 -> /walstorage/15waldata`
- `PG16 -> /walstorage/16waldata`
- `PG17 -> /walstorage/17waldata`
- `PG18 -> /walstorage/18waldata`

gate 启动时必须：

1. 根据当前 PostgreSQL 主版本解析目标归档目录
2. 强校验实例当前归档实际落点就是对应子目录
3. 若归档未开启或未指向正确目录，则直接失败
4. 在测试前清空当前版本对应的归档子目录

若本次执行从全流程起点开始，且实际跑过实例准备阶段，则 gate 结束时无论成功还是失败，都必须：

- 再次清空当前版本对应的归档子目录

若本次执行是从中间阶段恢复：

- 不得在退出时清理已有归档目录
- 避免复跑 `run_flashback_checks` / `render_gate_report` 时误删前序阶段生成的 WAL

报告中必须记录：

- 目标归档目录
- 测试前清理是否成功
- 测试期间峰值占用
- 测试后清理是否成功

## 数据构造流程

### 1. 创建 `alldb`

固定使用名为 `alldb` 的数据库作为本轮 gate 数据库。

### 2. 基础造数

通过 `/root/alldbsimulator` 向 `alldb` 中插入：

- `50` 张表
- 每张表目标大小约 `100MB`

造数结束后必须校验：

- 表数量
- 每表大小
- 总库大小

若造数明显不达标，则直接失败，不进入后续阶段。

### 3. 5GB 目标大表

从已造出的表中固定选定一张目标表，并将其继续插入扩展到 `5GB`。

固定原则：

- 目标表名应来自配置，不要每次随机选
- 这样便于 baseline 稳定与版本间对比

必须记录：

- 目标表名
- 扩容开始时间
- 扩容结束时间
- 扩容后 `pg_total_relation_size`

扩容阶段约束：

- 不在该阶段采随机 truth snapshot
- 不把该阶段生成的时间点纳入 flashback 正确性窗口

### 4. 一小时 DML 压测

继续使用 `/root/alldbsimulator` 对 `alldb` 执行持续 `1h` 的 DML 压测，确保在“目标大表已完成扩容”的固定数据基线上生成足够的 WAL。

压测期间至少覆盖：

- 单行 `insert`
- 单行 `update`
- 单行 `delete`

批量与混合场景单独处理：

- 批量 `10000` 行 `insert`
- 批量 `10000` 行 `update`
- 批量 `10000` 行 `delete`
- 混合 `insert/update/delete`

这些场景不并入 `1h` schema 级压测本体，而是在后续定向 snapshot 阶段单独构造。

必须记录：

- `pressure_start_ts`
- `pressure_end_ts`
- 数据集随机种子

## truth snapshot 设计

### truth source

正确性 truth source 固定为：

- 测试过程主动打快照

不采用：

- 事后基于操作日志离线重算 truth

### 随机时间点

在“1 小时 DML 压测窗口”内随机抽取 `5` 个时间点。

要求：

- 必须固定 `random_seed`
- 随机过程可重放
- 随机选出的时间点必须写入运行产物和报告

### snapshot 采集

对每个随机时间点，立即导出 truth snapshot。

首版 truth snapshot 至少覆盖：

- 5GB 大表
- 2-3 张代表性 `100MB` 中表

随机 snapshot 采集窗口固定为：

- `grow_target_table` 完成之后
- `1h` DML 压测运行期间

## 运维可观测性补充

统一 shell 日志输出需要：

- 每条记录带时间戳
- 保持稳定前缀，便于按阶段和故障时刻做 grep / 关联分析

每个 snapshot 必须记录：

- `scenario_id`
- `target_ts`
- `table_name`
- `row_count`
- `sha256`
- `file_path`

## flashback 场景矩阵

### 随机场景

固定执行：

- `random_flashback_1`
- `random_flashback_2`
- `random_flashback_3`
- `random_flashback_4`
- `random_flashback_5`

即在压测一小时内任意五个随机时间点执行闪回。

### 定向 DML 场景

固定执行：

- `single_insert_flashback`
- `single_update_flashback`
- `single_delete_flashback`
- `bulk_insert_10k_flashback`
- `bulk_update_10k_flashback`
- `bulk_delete_10k_flashback`
- `mixed_dml_flashback`

### 落盘场景

固定执行：

- `copy_to_flashback`
- `ctas_flashback`

其中：

- `copy_to_flashback` 使用
  - `COPY (SELECT * FROM pg_flashback(...)) TO ...`
- `ctas_flashback` 使用
  - `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)`

### 场景覆盖范围

发布前 gate 的性能重点放在：

- 5GB 目标大表
- 2-3 张代表性中表

不要求对全部 50 张表做全量 flashback，对它们的主要作用定义为：

- 作为 WAL 背景噪声
- 保证时间窗与 archive/WAL 规模足够大

## 正确性判定

### 标准化输出

所有 truth snapshot 与 flashback 结果统一导出为标准化 CSV。

首版正确性 gate 主要覆盖“有主键/稳定唯一键”的表，导出规则为：

- 固定列顺序
- 固定编码
- 固定 `NULL` 表示
- 固定 `ORDER BY key`

`COPY TO` 与 `CTAS` 结果也统一落回标准化 CSV 后再比较。

### 判定层级

每个场景正确性校验分三层：

1. `row_count` 一致
2. 结果文件 `sha256` 一致
3. 若不一致，保留全量 diff 产物

任何一个场景正确性失败，均直接判定本轮 gate 失败。

## 性能基线与阻断规则

### baseline 来源

性能基线固定来自仓库维护的 golden baseline，而不是“上一发布版本”。

目录建议：

- `tests/release_gate/golden/pg14.json`
- `tests/release_gate/golden/pg15.json`
- `tests/release_gate/golden/pg16.json`
- `tests/release_gate/golden/pg17.json`
- `tests/release_gate/golden/pg18.json`

### baseline 内容

每个 golden baseline 文件至少记录：

- baseline 生成日期
- `pg_flashback` commit / version
- PostgreSQL 主版本
- 机器指纹摘要
- 数据集 seed
- 目标表名
- 归档目录约束
- 场景列表

每个场景记录：

- `scenario_id`
- `operation_kind`
- `table_class`
- `expected_row_count`
- `expected_hash`
- `warmup_policy`
- `baseline_elapsed_ms`
- `baseline_output_bytes`
- `ratio_threshold`
- `absolute_threshold_ms`

### 预热与计时

为减小冷缓存抖动，性能 gate 固定执行：

1. 第一次执行：`warm-up`，不计入 gate
2. 第二次执行：`measured`
3. 第三次执行：`measured`
4. 取两次 measured run 的较小值或中位值作为本次场景耗时

golden baseline 也必须用同一规则生成。

### 双阈值阻断公式

若某场景同时满足：

- `current_ms > baseline_ms * (1 + ratio_threshold)`
- `current_ms - baseline_ms > absolute_threshold_ms`

则判定该场景性能回归，阻断发布。

### 失败类型

报告中需要明确区分：

- `correctness_fail`
- `performance_regression_fail`
- `infrastructure_fail`

## 目录与脚本分层

建议新增独立目录：

- `tests/release_gate/`
  - `README.md`
  - `bin/`
  - `sql/`
  - `config/`
  - `golden/`
  - `templates/`

建议脚本职责如下：

- `run_release_gate.sh`
  - 总编排入口
- `prepare_empty_instance.sh`
  - 清空实例、检查归档、创建 `alldb`
- `load_alldb_seed.sh`
  - 调 `alldbsimulator` 建 `50 x 100MB`
- `run_alldb_dml_pressure.sh`
  - 跑 `1h` DML 压测
- `grow_flashback_target.sh`
  - 扩容到 `5GB`
- `capture_truth_snapshots.sh`
  - 采集五个随机时间点 truth snapshot
- `run_flashback_matrix.sh`
  - 执行全部 flashback 场景
- `evaluate_gate.sh`
  - 读取结果与 golden baseline，给出 verdict
- `render_report.sh`
  - 生成 Markdown 报告

## 报告格式

每次运行必须生成单独 Markdown 报告，建议结构固定为：

1. 标题
   - PG 版本
   - 日期
   - commit
   - 执行人或 agent
2. 总结
   - `PASS/FAIL`
   - 失败分类
   - 场景通过率
3. 环境信息
   - PG 版本
   - 扩展版本/commit
   - `archive_mode`
   - 归档目录
   - seed
4. 数据集摘要
   - `50 x 100MB` 达成情况
   - 5GB 目标表信息
   - WAL / 归档摘要
5. 正确性结果
   - 每个场景的 `row_count/hash/diff`
6. 性能结果
   - 当前耗时
   - golden 值
   - 相对变化
   - 绝对变化
   - 阈值
   - 是否阻断
7. 失败明细
   - 仅列失败场景
   - 指向原始产物路径
8. 附录
   - 随机时间点清单
   - truth snapshot 路径
   - 原始日志路径

## 执行口径

首版标准发布前 gate 的通过条件固定为：

1. 所有正确性场景通过
2. 所有性能场景未触发双阈值回归
3. 归档目录清理前后都成功
4. 报告成功生成

只要任一条件失败，即本轮发布 gate 失败。

## 风险与后续实现注意点

### 风险

- `alldbsimulator` 当前未必直接暴露“按场景打点 + 立即 truth snapshot”能力，可能需要额外包装脚本
- 大量数据导出和 hash 计算本身也会有成本，需要避免把报告生成时间误算进 flashback 性能
- 若机器噪声较大，双阈值需要用 golden baseline 数据继续调优

### 实现注意点

- 所有随机行为都必须固定 seed 并写入产物
- 结果文件格式必须严格固定，避免无关格式抖动污染 hash
- warm-up 与 measured run 必须严格分离
- 归档目录清理必须放进 `trap/finally` 风格的收尾逻辑，不能只在成功路径清理
- baseline 更新必须是显式动作，不能在普通 gate 中自动覆盖

## 验收标准

当该设计完成实现后，应至少满足：

1. agent 能通过一个统一入口启动 `PG14-18` 任一版本的 release gate
2. agent 不需要手工拼接 `alldbsimulator` 与 flashback 校验步骤
3. gate 输出单独 Markdown 报告
4. gate 能对正确性和性能回归给出明确 PASS/FAIL
5. `/walstorage/<major>waldata` 在每轮前后都被正确清理
