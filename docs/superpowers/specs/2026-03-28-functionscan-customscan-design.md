# FROM pg_flashback CustomScan 设计

## 目标

在不改变当前用户 SQL 入口的前提下：

```sql
SELECT *
FROM pg_flashback(NULL::schema.table, target_ts_text);
```

避免 PostgreSQL 默认 `FunctionScan` 将 `pg_flashback()` 的全部输出 materialize 到 `tuplestore`，从而消除由 `FunctionScan -> ExecMakeTableFunctionResult -> base/pgsql_tmp/*` 带来的大规模临时文件写入。

## 根因

当前扩展内部虽然已经只实现 `ValuePerCall`，但这只能决定函数如何向调用方逐次返回行，不能决定 `FROM` 子句中的 table function 如何被 PostgreSQL executor 消费。

在 `FROM pg_flashback(...)` 场景下：

- planner 生成 `FunctionScan`
- executor `nodeFunctionscan.c` 首次取数时调用 `ExecMakeTableFunctionResult(...)`
- 该函数会把 table SRF 的全部输出灌进 `tuplestore`
- 超过 `work_mem` 后自动 spill 到 `base/pgsql_tmp/*`

因此，问题不在扩展内部的 SRF 模式选择，而在 PostgreSQL 对 `RTE_FUNCTION` 的默认执行节点。

## 方案对比

### 方案 A：继续优化现有 ValuePerCall

不做 planner/executor 接管，只继续优化扩展内部逐行发射。

缺点：

- 无法改变 `FunctionScan` 必然 materialize 的事实
- 无法消除 `base/pgsql_tmp/*`

### 方案 B：更改用户入口

放弃 `FROM pg_flashback(...)`，改成其他对象类型或调用形态。

优点：

- 可以彻底绕开 `FunctionScan`

缺点：

- 破坏既有用户接口
- 与当前产品口径冲突

### 方案 C：推荐，使用 CustomScan 接管 `FROM pg_flashback(...)`

保留用户 SQL 接口不变，但在 planner 中识别目标 `RTE_FUNCTION`，将默认 `FunctionScan` 替换为扩展自己的 `CustomScan`。

优点：

- 接口不变
- 彻底绕开 `FunctionScan -> tuplestore`
- 可以直接复用现有 `fb_apply` 流式主链

缺点：

- 需要新增 planner hook 和 custom scan executor 代码

## 设计

### 1. 识别目标 RTE_FUNCTION

为了让新会话在规划当前语句前就能拿到 hook，`pg_flashback(anyelement, text)` 还需要绑定一个轻量 planner support function。该 support function 不负责改写计划，只负责让 PostgreSQL 在 `set_function_size_estimates()` 阶段提前加载 `pg_flashback.so`，从而执行 `_PG_init()` 并注册 `set_rel_pathlist_hook`。

之后在 `set_rel_pathlist_hook` 中检查：

- `rte->rtekind == RTE_FUNCTION`
- `rte->functions` 只有一个 `RangeTblFunction`
- 函数调用是 `pg_flashback(anyelement, text)`

仅在上述条件全部成立时，向该 relation 注入 `CustomPath`。

### 2. planner 输出 CustomPath / CustomScan

新增 `fb_custom_scan` 模块，负责：

- `CustomPathMethods`
- `CustomScanMethods`
- `CustomExecMethods`

规划阶段：

- 为目标 relation 构造 `CustomPath`
- 成本初期直接复用/近似默认 `FunctionScan` 成本
- 不支持 backward / mark-restore
- 初期不启用 parallel custom scan

计划阶段：

- 输出 `CustomScan`
- `scan.scanrelid = 0`
- 通过 `custom_scan_tlist` 描述输出 tuple 结构
- `custom_scan_tlist` 必须按目标表完整列布局构造，不能直接复用上层 `tlist`
- 将解析后的目标 relation 信息、目标时间文本等必要元数据序列化进 `custom_private`

### 3. 执行阶段直接复用现有 flashback 主链

`BeginCustomScan`：

- 解析 `custom_private`
- 建立与 `pg_flashback()` 相同的查询状态
- 调用现有入口级 helper，初始化 runtime / wal / replay / reverse / apply

`ExecCustomScan`：

- 每次调用只取一行
- 复用 `fb_apply_next(...)`
- 将结果填入 `CustomScanState` 的结果 slot

`EndCustomScan`：

- 释放 flashback 查询状态

### 4. 代码边界

尽量不把现有 `fb_entry.c` 再做大，抽出一层可复用的“flashback query session”：

- 现有 `pg_flashback(PG_FUNCTION_ARGS)` 继续服务非 custom-scan 路径和已有测试
- CustomScan 和 SQL SRF 共享同一套构建/清理逻辑
- `fb_apply` / `fb_replay` / `fb_wal` 不感知 planner/executor 差异

### 5. 验证目标

对于用户 live case：

```sql
SELECT count(*) FROM pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-03-28 18:25:13');
```

目标是：

- 计划不再出现 `Function Scan`
- `stage 8/9` 期间不再生成 `base/pgsql_tmp/pgsql_tmp*`
- `/isoTest` 空间不再因该查询的 table-function 框架而暴涨

## 风险

- CustomScan 需要正确处理 tuple descriptor、projection、qual
- 若某些查询形态仍被 planner 包裹为会强制 materialize 的上层节点，仍可能产生 temp spill，但那不再是 `FunctionScan` 的必然行为
- 需要谨慎处理 executor 生命周期，避免重复清理和中断清理缺失
