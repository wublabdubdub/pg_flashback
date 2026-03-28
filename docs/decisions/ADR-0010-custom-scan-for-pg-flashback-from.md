# ADR-0010: 用 CustomScan 接管 FROM pg_flashback(...)

## 状态

已接受

## 背景

当前公开入口固定为：

```sql
SELECT *
FROM pg_flashback(NULL::schema.table, target_ts_text);
```

此前已移除扩展内部的 materialized SRF 分支，并将 `pg_flashback()` 实现统一收口为 `ValuePerCall`。

但是线上 live case 调试证明，这并不足以阻止大结果集在 `stage 8/9` 期间大量写入 `base/pgsql_tmp/*`：

- 计划节点仍是 `Function Scan on public.pg_flashback`
- PostgreSQL `nodeFunctionscan.c` 会在首次取数时调用 `ExecMakeTableFunctionResult(...)`
- `ExecMakeTableFunctionResult(...)` 会将 table SRF 的全部输出灌入 `tuplestore`
- `work_mem` 不足时会落盘到 `base/pgsql_tmp/*`

因此，“扩展内部只走 `ValuePerCall`”与“用户查询不会产生 temp spill”不是同一件事。

## 决策

保留当前 SQL 用户接口不变，但不再依赖 PostgreSQL 默认的 `FunctionScan` 执行 `FROM pg_flashback(...)`。

新增规划/执行层接管：

- 为 `pg_flashback(anyelement, text)` 绑定 planner support function，使新会话在规划阶段就加载模块并完成 `_PG_init()`
- 在 planner `set_rel_pathlist_hook` 中识别 `RTE_FUNCTION`
- 当该 `RTE_FUNCTION` 的唯一函数调用为 `pg_flashback(anyelement, text)` 时，向该 baserel 注入 `CustomPath`
- 由 `CustomPath -> CustomScan` 替换默认 `FunctionScan`
- 在 `CustomScan` executor 中直接复用现有 `fb_entry -> fb_apply` 流式链路逐行产出结果
- 不再经过 `ExecMakeTableFunctionResult(...)` 和 `tuplestore`

## 结果

预期效果：

- 保持现有调用形态 `SELECT * FROM pg_flashback(...)`
- 避免 `FunctionScan` 对 `pg_flashback()` 全量 materialize
- 将 temp spill 风险从“必然由 `FunctionScan` 触发”降为“仅在上层其他节点确实需要时触发”
- `count(*) FROM pg_flashback(...)` 这类查询不再因为 table SRF 框架本身而把全部历史结果写入 `pgsql_tmp`

## 代价

- 需要引入 planner hook、CustomPath、CustomScan、executor 状态管理
- 需要为 `RTE_FUNCTION` 自定义 scan tuple descriptor / qual / projection 适配
- 这是一次架构级改动，验证面覆盖 planner、executor、SRF 生命周期和 explain 输出

## 不做的事

- 不修改用户 SQL 入口
- 不回退到扩展内部 materialized SRF
- 不尝试继续优化 `FunctionScan` 路径；该路径的根因在 PostgreSQL 内核默认 table-function 执行模型
