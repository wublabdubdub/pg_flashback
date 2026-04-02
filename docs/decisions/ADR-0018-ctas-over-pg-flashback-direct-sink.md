# ADR-0018: `pg_flashback(...)` full-output 快路径

## 状态

Accepted

## 决策

删除 `pg_flashback_to(regclass, text)` 后，全表闪回若需落地到库内新表，正式承接面固定为 PostgreSQL 原生：

```sql
CREATE TABLE new_table AS
SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);
```

而：

```sql
COPY (
  SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text)
) TO ...
```

只作为导出路径，不作为“直接创建闪回结果表”的产品路线。

在实现层：

- 保持用户继续使用标准 `CTAS`
- 不新增新的公开 helper
- 不替换 PostgreSQL 原生 `CTAS` receiver
- 统一把优化重点放到 `pg_flashback(...)` 的 full-output 上游快路径：
  - `SELECT * FROM pg_flashback(...)`
  - `CTAS AS SELECT * FROM pg_flashback(...)`
  - `COPY (SELECT * FROM pg_flashback(...)) TO ...`

## 原因

- PostgreSQL 内核原生支持 `CREATE TABLE AS query`
- PostgreSQL `COPY` 原生只支持：
  - `COPY table FROM ...`
  - `COPY (query) TO ...`
- 不存在“`COPY FROM SELECT` 直接创建新表”的内核语法
- 当前 `pg_flashback(...)` 已在 `SELECT` 与 `CTAS` 下进入扩展 `CustomScan` 链
- PostgreSQL `CTAS` 内核 `IntoRelDestReceiver` 本身已直接使用 `BulkInsertState + table_tuple_insert()`
- 因此“替换 CTAS receiver 本身”不是当前最值得做的优化点
- 对全表闪回导出/落地而言，更值得收口的是共享的 full-output 行产出与 tuple 搬运成本

## 影响

- 产品边界继续收口为一个公开 flashback 入口：
  - `pg_flashback(anyelement, text)`
- 用户若要库内新表，写标准 `CTAS AS SELECT * FROM pg_flashback(...)`
- 用户若要导出，写 `COPY (SELECT * FROM pg_flashback(...)) TO ...`
- 未来实现应优先让三条 full-output 路径共享同一条快路径，而不是只偏向 `CTAS`

## 不做的事

- 不恢复 `pg_flashback_to`
- 不新增新的公开建表 helper
- 不把 `COPY` 伪装成“建新表”路径
- 不替换 PostgreSQL 原生 `CTAS` bulk insert receiver
- 不在本 ADR 中引入后台预物化历史快照体系
