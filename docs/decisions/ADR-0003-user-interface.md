# ADR-0003: 最终用户入口采用 `pg_flashback()` 返回结果表名

## 状态

Accepted

## 决策

用户态主入口定为：

```sql
SELECT pg_flashback(
  'fb1',
  'public.t1',
  '2026-03-22 10:00:00+08'
);

SELECT * FROM fb1;
```

- `pg_flashback(text, text, text)`
  - 主用户入口
  - 纯文本参数
  - 自动解析表名和时间
  - 自动从数据字典生成列定义
  - 默认创建 `UNLOGGED` 结果表
  - 默认走当前 backend 内串行 apply/write
  - 显式设置 `pg_flashback.parallel_apply_workers > 0` 后，可切到 bgworker 并行 apply/write
  - 不保留 `fb_create_flashback_table(text, text, text)` 兼容别名

## 原因

- `pg_flashback()` 能消除：
  - `::regclass`
  - `::timestamptz`
- 用户不应接触内部 `SETOF record` / `AS t(...)` 形态
- 开发期 debug/旁路 SQL 入口会稀释产品边界，也会误导性能判断
- 结果直接落表并返回表名，仍然保留“先调用，再查询结果表”的使用习惯
- 默认 `UNLOGGED` 结果表比旧的 `tuplestore -> TEMP TABLE` 路径更符合当前速度优先目标
- 并行 apply/write 只能安全落在共享 `UNLOGGED` 结果表上，不能继续要求 `TEMP TABLE` 语义
- 若显式开启并行路径，结果表由独立 worker 自主事务创建并提交，因此不再跟随调用方事务回滚

## 明确不做

- 不对用户暴露内部 `SETOF record` / `AS t(...)` 形态
- 不把开发期 debug/旁路 SQL 入口保留为产品能力
- 不为首版引入解析器级语法扩展
- 不自动覆盖已存在的目标结果表
