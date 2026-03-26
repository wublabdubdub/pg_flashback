# ADR-0005: 公开入口改为直接结果集查询与变化集小内存执行

## 状态

Accepted

## 决策

公开用户入口从“返回结果表名”改为“直接返回结果集”：

```sql
SELECT *
FROM pg_flashback(
  NULL::public.t1,
  '2026-03-25 10:00:00+08'
);
```

- `pg_flashback(anyelement, text)`
  - 主用户入口
  - 第一个参数使用 `NULL::schema.table` 作为目标表复合类型锚点
  - 第二个参数使用 `text` 传入目标时间点
  - 直接返回 `SETOF anyelement`
  - 不创建结果表
  - 不经过 `tuplestore`
  - 结果按 SRF 逐行返回

apply 执行口径同时改为变化集驱动的小内存模型：

- `keyed`
  - 仅维护“窗口内发生变化的 key -> reverse action list”
  - 当前表扫描时，未命中变化 key 的当前行直接返回
  - 命中变化 key 的当前行在扫描时即时应用 action list 后决定是否返回
  - 扫描结束后只补发“当前不存在、但 target_ts 时存在”的 residual 历史行
- `bag`
  - 仅维护“窗口内发生变化的 row_identity -> delta_count”
  - 当前表扫描时按负向 delta 吞掉应回退消失的当前行
  - 扫描结束后按正向 delta 补发 residual 历史行

## 原因

- 用户无法接受 `AS t(...)` 展开几百列
- 用户也无法接受先创建结果表、再二次查询的交互模型
- 12GB 级当前表不允许在 apply 阶段构造 12GB 级内存工作集
- 将内存占用从“与表大小相关”改为“与变化集大小相关”是首要约束
- 删除结果表后，单次查询链路减少了一次 bulk insert 和一次结果表再扫描

## 明确不做

- 不保留 `pg_flashback(text, text, text)` 的结果表入口
- 不保留 `parallel_apply_workers` 与并行结果表写入方案
- 不回到 `SETOF record + AS t(...)` 的用户形态
- 不为首版引入 parser hook / 自定义 SQL 语法
- 不复制当前整表到内存或最终结果表
