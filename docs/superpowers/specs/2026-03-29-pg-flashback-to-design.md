# `pg_flashback_to` 设计稿

## 目标

新增：

```sql
SELECT pg_flashback_to('public.t1'::regclass, '2026-03-29 10:00:00+08');
```

它会在同 schema 下创建 `t1_flashback`，把 `target_ts` 时刻的历史结果批量写入该表，供用户后续直接查询。

## 边界

- 目标表固定为 `logged` 普通持久表
- 命名固定为 `<source_relname>_flashback`
- 同名表已存在直接报错
- 首版复制：
  - 列定义
  - `NOT NULL`
  - `DEFAULT`
  - `CHECK`
  - `PRIMARY KEY`
  - `UNIQUE`
  - 普通索引
- 首版不复制：
  - 外键
  - 触发器
- 不改写原表

## 方案

核心 flashback 主链保持不变，只替换输出端：

```text
pg_flashback_to()
  -> clone base table shape
  -> flashback query begin
  -> bulk-load rows into target table
  -> add PK/UNIQUE constraints
  -> create remaining indexes
```

其中：

- 建表阶段使用 `LIKE` 复制列、默认值、约束等基础定义，但不预建索引
- 数据装载阶段使用 copy-style 批量写入，不走逐行 `INSERT`
- 约束/索引恢复阶段：
  - `PRIMARY KEY` / `UNIQUE` 用约束定义恢复
  - 非约束索引用索引定义恢复

## 性能判断

- 该能力能减少“把大结果集持续发给客户端”的成本
- 但不会显著降低 `WAL scan / replay / reverse-op / apply` 自身的峰值内存
- 它更适合：
  - 同一时间点结果需要反复使用
  - 后续还要联表、过滤、导出
- 它不是当前 flashback 主链慢问题的根修
