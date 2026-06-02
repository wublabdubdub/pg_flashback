# 流式结果集 flashback 设计规格

## 背景

当前 `pg_flashback` 虽然已经具备页级重放、`ForwardOp/ReverseOp` 和 `keyed/bag` 正确性主链，但公开入口仍要求：

1. 调用 `pg_flashback(text, text, text)`
2. 创建结果表
3. 再 `SELECT * FROM result_table`

这一方案不满足新的产品约束：

- 最终结果不落盘
- 不接受 `AS t(...)`
- 对 12GB 级大表，apply 阶段不能再按表大小构造同量级内存工作集

## 新公开接口

公开主入口改为：

```sql
SELECT *
FROM pg_flashback(
  NULL::public.big_table,
  '2026-03-25 10:00:00+08'
);
```

接口含义：

- `NULL::public.big_table` 提供目标表的复合类型锚点
- 函数签名为 `pg_flashback(anyelement, text) RETURNS SETOF anyelement`
- PostgreSQL 通过 polymorphic result type 自动推导输出列结构
- 用户无需再写 `AS t(...)`

## 执行模型

### 1. WAL / replay / reverse-op 主链保持不变

以下路径继续沿用当前实现：

- `target_ts -> query_now_ts` WAL 时间窗扫描
- `checkpoint + FPI + block redo`
- `ForwardOp`
- `ReverseOp`

### 2. apply 从“当前整表工作集”改为“变化集驱动”

旧模型：

- keyed：当前整表按 key 装入 hash
- bag：当前整表按 row_identity/count 装入 hash

新模型：

- keyed：仅维护变化 key 的 reverse action list
- bag：仅维护变化 row_identity 的 delta_count

这样内存占用不再与当前表大小线性相关。

### 3. keyed 流式算法

对 `ReverseOp` 做预处理，生成：

```text
key_identity -> [REMOVE | ADD(old_tuple)]*
```

扫描当前表时：

- key 不在变化集：当前行直接返回
- key 在变化集：把 action list 作用在“当前行或空状态”上，立即决定该 key 在 `target_ts` 时是否有结果行

扫描结束后：

- 对于当前扫描中未出现的变化 key，再以空状态执行 action list
- 若结果非空，则补发 residual 历史行

### 4. bag 流式算法

对 `ReverseOp` 做预处理，生成：

```text
row_identity -> delta_count
row_identity -> representative_tuple
```

扫描当前表时：

- 若某 `row_identity` 的 delta 仍为负，消费掉一条当前行，不返回
- 否则直接返回当前行

扫描结束后：

- 对 delta 仍为正的 identity，按次数补发 representative 历史行

### 5. SRF 逐行返回

入口改为 value-per-call SRF：

- 不使用 `tuplestore`
- 不创建结果表
- 不保留跨调用的最终结果缓存
- 只保留：
  - replay / reverse-op 状态
  - keyed action list 或 bag delta map
  - 当前表扫描游标

## 内存口径

本次改造的明确目标是：

- 不再为“当前整表基线”分配与表大小线性相关的 apply 内存
- 当前 apply 内存规模只与变化集相关

本次首版不解决的点：

- 大时间窗 / 高变化率下，`ReverseOp` 自身仍可能很大
- 后续继续向 bounded spill 演进，但这不是本次替换结果表入口的阻塞项

## 删除项

本次落地后需要删除：

- `pg_flashback(text, text, text)` 入口
- `parallel_apply_workers`
- `fb_parallel` 模块
- `fb_flashback_materialize` 遗留夹具
- 所有“先创建结果表再查询”的回归与文档描述
