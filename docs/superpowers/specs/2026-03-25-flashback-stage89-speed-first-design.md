# Flashback Stage 8/9 Speed-First Design

**日期**：2026-03-25

## 目标

把当前 `pg_flashback()` 的结果落表路径从：

- `apply -> tuplestore`
- `tuplestore -> TEMP TABLE`

改成速度优先的：

- `apply -> UNLOGGED heap result table`

并为后续真正的 background worker 并行 `apply/write` 预留稳定边界。

## 已确认取舍

- 当前优先级是速度，不再优先保持 `TEMP TABLE` 语义
- 用户入口仍保留：
  - `pg_flashback(text, text, text)`
- 结果表改为 `UNLOGGED heap`
- 首轮实现先完成“直写结果表”基础重构
- 真正的并行 worker 写入不在本轮一次性硬上

## 当前瓶颈

当前主链里，阶段 `8/9` 的成本包含两次结果物化：

1. `fb_apply_*` 先把最终结果写入 `tuplestore`
2. `pg_flashback()` 再逐行从 `tuplestore` 读取，并 `simple_table_tuple_insert()` 到结果表

这带来：

- 一次额外的 tuple deform / materialize
- 一次额外的中间层遍历
- 一次额外的入口层单线程写表瓶颈

## 目标架构

### 第一阶段：直写 `UNLOGGED heap`

- `fb_apply_keyed` / `fb_apply_bag` 不再输出到 `tuplestore`
- apply 层改为向统一 `result sink` 输出最终行
- `pg_flashback()` 在 apply 前先创建 `UNLOGGED` 结果表
- stage `9` 由 apply 末尾直接驱动写表与进度

### 第二阶段：并行 apply/write

- 基于第一阶段新增的 `result sink` 边界
- 将 keyed / bag 最终工作集按分区拆分
- 由多个 background worker 各自处理一部分分区并写共享结果表

## 结果表语义

- 结果表不再是 `TEMP TABLE`
- 结果表改为 `UNLOGGED heap`
- 仍由用户传入结果表名
- 如果同名表已存在，继续直接报错

速度收益优先于会话隔离语义。

## 模块改造

### `fb_entry`

- 负责创建 `UNLOGGED` 结果表
- 不再负责从 `tuplestore` 二次搬运结果
- 改用 table-AM bulk insert 路径承接 sink 写入

### `fb_apply_keyed` / `fb_apply_bag`

- 当前表扫描与 reverse op 应用逻辑保持不变
- 最终输出阶段改为直接写 sink
- 进入 stage `9` 后直接写结果表并推进百分比

### `fb_apply`

- 新增统一 `result sink` 抽象
- 首轮提供 table sink
- 后续 parallel worker 版本复用同一接口

## 进度模型

- stage `8`：继续表示 current scan + reverse op apply
- stage `9`：表示最终结果行落表
- stage `9` 百分比口径不变：
  - 分母：最终结果总行数
  - 分子：已写入结果表的行数

## 首轮非目标

- 本轮不实现真正的 background worker 并行写入
- 本轮不实现分区级动态调度
- 本轮不恢复 `TEMP TABLE` 兼容语义

## 风险

- 结果表从 `TEMP` 改为 `UNLOGGED` 是明确的用户语义变化
- 需要补充结果表生命周期和清理策略文档
- 后续真并行时仍要处理：
  - keyed / bag 分区一致性
  - worker 错误传播
  - shared result table 的并发写入与进度汇总
