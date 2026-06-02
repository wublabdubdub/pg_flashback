# 2026-03-28 内存预算与磁盘回退选择设计

## 背景

当前 `pg_flashback()` 即使把 `pg_flashback.memory_limit` 调大，仍然可能出现用户感知上的“还是会写文件”：

1. query 生命周期一开始就会创建 `fb_spool` session 目录；
2. `WAL record index` 当前就依赖 spool log；
3. `FbReverseOpSource` 超过 run limit 后会把 reverse-op 排序写成 spill runs；
4. stage 8 的 apply 只是消费 `ReverseOpSource`，并不是首次决定是否落盘的地方。

因此，现状里“内存预算”和“是否允许/是否选择走磁盘”并没有被清楚地区分出来，用户也无法在执行前得到足够明确的提示。

## 问题陈述

本轮要解决两个用户面问题：

1. 用户希望只保留一个内存预算参数，不再引入第二个“内存阈值”参数；
2. 当 flashback 预计超出当前内存预算时，系统需要在真正进入高成本 replay/apply 前给出明确选择：
   - 继续坚持纯内存；
   - 允许走磁盘 spill；
   - 或者调大 `pg_flashback.memory_limit`。

## 目标

- 保留单一内存预算参数：`pg_flashback.memory_limit`
- 新增一个非内存策略参数：`pg_flashback.spill_mode`
- 在 `WAL scan` 结束后、进入重放主链前做一次 preflight working-set estimate
- 当 estimate 明显超过 `memory_limit` 时，不再静默进入后续阶段
- 给用户明确报错与可执行建议

## 非目标

- 不承诺本轮把 `WAL index` / `replay` / `apply` 全链条都改成完全不落盘
- 不尝试在查询开始前给出 100% 精确的总大小
- 不引入第二个大小阈值参数
- 不新增新的 SQL 主入口

## 用户接口

### 1. 保留单一内存预算

- `pg_flashback.memory_limit`

语义保持：

- 这是用户愿意分配给 `pg_flashback` 的唯一内存预算
- 既用于当前 hot-path memory accounting，也用于 preflight 估算决策

### 2. 新增执行策略参数

- `pg_flashback.spill_mode = auto | memory | disk`

语义：

- `auto`
  - 默认值
  - 当 estimate 没超过 `memory_limit` 时，继续执行
  - 当 estimate 超过 `memory_limit` 时，直接报错，让用户显式选择
- `memory`
  - 强制要求按当前预算走纯内存
  - 若 preflight estimate 超预算，直接报错
  - 若 preflight 低估、执行期仍超预算，也继续按当前 memory-limit error 报错
- `disk`
  - 允许继续使用现有 spool / spill 路径
  - 即使 preflight estimate 超出 `memory_limit`，也允许继续

这里刻意不引入第二个“2GB 阈值”参数。
如果用户希望“估算值超过 2GB 就先停下来”，直接把 `pg_flashback.memory_limit` 设为 `2GB` 即可。

## 预估时机

preflight estimate 放在：

- `fb_wal_build_record_index()` 完成之后
- `fb_replay_build_reverse_source()` 之前

原因：

- 这个时点已经知道：
  - 目标 relation
  - 候选 WAL 记录数量
  - record payload/spool log 的规模
  - relation tuple width / keying mode
- 但还没真正进入 replay / reverse-op / apply 高成本阶段

## 估算口径

本轮估算值是“执行前的保守近似值”，不是精确值。

### 估算来源

组合以下已知量：

- `RecordRef` 总数
- `record_log` / `record_tail_log` 的 spool 大小
- 目标 relation 的平均 tuple 宽度
- replay/reverse-op 的候选操作数量
- keyed/bag apply 需要维护的工作集近似上界

### 估算原则

- 优先宁可偏大，不可明显偏小
- 对 TOAST / old-new row image 做额外放大系数
- 对 keyed / bag 模式分别使用不同估算公式

### 局限性

无法在执行前完全精确知道：

- old/new row image 最终会有多少条
- TOAST inline 后的实际字节数
- replay 过程中会不会出现额外 retired chunk / row-image 放大

因此该 estimate 只作为“是否需要用户显式选择”的前置判断，而不是最终资源保证。

## 执行行为

### 情况 A：`estimate <= memory_limit`

无论 `spill_mode` 是什么，都继续执行。

### 情况 B：`estimate > memory_limit` 且 `spill_mode = auto`

直接报错，不进入 replay 主链。

错误模型：

- `ERROR: estimated flashback working set exceeds pg_flashback.memory_limit`
- `DETAIL: estimated=... limit=... mode=auto phase=preflight`
- `HINT: Increase pg_flashback.memory_limit, or set pg_flashback.spill_mode = 'disk' to allow spill.`

### 情况 C：`estimate > memory_limit` 且 `spill_mode = memory`

直接报错，不进入 replay 主链。

错误模型：

- `ERROR: flashback is configured to run in memory-only mode, but the estimated working set exceeds pg_flashback.memory_limit`
- `DETAIL: estimated=... limit=... mode=memory phase=preflight`
- `HINT: Increase pg_flashback.memory_limit, or set pg_flashback.spill_mode = 'disk'.`

### 情况 D：`estimate > memory_limit` 且 `spill_mode = disk`

允许继续。

此时：

- preflight 会输出一条明确 `NOTICE` 或 debug summary，说明估算已超出内存预算、将允许后续 spill
- 真正是否发生 reverse-op spill，仍由现有 `FbReverseOpSource` / spool 逻辑决定

## 当前落盘行为的用户解释

为了避免继续把 stage 8 误解成“开始落盘”的地方，本轮需要统一文档与诊断口径：

- `fb_spool` session 目录并不等于“已经发生大规模 spill”
- `WAL index` 当前天然就依赖 spool log
- reverse-op spill 发生在 reverse source 构建阶段，而不是 apply 阶段
- apply 阶段只是读取 reverse-op source

## 架构改动点

### `fb_guc`

新增：

- `pg_flashback.spill_mode`

职责：

- 定义枚举型 GUC
- 暴露 getter 供 entry/replay 主链读取

### `fb_entry`

新增 preflight orchestration：

- `WAL scan` 完成后，构造 working-set estimate
- 根据 `memory_limit + spill_mode` 决定：
  - 继续
  - 提前报错
  - 或允许后续 spill

### `fb_reverse_ops`

不重写已有 spill 机制，但需要：

- 暴露更清晰的 debug summary / stats
- 让 preflight 能复用现有 run-limit 计算口径

### 文档/错误模型

更新：

- README
- architecture docs
- STATUS/TODO
- regression expected outputs

## 测试策略

### 回归 1：GUC 表面

- `spill_mode` 默认值、合法值、非法值

### 回归 2：preflight 拦截

- `memory_limit` 很小
- `spill_mode=auto`
- 预期在 replay 前直接报 preflight error

### 回归 3：memory-only 拦截

- `spill_mode=memory`
- 预期同样在 preflight 阶段报错

### 回归 4：disk 允许继续

- `spill_mode=disk`
- 相同 workload 下允许继续，并最终成功返回结果

### 回归 5：旧错误仍保留

- 即使 preflight 低估，执行期真实超内存时，仍保留现有 memory-limit exceeded error

## 决策总结

本轮拍板如下：

- 只保留一个内存预算参数：`pg_flashback.memory_limit`
- 不新增第二个大小阈值参数
- 新增非内存策略参数：`pg_flashback.spill_mode = auto|memory|disk`
- preflight estimate 放在 `WAL scan` 之后、`replay/reverse-op` 之前
- estimate 超预算时，默认不自动继续，而是要求用户显式选择
