# 行身份与无主键语义 / Row Identity And Keyless Semantics

[中文](#中文) | [English](#english)

## 中文

`pg_flashback` 当前支持两类结果语义：

- keyed
- bag

这不是“优化开关”，而是正确性模型。
系统必须先判断目标表能不能稳定识别“同一逻辑行”，再决定 apply 该怎么做。

相关代码：

- `src/fb_catalog.c`
- `include/fb_common.h`
- `src/fb_apply.c`
- `src/fb_apply_keyed.c`
- `src/fb_apply_bag.c`

## 一、选择发生在什么地方

入口并不是在 apply 阶段才临时决定模式，而是在 relation gate 就写进 `FbRelationInfo`。

代码：

- `src/fb_catalog.c`

关键函数：

- `fb_catalog_find_stable_unique_key()`
- `fb_catalog_choose_mode()`
- `fb_catalog_load_relation_info()`

最终结果保存在：

- `FbRelationInfo.mode`
- `FbRelationInfo.mode_name`
- `FbRelationInfo.key_natts`
- `FbRelationInfo.key_attnums`

## 二、什么是 keyed

### 适用条件

当前代码把以下索引视为“可接受的稳定键”：

1. 主键
2. 或满足全部条件的唯一索引：
   - `indisunique`
   - `indimmediate`
   - 没有 predicate
   - 没有 expression

也就是：

- 延迟唯一约束不算
- 部分唯一索引不算
- 表达式唯一索引不算

原因是当前 apply 需要一个简单、稳定、能直接从 tuple 中取值的逻辑身份。

### keyed 的目标

对于有键表，目标是：

- 精确恢复逻辑行
- 当前结果集中每个 key 最终只出现它在 `target_ts` 时应该有的版本

### keyed 当前怎么构建身份

apply 层不会直接拿 tuple pointer 或 ctid 当身份，而是按 key 列生成 identity 字符串，必要时再辅以 typed fast path。

相关函数：

- `fb_apply_build_key_identity()`
- `fb_apply_build_key_identity_slot()`

默认 identity 文本形态类似：

```text
id='42'|tenant='cn'
```

这层设计的好处是：

- 跨 replay / current scan 两端都能统一表达
- 调试可读性强

坏处是：

- 纯文本 identity 有分配和格式化成本

所以 keyed apply 又继续引入了 typed key fast path。

## 三、keyed apply 当前的内部表示

`src/fb_apply_keyed.c` 中，核心状态是 `FbKeyedApplyState`。

关键字段：

- `buckets`
- `entries_head`
- `residual_cursor`
- `tracked_bytes`
- `memory_limit_bytes`
- `use_typed_keys`
- `key_meta`

单个 key 对应 `FbKeyedEntry`，记录：

- `key_identity`
- key 值和 null 标记
- `current_seen`
- `replacement_tuple`

### keyed 的语义动作

对当前表扫描来说，某个 key 会落到三种典型结果：

1. 当前行没被 reverse-op 命中
   当前行直接输出

2. 当前行命中 reverse-op，需要替换
   输出历史 replacement tuple

3. 当前扫描结束后，某些历史 key 当前已不存在
   在 residual 阶段补发

也就是说，keyed 不是“重算整张历史表”，而是“只维护变化 key 的动作表”。

## 四、什么是 bag

### 适用条件

当 relation 没有可接受的稳定唯一键时，系统回退到 `FB_APPLY_BAG`。

这不是失败，而是另一种明确语义：

- 按 multiset / bag 恢复结果集
- 保证结果内容与重复次数正确
- 不保证同值重复行的物理身份

### bag 解决的真实问题

例如表中有两行完全相同：

```text
(a=1, b='x')
(a=1, b='x')
```

没有主键时，我们无法稳定回答“被 delete 的到底是哪一条物理行”。
但对于用户关心的历史查询，往往更重要的是：

- 历史结果里这种值出现几次

bag 语义就是为此服务的。

## 五、bag 当前怎么构建身份

bag 模式下，identity 覆盖整行所有非 dropped 列。

相关函数：

- `fb_apply_build_row_identity()`
- `fb_apply_build_row_identity_slot()`

文本形式类似：

```text
id='42'|name='alice'|note=NULL|payload='...'
```

不过 bag 内部同样尽量走 typed identity fast path，不只是纯字符串比较。

## 六、bag apply 当前的内部表示

`src/fb_apply_bag.c` 中，核心状态是 `FbBagApplyState`。

关键字段：

- `buckets`
- `entries_head`
- `residual_cursor`
- `residual_repeat`
- `tracked_bytes`
- `memory_limit_bytes`
- `use_typed_identity`
- `attr_meta`

单个 entry 对应：

- `row_identity`
- `values/nulls`
- `delta`
- `tuple`

这里的 `delta` 是核心：

- 负 delta：当前扫描时要抵消
- 正 delta：残留到 residual 阶段补发

## 七、keyed 与 bag 的本质差异

### keyed

关注的是：

- “这个 key 在历史时刻对应哪一行”

适合：

- 主键表
- 稳定唯一键表

### bag

关注的是：

- “这种完整行值在历史时刻应该出现几次”

适合：

- 无主键表
- 无稳定唯一键表

## 八、为什么当前不使用 `ctid` / 物理 tuple 身份

因为 flashback 目标是逻辑结果恢复，不是物理页时间旅行。

`ctid` 不稳定的原因包括：

- HOT update
- 跨页 update
- vacuum / page prune 影响物理布局
- 不同时间点同一逻辑行可能换了物理位置

因此当前实现只把页级重放当成“恢复 row image 的手段”，不会把物理位置暴露成 apply 的最终身份。

## 九、当前实现对主键变化的状态

这是当前仍未完全闭合的边界之一。

现状：

- keyed 主链已经可用
- 但“主键更新后 old_key/new_key 如何在所有场景稳定收敛”仍在继续补正确性

这也是为什么当前文档必须区分：

- “keyed 模式已实现”
- “keyed 在主键变化极端场景已完全闭合”

前者成立，后者当前还不能轻易宣称。

## 十、当前性能上的取舍

### 已经避免的问题

1. keyed 不再按当前整表建全量 hash
2. bag 不再把当前整表全部收进 multiset

### 仍然存在的成本

1. identity 构造与比较
2. residual tuple 持有
3. 宽表 / TOAST-heavy 场景下的行像内存

所以当前 apply 的内存已经不再线性依赖“当前表大小”，但仍然会受“变化集规模”和“单行宽度”影响。

## 十一、如何判断一张表当前会走哪种模式

最简单的方法：

```sql
SELECT fb_check_relation('public.t1'::regclass);
```

当前返回类似：

```text
mode=keyed has_toast=true
```

或者：

```text
mode=bag has_toast=false
```

## 十二、最短结论

- 有主键或稳定唯一键：按 keyed 精确恢复逻辑行
- 没有稳定键：按 bag 恢复结果集内容和重复次数
- 当前 apply 的核心优化，不是“更聪明地扫当前表”，而是“只跟踪变化 key / 变化 row identity”

## English

This document explains how `pg_flashback` chooses between keyed and bag
semantics.

Key points:

- Mode selection is done during relation inspection and stored in
  `FbRelationInfo`.
- Tables with a primary key or a qualifying stable unique index use `keyed`
  mode.
- Tables without a stable key use `bag` mode.
- `keyed` mode reconstructs exact logical rows by changed keys.
- `bag` mode restores row content and duplicate counts without preserving
  physical identity.
- The main optimization is to track only changed identities instead of copying
  the full current table into memory.
