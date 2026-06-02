# 反向操作流 / Reverse Op Stream

[中文](#中文) | [English](#english)

## 中文

`pg_flashback` 当前对用户暴露的是“历史结果集查询”，但实现中真正把 replay 和 apply 连起来的中间层，不是整张历史表快照，而是反向操作流。

相关代码：

- `include/fb_reverse_ops.h`
- `src/fb_replay.c`
- `src/fb_reverse_ops.c`
- `src/fb_apply.c`
- `src/fb_apply_keyed.c`
- `src/fb_apply_bag.c`

## 一、为什么需要这一层

当前主链前半段是页级重放，后半段是按逻辑语义恢复结果集。

页级重放擅长回答：

- 某个 block 在某条 LSN 后是什么样
- 某个 tuple 在更新前后页内长什么样

但 apply 需要的是：

- 针对当前行，需要做删除、补回还是替换
- 针对无主键表，需要把哪一类重复行的计数加减多少

所以必须把“物理页变化”提升成“逻辑行变化”。

## 二、当前数据结构

定义位于 `include/fb_reverse_ops.h`。

### `FbRowImage`

字段：

- `HeapTuple tuple`
- `bool finalized`

含义：

- `tuple` 是当前持有的完整行像
- `finalized` 表示该 tuple 是否已经过 TOAST inline 重写等最终整理

### `FbForwardOp`

字段：

- `type`
- `xid`
- `commit_ts`
- `commit_lsn`
- `record_lsn`
- `old_row`
- `new_row`

它表示从 WAL 看见的正向逻辑变化。

### `FbReverseOp`

字段与 `FbForwardOp` 基本一致，只是 `type` 换成了反向语义：

- `FB_REVERSE_REMOVE`
- `FB_REVERSE_ADD`
- `FB_REVERSE_REPLACE`

这是 apply 层真正消费的结构。

## 三、ForwardOp 是怎么来的

当前代码并没有单独维护一个“长期存活的 ForwardOp 数组”，而是在 replay 阶段边构造边转入 reverse source。

主路径在：

- `src/fb_replay.c`

典型入口：

- `fb_replay_build_reverse_source()`

几个关键 append helper 逻辑是：

- insert 后构造“这行在 target_ts 之后被插入了”
- delete 前构造“这行在 target_ts 之后被删除了”
- update 前后都抓，构造“这行在 target_ts 之后被替换了”

这里的 old/new row 不来自 SQL 层扫描，而来自 replay 后页面中的 tuple 拷贝。

## 四、从 ForwardOp 到 ReverseOp 的转换

逻辑上转换规则很简单：

### 1. 正向插入

```text
Forward:  INSERT new_row
Reverse:  REMOVE new_row
```

含义：

- 如果一行是在 `target_ts` 之后才插入的，那么在历史结果里应该把它删掉

### 2. 正向删除

```text
Forward:  DELETE old_row
Reverse:  ADD old_row
```

含义：

- 如果一行是在 `target_ts` 之后被删除的，那么历史结果里应该补回它

### 3. 正向更新

```text
Forward:  UPDATE old_row -> new_row
Reverse:  REPLACE new_row WITH old_row
```

含义：

- 当前表里看见的是更新后的新值
- 历史结果里应恢复成旧值

## 五、为什么顺序必须反过来

对 `(target_ts, query_now_ts]` 时间窗内的变化来说，恢复历史结果本质上是在“倒带”。

因此 apply 必须按从新到旧的顺序消费 reverse-op。

当前排序规则在 `src/fb_reverse_ops.c` 的 `fb_reverse_op_cmp()`：

1. `commit_lsn` 大的先处理
2. 同一事务内 `record_lsn` 大的先处理

这等价于：

- 事务按提交先后从新到旧
- 单事务内部按原记录顺序逆序

这条规则非常关键。
如果顺序错了，update 链、同 key 多次变更、bag delta 抵消都会错。

## 六、当前 reverse source 不是“永远纯内存”

`FbReverseOpSource` 定义在 `src/fb_reverse_ops.c`，它是当前 bounded spill 进入主链后的关键边界。

它的两种工作方式：

### 1. 纯内存模式

当累计 `row_bytes + array_bytes` 没超过当前 run limit 时：

- reverse-op 保存在 `source->ops`
- finish 时直接原地排序

### 2. spill runs 模式

当累计内存超过 `run_limit_bytes` 且有 `FbSpoolSession` 时：

- 当前批次 reverse-op 排序
- 序列化到一个 spool log
- 形成一个 run
- 释放对应 row image 与数组内存

后续 reader 会像读外排序结果一样做 merge-read。

当前用户面上，这条 spill 路径是否允许继续，额外受
`pg_flashback.spill_mode` 约束：

- `disk`：允许继续走 run spill
- `auto` / `memory`：preflight 若判断超出 `pg_flashback.memory_limit`，会在进入 replay/reverse-op 前提前报错

这部分代码在：

- `fb_reverse_run_append()`
- `fb_reverse_reader_open()`
- `fb_reverse_reader_next()`

## 七、当前 row image 为什么只保留 old/new，不再显式保留 key

早期设计稿里常见 `old_key/new_key` 字段，但当前正式头文件已经没有这组字段。

原因是：

1. apply 身份构造已经下沉到 apply 层
2. keyed / bag 的身份规则不同
3. 当前实现更倾向于保留完整 old/new row，再由消费侧按 `TupleDesc` 生成 key identity 或 row identity

对应代码：

- `src/fb_apply.c`
- `src/fb_apply_keyed.c`
- `src/fb_apply_bag.c`

这也是为什么当前 reverse-op 更像“逻辑行变化记录”，而不是“已经绑定特定 apply 语义的最终指令”。

## 八、TOAST 在这一层怎么处理

reverse-op 层本身不主动重建 TOAST。
TOAST 的真实处理点在 replay 阶段构造 row image 时：

- `src/fb_replay.c`
- `src/fb_toast.c`

当前原则：

- 先让 TOAST relation 自己进入 replay
- 再在主表 row image 生成时，用历史 TOAST store 把 external datum inline 化

因此进入 `FbReverseOp` 的 row image，目标是尽量已经“可直接消费”，而不是把 external pointer 留到 apply 再处理。

## 九、apply 如何消费 reverse-op

### keyed

`fb_keyed_apply_begin()` 会先读取全部 reverse-op，收敛成：

```text
changed key -> replacement / residual action
```

随后在扫描当前表时：

- key 未命中变化集，当前行直接保留
- key 命中变化集，按 reverse-op 抵消或替换
- 扫描结束后补 residual 历史行

### bag

`fb_bag_apply_begin()` 会把 reverse-op 收敛成：

```text
row_identity -> delta
```

随后：

- 当前扫描阶段优先抵消 delta
- residual 阶段把剩余正 delta 补出来

## 十、这一层当前解决了什么问题

1. 把物理页 replay 与逻辑查询恢复解耦
2. 让有键表和无键表复用同一套前置链路
3. 让 undo 导出和历史查询未来共用同一中间表示
4. 给 bounded spill 提供统一承载边界

## 十一、这一层当前还没完成什么

1. `fb_export_undo()` 还没有真正消费 reverse-op source
2. 主键变化场景还在继续补正确性
3. 大窗口下 reverse source 已能 spill，但更前面的 WAL 索引 / replay 主链还没完全 spill 化

## 十二、最短心智模型

如果你只想先抓住这层的本质，可以记一句：

```text
replay 负责把页变回去，reverse-op 负责把“怎么改行”表达清楚，apply 负责把这些变化真正映射到当前结果集
```

## English

This document explains the reverse-op layer that bridges physical page replay
and logical result reconstruction.

Key points:

- Replay can recover page state, but apply needs row-level actions.
- `FbForwardOp` represents the logical change extracted from replayed WAL.
- `FbReverseOp` converts that change into the inverse action consumed by apply.
- The reverse-op source preserves ordering and row images so apply can restore
  the historical result correctly.
- Keyed and bag modes consume the same reverse-op stream with different
  semantics.

The shortest mental model is: replay rebuilds pages, reverse-op describes row
changes, and apply maps those changes onto the final result set.
