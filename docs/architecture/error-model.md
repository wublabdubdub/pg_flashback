# 错误模型 / Error Model

[中文](#中文) | [English](#english)

## 中文

本文描述当前代码对“不能安全返回历史结果”的处理原则。这里说的“错误模型”不是泛泛而谈，而是对应当前主链里哪些条件会在什么层直接报错，以及为什么不能降级。

相关代码：

- `src/fb_entry.c`
- `src/fb_catalog.c`
- `src/fb_guc.c`
- `src/fb_wal.c`
- `src/fb_replay.c`
- `src/fb_toast.c`

## 总原则

`pg_flashback` 当前是严格失败模型，不做 best-effort。

只要扩展无法证明“当前返回的就是 `target_ts` 时刻的正确逻辑结果”，就必须直接报错。实现上主要有三类：

1. 入口参数或目标 relation 本身不合法
2. WAL 时间窗不完整，或时间窗内命中已知不支持边界
3. 页级重放、TOAST 重建、apply 发射过程中发现结果已不可信

禁止行为：

- 返回部分历史结果
- 静默跳过缺失 WAL / 缺失页基线
- 把不支持边界降级成“尽量查”
- 发现 TOAST 缺 chunk 时偷偷回退到错误值

## 一、入口与 relation gate 直接失败

### 1. 目标时间晚于当前时间

入口函数 `pg_flashback()` 与 `fb_export_undo()` 都先调用 `fb_require_target_ts_not_future()`。

代码：

- `src/fb_entry.c`

行为：

- 若 `target_ts > GetCurrentTimestamp()`，直接报 `target timestamp is in the future`

### 2. 目标参数不是表行类型

`pg_flashback(anyelement, text)` 依赖第一个参数的复合类型 Oid 反推出 relation。

代码：

- `src/fb_entry.c` 中的 `fb_resolve_target_type_relid()`

行为：

- 无法解析参数类型时直接报错
- 类型不是表 row type 时直接报 `pg_flashback target must be a table row type`

这也是当前必须写成：

```sql
SELECT *
FROM pg_flashback(NULL::public.t1, '2026-03-28 10:00:00+08');
```

### 3. relation 类型不支持

`fb_catalog_load_relation_info()` 会调用 `fb_catalog_require_supported_relation()`。

代码：

- `src/fb_catalog.c`

当前直接拒绝：

- temporary relation
- unlogged relation
- materialized view
- partitioned table parent
- system catalog
- 非普通 heap relation

原因很直接：当前 replay / apply / TOAST 路径只按普通持久化 heap 表设计。

## 二、运行时与来源解析直接失败

### 1. 没有可用的 archive 配置

入口层会调用 `fb_require_archive_dir()`，它本质上要求“当前至少能解析出一个有效 archive 目录”。

代码：

- `src/fb_entry.c`
- `src/fb_guc.c`

失败场景：

- `pg_flashback.archive_dest` 未设置
- 兼容项 `pg_flashback.archive_dir` 未设置
- PostgreSQL `archive_command` 为空，或虽不为空但不是当前可安全识别的本地命令

这里不会去猜复杂 shell 逻辑，也不会去推断远程归档。

### 2. 目录存在但没有 WAL 段

WAL 准备阶段会进一步检查解析出的来源目录里是否存在段文件。

代码：

- `src/fb_wal.c`

对应错误属于“运行时前置条件不满足”，而不是 replay 阶段错误。

## 三、WAL 时间窗直接失败

### 1. 找不到 `target_ts` 前最近 checkpoint

WAL 准备阶段必须先找到全局锚点。

代码：

- `src/fb_wal.c`

当前页级重放主链固定依赖：

- `checkpoint_lsn`
- `redo_lsn`

如果没有 checkpoint 锚点，后续 `RecordRef + block redo` 不成立，直接失败。

### 2. WAL 不完整

这是最重要的硬错误之一。

代码：

- `src/fb_wal.c`
- `src/fb_ckwal.c`

当前 resolver 会综合三路来源：

- `archive_dest`
- `pg_wal`
- `DataDir/pg_flashback/recovered_wal`

当需要的 segment 三路都无法提供可信副本时：

- 先尝试 `fb_ckwal_restore_segment()`
- 仍失败则报 `WAL not complete`

这里没有“缺一点也先查”的降级路径。

### 3. 时间窗命中 storage boundary / rewrite / truncate

`fb_wal_build_record_index()` 在建立索引时会同时记录 unsafe window。

代码：

- `src/fb_wal.c`
- `src/fb_entry.c` 中的 `fb_build_unsafe_detail()`

当前 unsafe 分类见 `include/fb_wal.h`：

- `FB_WAL_UNSAFE_TRUNCATE`
- `FB_WAL_UNSAFE_REWRITE`
- `FB_WAL_UNSAFE_STORAGE_CHANGE`

入口层在拿到 `FbWalRecordIndex` 后，如果 `index.unsafe = true`，会直接报：

```text
fb does not support WAL windows containing ...
```

并带 detail：

- scope：`main` / `toast`
- operation：如 `smgr truncate`
- target / toast relation 名
- xid
- commit_ts
- lsn

### 4. 当前对主表与 TOAST 的边界不同

这是当前实现里需要特别说明的地方。

主表仍是严格 storage boundary：

- `TRUNCATE`
- rewrite
- create/recreate 等 storage change

TOAST relation 当前已经放宽一层：

- `TOAST SMGR TRUNCATE` 不再在 relation 级直接拒绝
- 允许继续 replay
- 最终若历史 chunk 真缺失，再在 TOAST 重建阶段报错

这部分是当前代码现状，不是未来设计稿。

## 四、事务状态无法判定时直接失败

`RecordRef` 只对“提交状态明确”的记录做最终判定。

代码：

- `src/fb_wal.c`
- `include/fb_wal.h`

`FbRecordRef` 上有这些状态位：

- `committed_after_target`
- `committed_before_target`
- `aborted`

如果某条关键记录的事务状态无法归类，结果就不再可信，必须报错；不能把它假定为已提交或已回滚。

## 五、页级重放直接失败

### 1. 缺页基线且回补失败

当前 replay 主链严格依赖：

- checkpoint
- FPI / INIT_PAGE
- block redo

代码：

- `src/fb_replay.c`

若 discover / warm / final 三阶段之后，某 block 仍找不到可恢复基线，就会触发 `missing FPI` 类错误。

这里当前也不允许：

- 按 block 无上限地回扫
- 忽略该 block 继续返回其他行

### 2. heap redo 前置条件不满足

例如：

- insert 缺 block data
- update 缺 new tuple data
- `old_offnum` / `new_offnum` 非法
- 目标 tuple 在页内不存在

代码：

- `src/fb_replay.c`

这些错误都属于“物理页状态与 WAL 不一致”，继续执行只会得到错误行像，因此直接失败。

### 3. 不能跳过 aborted record 的物理影响

当前代码已经修正为 replay 主循环不跳过 `record.aborted` 的物理页影响。

原因：

- 回滚事务虽然逻辑上不应进入最终结果
- 但它仍可能占过页槽位、影响后续 offset / page shape

如果简单跳过，会造成后续 redo 在错误页态上执行。

## 六、TOAST 重建直接失败

### 1. 历史 TOAST chunk 缺失

代码：

- `src/fb_toast.c` 中的 `fb_toast_reconstruct_datum()`

当前策略：

- 先从历史 TOAST store 查 chunk
- 对“目标时刻后未变化、但历史 store 只覆盖部分块”的 datum，可尝试回退读取 live TOAST
- 如果 live 也无法解析，就直接报错

不会做的事：

- 返回残缺值
- 用空串或截断值代替
- 静默忽略缺 chunk

### 2. chunk payload 不可读或越界

例如：

- spill 文件中 chunk 读不回来
- chunk_len 超过剩余 ext_size

这类情况同样直接失败，因为它意味着 TOAST 历史值已经无法可靠重建。

## 七、apply / 结果发射直接失败

### 1. SRF 上下文不满足

代码：

- `src/fb_entry.c`

当前 `pg_flashback()` 要求：

- 在 set-returning context 中调用
- 执行器允许 `SFRM_ValuePerCall`，或者允许 `SFRM_Materialize`

否则直接报错，而不是偷偷改用别的结果模型。

### 2. 发射出的行类型非法

代码：

- `src/fb_apply.c`

`FbApplyEmit` 只允许：

- `FB_APPLY_EMIT_SLOT`
- `FB_APPLY_EMIT_TUPLE`

其余状态直接 `elog(ERROR)`。这类错误通常代表 apply 内部状态机出错，而不是用户输入问题。

## 八、当前明确允许的“非 fatal”情况

不是所有异常 record 都会在扫描阶段直接拦截。当前几个重要的“允许继续”边界：

1. standalone `standby_lock`
   只有 `AccessExclusiveLock` 语义且没有更具体 storage change 时，不再单独阻塞 flashback。

2. TOAST `SMGR TRUNCATE`
   允许进入 replay，最终缺 chunk 时再报。

3. `pg_wal` 中名字与真实页头不匹配的 segment
   不直接信任，但会先转入 `recovered_wal` 再回灌 resolver；只有恢复失败才报错。

## 九、读错误时的定位顺序

可按错误信息的来源快速判断层级：

1. 参数 / row type / SRF context：先看 `src/fb_entry.c`
2. 不支持 relation：看 `src/fb_catalog.c`
3. archive / autodiscovery / GUC：看 `src/fb_guc.c`
4. WAL not complete / unsafe window：看 `src/fb_wal.c`、`src/fb_ckwal.c`
5. missing FPI / heap redo：看 `src/fb_replay.c`
6. toast chunk 缺失：看 `src/fb_toast.c`

## 结论

当前错误模型的核心不是“尽量多返回”，而是“宁可失败，也不能错”。
只要无法证明结果正确，当前代码就会在最接近问题发生处直接报错，并尽量把 relation、scope、xid、commit time、lsn 这些诊断信息带出来。

## English

This document explains how `pg_flashback` behaves when it cannot safely prove a
correct historical result.

Key rules:

- The extension uses a strict fail-fast model rather than best-effort output.
- It errors for invalid targets, unsupported relations, future timestamps, and
  missing runtime prerequisites.
- It rejects incomplete WAL windows and unsupported boundaries such as DDL,
  rewrite, truncate, relfilenode changes, or unsafe storage events.
- It also fails when page replay, TOAST reconstruction, or apply/result
  emission cannot preserve correctness.

The core principle is simple: if correctness cannot be proved, the query must
fail instead of returning a partial or possibly wrong result.
