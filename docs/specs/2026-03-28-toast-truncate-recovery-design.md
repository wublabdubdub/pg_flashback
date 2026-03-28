# TOAST Truncate 可恢复 flashback 设计

## 背景

当前实现会把目标主表及其 TOAST relation 视为同一个 storage boundary。

因此，只要 `target_ts -> query_now_ts` 时间窗内命中：

- 主表 truncate / rewrite / storage change
- TOAST truncate / storage change

就会在 WAL 扫描阶段直接报错，不进入 replay。

这对主表 boundary 是合理的，但对 TOAST `SMGR TRUNCATE` 过于保守。真实线上场景里，这类记录通常来自：

- 主表宽列更新或删除后留下死 TOAST chunk
- 后续 `VACUUM` / autovacuum 回收 TOAST relation 尾部空页

此时主表的历史逻辑行仍然可能可恢复，因为：

- 主表历史 tuple 仍由主表 WAL/replay 决定
- 历史 TOAST 值由 `fb_toast` 的 chunk store 重建
- TOAST 页级 FPI / page image 仍可能在 truncate 前已经把所需 chunk 带入当前历史 store

当前“relation 级直接拒绝”的问题不是“必然不可恢复”，而是“没有区分主表 boundary 与 TOAST 尾页回收”。

## 目标

- 不再因为 TOAST `SMGR TRUNCATE` 直接拒绝 flashback
- 保持主表 truncate / rewrite / relfilenode 变化继续直接报错
- 保持用户结果语义不变：`SELECT * FROM pg_flashback(...)` 必须返回完整历史行，包括 TOAST 列
- 如果历史 TOAST 值最终仍无法重建，继续明确报错，而不是静默降级

## 非目标

- 不实现“跨任意 relation truncate 的完整 relation size 时态恢复”
- 不把主表 storage_change 的拒绝边界放宽
- 不引入“主表列正确但 TOAST 列可空/可跳过”的降级模式
- 不为本轮引入新的用户可见 GUC 或 SQL 入口

## 决策

本轮采用“主表 boundary 继续严格、TOAST truncate 改为延迟判定”的方案。

具体规则：

1. 主表 `truncate / rewrite / storage_change` 继续在 WAL 扫描阶段直接拒绝。
2. TOAST `SMGR TRUNCATE` 不再被视为 relation 级 fatal unsafe。
3. standalone `STANDBY LOCK` 不再被视为 storage boundary：
   - 它只表示 PostgreSQL 为 Hot Standby 记录了 `AccessExclusiveLock`
   - 若后续没有更具体的 `truncate/create/rewrite`，则不应单独阻塞 flashback
4. TOAST `STANDBY LOCK` 如果最终被同事务内的 `SMGR TRUNCATE` 具体化，则同样不再直接拒绝。
5. 真正的正确性边界改为：
   - 若 replay + TOAST store 足以重建目标历史值，则查询成功
   - 若目标历史 tuple 依赖的 TOAST chunk 最终缺失，则在 tuple finalize 阶段报错

这意味着：

- “是否见过 TOAST truncate”不再是失败条件
- “目标历史值是否真的缺 chunk”才是失败条件

## 方案原因

### 为什么不能只看主表

用户入口是完整历史结果集，不是“键列/非 TOAST 列子集”。

只要目标历史 tuple 含有 external toast pointer，就必须重建其历史值；否则：

- `SELECT *` 语义不成立
- 返回 live TOAST 或空值都会产生错误历史结果

因此不能把 TOAST 完全排除出 flashback 主链。

### 为什么 TOAST truncate 不应直接失败

当前 `fb_toast` 不是直接读取 live TOAST relation，而是维护历史 chunk store：

- `live_chunks`
- `retired_chunks`

store 的键是 `valueid + chunk_seq`，而不是“当前物理文件是否仍保留该页”。

只要在 replay 过程中，这些历史 chunk 已经由：

- TOAST DML
- TOAST block image / FPI
- 共享 warm pass 的更早页锚点

写入 store，那么后续 `SMGR TRUNCATE` 把尾页裁掉，并不必然破坏目标历史值重建。

换句话说，TOAST truncate 更像“物理回收尾部空页”，而不是“逻辑历史值必然不可恢复”。

## 实现改动

### 1. WAL unsafe 判定

`fb_wal` 当前把所有 relation 级 unsafe 都统一提升为 `ctx->unsafe`。

本轮改为：

- 主表 truncate / rewrite / storage_change：保持现状
- standalone `STANDBY LOCK`：仅记录上下文，不触发 fatal unsafe
- TOAST `SMGR TRUNCATE`：仅记录上下文，不触发 fatal unsafe
- TOAST `STANDBY LOCK`：若后续被同 xid 的 `SMGR TRUNCATE` 覆盖为具体操作，则不触发 fatal unsafe

这样可以继续完成：

- metadata 扫描
- payload capture
- replay

而不是在扫描阶段提前退出。

### 2. replay / toast 语义

`fb_replay` 与 `fb_toast` 本轮不新增“truncate 物理删页”模型。

仍沿用现有机制：

- TOAST image / tuple WAL 把历史 chunk 写入 store
- 主表 row image finalize 时按 `valueid + chunk_seq` 从 store 重建历史值

TOAST truncate 之后：

- store 中已有的 chunk 不主动清除
- 未被目标历史 tuple 引用的多余 chunk 不影响结果
- 若目标需要的 chunk 根本不在 store，则继续报错

## 错误模型

新的错误边界如下：

- 主表 boundary：继续 `feature_not_supported`
- TOAST truncate 后仍可重建：查询成功
- TOAST truncate 后确实缺 chunk：继续报 `failed to finalize toast-bearing forward row` / `missing toast chunk ...`

这符合首版“只返回正确结果，不做 silent corruption”的约束。

## 回归策略

新增/修改回归 `fb_flashback_toast_storage_boundary`：

1. 构造带大 `text` 的 TOAST-heavy 目标表
2. 记录 `target_ts`
3. 建立 target 时刻 truth
4. 将部分宽列更新为小值
5. 执行 `VACUUM` 触发 TOAST relation 尾页 truncate
6. 验证：
   - TOAST relation size 变小
   - `pg_flashback(NULL::target, target_ts)` 不再报 storage boundary 错误
   - 返回结果与 truth 完全一致

## 风险

- 当前方案默认“store 中残留的历史 chunk 不会污染目标结果”，其成立前提是 tuple 重建严格按历史 tuple 中的 `valueid + chunk_seq` 取值
- 若后续发现 TOAST `SMGR TRUNCATE` 仍存在无法由现有 store/FPI 覆盖的场景，再继续扩展到“TOAST relation size 时间线 + truncated block range 恢复”方案

## 本轮验收标准

- 用户 live 案例中的 TOAST truncate 不再在 WAL 阶段被直接拒绝
- 目标历史结果仍可正确返回
- 主表 storage boundary 相关回归不回退
- TOAST 历史 chunk 真缺失时仍明确报错
