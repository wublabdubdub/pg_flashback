# ADR-0017 统一 flashback 并行 worker 参数

## 状态

已接受

## 背景

当前仓库里与 flashback 主链直接相关的并行开关只有：

- `pg_flashback.parallel_segment_scan`

它只控制 WAL segment prefilter，不控制：

- resolver / sidecar
- WAL metadata scan
- WAL payload / materialize
- 后续 flashback 主链中新增的并行阶段

这会带来两个问题：

1. 用户看到“并行开关”，但它实际上只覆盖一个子阶段，语义过窄。
2. 接下来要继续把 flashback 主链其余可安全并行的阶段并进来时，原参数名已经不再准确。

## 决策

将 flashback 主链并行控制统一收口为：

- `pg_flashback.parallel_workers`

参数语义固定为：

- `0`：
  - flashback 主链强制串行
- `> 0`：
  - 允许 flashback 主链进入并行实现
  - 参数值即 flashback 主链并行 worker 上限
- `< 0`：
  - 非法

本次统一开关覆盖 flashback 主链中“在不改变语义前提下允许并行”的阶段，包括：

- resolver / sidecar
- WAL segment prefilter
- WAL metadata scan
- WAL payload / materialize
- 后续 flashback 主链中新增的并行阶段

这里的“允许并行”固定解释为：

- 该阶段在设计上允许被 `pg_flashback.parallel_workers` 接管
- 但只有在 correctness 与收益都达标后，才允许默认进入正式主路径
- 仍在调优、尚未打赢稳定串行基线的 prototype，可以保留在代码里继续优化，但不能因为共享同一个 worker 参数就默认打开

当前已验证的“默认不能打开”的例子包括：

- keyed query-side apply 并行 prototype

当前已进入正式主路径的例子包括：

- resolver / sidecar 的文件级并行
- WAL segment prefilter
- WAL payload / materialize 并行
  - 进程级 payload worker
  - shared segment snapshot
  - raw spool merge + anchor rebuild
  - 连续大窗口的 segment 细分
  - overlap read + logical emit boundary，保证跨 segment record 不丢失

当前已验证但默认不能打开的例子还包括：

- WAL metadata 两段式并行
  - Phase A：metadata worker 并行收集 checkpoint / touched_xids / unsafe
  - Phase B：leader 串行只扫 `RM_XACT_ID` 回填事务状态
  - correctness 合同可成立，但 live case 上尚未稳定打赢串行 metadata 基线

旧参数处理固定为：

- 删除 `pg_flashback.parallel_segment_scan`
- 不保留兼容别名

## 语义要求

统一开关接入后，flashback 主链即使启用并行，也必须保持以下语义不变：

- 相同的 `anchor_checkpoint_lsn / anchor_redo_lsn`
- 相同的 `unsafe` 判定
- 相同的 `touched_xids / xid_statuses`
- 相同顺序的 `RecordRef`
- 相同的 replay / reverse-source / apply 结果

也就是说，并行只允许改变实现形态，不允许改变逻辑结果。

## 后果

优点：

- 用户只需要理解一个 flashback 主链并行 worker 参数
- 后续扩展更多 flashback 阶段并行时，不必继续增加用户侧并行度参数
- 文档和调试口径可以统一为“flashback 并行度”

代价：

- 需要迁移现有依赖 `parallel_segment_scan` 文案和回归的地方
- 旧参数删除后，已有会话级配置需要改名
- 仓库内部实现仍需继续细化“哪些阶段真正已并行化”，不能因为 worker 参数统一就在文档里夸大实现范围

## 分阶段实施顺序

为了避免在并行改造过程中引入误闪回或结果重复，当前实施顺序固定为：

1. `parallel_workers=0` 仍保留串行 prefilter 基线
2. WAL payload / materialize pass 并行
3. WAL metadata 两段式并行
4. replay / reverse-source 分片并行
5. apply 通用并行

每个阶段都必须先满足以下合同，再继续下一阶段：

- 相同的 `anchor_checkpoint_lsn / anchor_redo_lsn`
- 相同的 `unsafe` 判定
- 相同的 `xid_statuses`
- 相同的 `RecordRef` 语义与顺序约束
- 相同的最终历史结果
