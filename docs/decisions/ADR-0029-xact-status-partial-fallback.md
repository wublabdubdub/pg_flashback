# ADR-0029 Xact-Status Partial Fallback And Coalesced WAL Windows

## 状态

已接受

## 背景

`summary v3` 已经把 `xid outcomes` 收到 segment sidecar 中，查询期也已改成
summary outcome 优先。

但当前 live case
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
确认暴露出 `3/9 xact-status` 的新放大路径：

- `fb_summary_fill_xact_statuses()` 当前仍是 all-or-nothing 语义
  - 只要还有任意 `touched/unsafe xid` 未被 summary outcome 解出
  - 整体就返回失败
- 调用方当前一旦失败，就直接回退到旧的串行 WAL xact 扫描
  - 继续使用高碎片 relation/span windows
  - 会重扫大量 `RM_XACT_ID` 记录
  - 也会重复触达已由 summary 命中的 xid

现场 `gdb` 已确认长尾热点落在：

- `fb_wal_fill_xact_statuses_serial`
- `fb_wal_visit_window`
- `WALRead`

同次现场 `window_count` 已达到 `97705`。

## 决策

保持 correctness 语义不变：

- 不允许因为 summary 覆盖不足漏掉 xid status
- 不允许因为优化而跳过 unresolved xid 的事务结论

在此前提下，把 `xact-status` 收敛为两段式：

- summary 命中阶段
  - 先把 sidecar 中已经能回答的 xid outcome 写入 query index
  - 不再把“部分命中”整体视为失败
- fallback 阶段
  - 仅对 unresolved `touched/unsafe xid` 构建 fallback 集
  - 仅在 fallback 集非空时继续读取原始 WAL
  - fallback 使用 xact-status 专用 coalesced windows
    - 不再直接复用高碎片 relation/span windows
    - 允许为减少碎读而把覆盖窗口扩成 segment-run 级读取
    - 仍只消费 `RM_XACT_ID` 记录，并继续按 unresolved xid 过滤

## 为什么不继续保留全量回退

- 当前最昂贵的不是“检查 unresolved xid 是否存在”，而是
  “发现任意 unresolved 后重新把整批窗口顺扫一遍”
- 旧路径既重复读取 WAL，也重复处理已命中的 xid
- live case 已证明这条路径会把 `3/9 xact-status` 放大到不可接受的量级

## 后果

优点：

- summary 已命中的 xid 可以直接保留收益，不再因少量缺口失效
- xact fallback 读取从“碎窗口数”收敛到“coalesced segment runs”
- 继续复用现有 summary 文件格式与 query cache，不强制新增 summary 版本

代价：

- query-side 需要维护 unresolved xid fallback 集
- `xact-status` 的调试与回归观测需要新增字段
- 若后续仍需继续压缩成本，可能还要补充更强的 per-segment xid presence/index

## 语义要求

- unresolved xid 仍必须得到确定 status，不能做 best-effort 跳过
- summary 部分命中不能导致 `target_commit_count/target_abort_count` 重复累计
- xact-status fallback 只服务事务状态补齐，不改变 payload / replay / apply 语义
- sidecar 缺失、损坏、旧版本或 recent tail 未覆盖时，仍必须继续安全回退
