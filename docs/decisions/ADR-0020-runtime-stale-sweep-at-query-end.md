# ADR-0020 查询结束时恢复 `runtime/` 安全清理

## 状态

已接受

## 背景

ADR-0018 取消了 `DataDir/pg_flashback/*` 的通用自动 cleanup，固定为：

- 不再保留 retention GUC
- 不再在 runtime 初始化时扫描目录
- 不再在查询结束时做通用删除
- `runtime/`、`recovered_wal/`、`meta/` 只承载产物

这让现场保留和排障更直接，但当前 `runtime/` 下的 query-owned 产物会持续积累，尤其包括：

- `fbspill-<pid>-<serial>/`
- `toast-retired-<pid>-<serial>.bin`

这些文件当前不具备跨查询缓存复用语义，只在拥有它们的查询生命周期内被同一查询或其 worker 使用。长期保留只会推高磁盘占用。

## 决策

恢复一条更窄的自动清理语义：

- 每次 flashback 查询结束时，扫描整个 `DataDir/pg_flashback/runtime`
- 仅处理 query-owned runtime 产物：
  - `fbspill-<pid>-<serial>/`
  - `toast-retired-<pid>-<serial>.bin`
- 若文件名中的 owner backend `pid` 已失活，则删除对应目录或文件
- 若 owner 仍活跃，直接跳过
- 若名称不匹配、解析失败、状态无法确定，也直接跳过
- 不恢复 retention GUC
- 不恢复 runtime 初始化时的目录 cleanup
- 不触碰 `recovered_wal`、`meta`、`meta/summary`

## 后果

优点：

- 运行期 query-owned spill 文件不再无限积累
- 并发查询仍安全，活跃 owner 的产物不会被误删
- 不影响 `recovered_wal` / `meta` 的缓存与排障保留语义

代价：

- 查询结束路径重新引入一次 `runtime/` 目录扫描
- 若 owner pid 被系统复用为无关进程，清理会保守跳过，可能留下少量残留
- 需要补并发与异常路径回归，确保不误删活跃查询产物

## 语义要求

- 自动清理只在查询结束路径触发，不在查询开始或 runtime 初始化时触发
- 清理逻辑必须“保守删除”：
  - 只有明确可判定 owner backend 已失活时才删除
  - 其余情况全部保留
- 目录遍历和删除中的 `ENOENT` 视作并发竞争下的可接受结果，不应报错打断查询
- 本 ADR 只回摆 `runtime/` 的安全 sweep，不推翻 ADR-0018 对其它目录的总体口径
