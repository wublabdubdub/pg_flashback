# ADR-0004: WAL 来源采用 archive_dest + pg_wal 双来源解析

## 状态

Accepted

## 决策

`pg_flashback` 的最终 WAL 来源模型定为：

- 主配置语义：`archive_dest`
- 运行时同时解析两个来源：
  - `pg_wal`
  - `archive_dest`

并按 segment 粒度选择来源。

## 来源选择规则

对每个所需 segment：

1. 若仅存在于 `pg_wal`，从 `pg_wal` 读取
2. 若仅存在于 `archive_dest`，从 `archive_dest` 读取
3. 若两端都存在：
   - 一律优先 `archive_dest`
4. 若两端都不存在：
   - 进入“缺失 WAL 恢复”流程
   - 恢复失败则报 `WAL not complete`

当前已落地的最小接口层：

- `pg_flashback.debug_pg_wal_dir`（开发期专用）
- 扩展内部 `DataDir/pg_flashback/recovered_wal/` 恢复目录

## segment 分层

- `pg_wal`
  - 视为 recent tail 补充来源
  - 允许 partial segment
  - 只有 `archive_dest` 缺失时才参与
- `archive_dest`
  - 视为历史归档池
  - 期望是长期保留、只读、稳定文件
  - 一旦已归档，就优先承接该 segment

## 被覆盖 WAL 的恢复

当所需 segment：

- 已不在 `pg_wal`
- `archive_dest` 中也不存在或损坏

则进入恢复层。该层后续参考：

- `/root/xman` 中的 `ckwal`

但约束固定为：

- 仅参考“缺失 segment 复原”的思路
- 不把 `pg_flashback` 变成外部工具驱动模型
- 恢复产物最终仍回到扩展自己的 WAL 来源解析层

## `pg_wal` 文件可信度判断

对来自 `pg_wal` 的 segment，要求校验：

- 文件名中的 `timeline + segno`
- first long page header 的 `xlp_seg_size`
- first long page header 的 `xlp_pageaddr`

若文件名与 page header 指向的真实 segment 不一致，则视为 recycled / 覆盖 / 错配文件，不能直接参与 flashback，必须进入 `ckwal` 恢复路径。

## 当前基线实现的定位

当前 `pg_flashback.archive_dir` 只代表：

- PG18 基线开发配置
- 单目录 WAL 来源

它不是最终产品接口，只是过渡实现。

## 原因

- 仅依赖 `pg_wal` 无法覆盖长期时间窗
- 仅依赖 `archive_dest` 又会错失 recent/partial WAL
- 双来源解析可以让最近 WAL 和历史 WAL 各走最合适的路径
- 把缺失恢复单独建模，比继续把错误压成“目录里没文件”更可维护
