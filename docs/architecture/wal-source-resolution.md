# WAL 来源解析

## 当前问题

当前 PG18 基线实现把：

- `pg_flashback.archive_dir`

直接当成“唯一 WAL 来源目录”。

这只适合开发验证，不适合作为最终产品模型。用户把它直接设成：

```sql
SET pg_flashback.archive_dir = '/isoTest/18pgdata/pg_wal';
```

虽然能跑通基线验证，但存在三个明显问题：

1. `pg_wal` 只覆盖最近 WAL，不能代表完整历史窗口。
2. 真正长期保留的 WAL 通常已经归档到 `archive_dest`。
3. `pg_wal` 中更老的 segment 可能已被覆盖。

## 已拍板模型

最终模型已经确定为：

- 主配置语义：`archive_dest`
- 运行时双来源解析：
  - `pg_wal`
  - `archive_dest`
- 当两端都不能提供所需 segment 时：
  - 进入缺失 WAL 恢复层
  - 恢复失败则统一报 `WAL not complete`

也就是说，最终产品不再是“用户告诉扩展一个固定目录，然后所有 WAL 都从这个目录读”。

## segment 状态分类

对目标时间窗需要的每个 segment，运行时需要判定四种状态：

- 仅存在于 `pg_wal`
- 仅存在于 `archive_dest`
- 同时存在于两端
- 两端都不存在

## 读取优先级

当同一个 segment 同时出现在两端时，规则已经定为：

- `archive_dest` 缺失时才从 `pg_wal` 读取
- 两端重叠时一律优先 `archive_dest`
- `pg_wal` 只承接 archive 尚未覆盖的 recent tail

也就是说，当前模型不再要求 resolver 去猜“recent / 历史”的优先级；只要 `archive_dest` 已有稳定归档文件，就直接以它为准。

## `pg_wal` 错配检测

对于来自 `pg_wal` 的候选 segment，来源解析层需要检查：

- 文件名解析出的 `timeline + segno`
- 首个 long page header 中的 `xlp_seg_size`
- 首个 long page header 的 `xlp_pageaddr`

如果：

- 文件名对应的 segno
- 和 long page header 对应的 segment 起点

不一致，就说明该文件可能已经 recycled / 覆盖 / 命名与内容错配。

这类 `pg_wal` 文件不能直接参与 flashback，应转入 `ckwal` 恢复路径。

## 被覆盖 WAL 的恢复

如果查询窗口需要的 segment：

- 已经不在 `pg_wal`
- `archive_dest` 中也缺失或损坏

则进入“缺失 WAL 恢复”模型。

这一层后续参考：

- `/root/xman` 中的 `ckwal`

但边界已经定死：

- 只参考“缺失 segment 复原”的思路
- 不把 `pg_flashback` 做成外部恢复工具壳子
- 复原后的 segment 仍然回到扩展自己的来源解析层继续消费
- 恢复层的输入应当是“需要哪个 `timeline + segno`”
- 恢复层的输出应当是“把可信 segment 放入扩展私有 `recovered_wal/` 后再次参与解析”
- 当前已经接入最小接口层：
  - `pg_flashback.debug_pg_wal_dir`（开发期专用）

当前实现语义：

- 优先尝试 `archive_dest`
- archive 缺失时才尝试 `pg_wal`
- `pg_wal` 名称/头部/pageaddr 错配时，不继续信任它
- 若 `pg_wal` 与 `archive_dest` 均无法提供可信 segment，则进入扩展内嵌恢复路径
- 对于错配/覆盖的 `pg_wal` 段，恢复层需要先把其实际 header 所对应的 segment 转换为标准 `ckwal` 结果
- 转换后的结果不仅要落到 `DataDir/pg_flashback/recovered_wal/`
- 还必须立即回灌到本轮 resolver 的候选集合
- resolver 需要持续消费：
  - `archive_dest`
  - `pg_wal`
  - `recovered_wal`
- 直到拼出连续可扫描的 segment 集，或确认三路来源都无法提供可信段

## 当前实现与最终模型的关系

- `archive_dir=/path/to/pg_wal` 只是当前 PG18 基线开发配置
- `archive_dir` 不是最终产品接口
- 后续代码实现应持续沿用独立的 `fb_wal_source_resolver`
- 现有单目录扫描逻辑只作为过渡基线

## 结论

- `pg_flashback()` 的 WAL 来源最终是“解析层决定”，不是“用户手填单目录”
- `pg_wal + archive_dest` 双来源解析是既定方案
- 被覆盖 WAL 的恢复已经进入正式架构，不再只是临时想法
