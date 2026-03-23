# WAL 来源解析

## 当前问题

当前 PG18 基线实现把：

- `pg_flashback.archive_dir`

直接当成“唯一 WAL 来源目录”。

这只适合开发验证，不适合作为最终产品模型。  
用户把它直接设成：

```sql
SET pg_flashback.archive_dir = '/isoTest/18pgdata/pg_wal';
```

虽然能跑通基线验证，但存在三个明显问题：

1. `pg_wal` 只覆盖最近 WAL，不能代表完整历史窗口。
2. 真正长期保留的 WAL 通常已经归档到 `archive_dest`。
3. `pg_wal` 中更老的 segment 可能已被覆盖。

## 目标模型

后续要升级为：

- 主配置语义：`archive_dest`
- 运行时来源判断：
  - recent WAL 可能仍在 `pg_wal`
  - 历史 WAL 可能只在 `archive_dest`
  - 两者都不完整时，需要走恢复链路

也就是说，后续不应再把产品配置建模成“用户告诉扩展一个固定目录，然后所有 WAL 都从这个目录读”。

## 需要解决的三个子问题

### 1. segment 位于哪里

对目标时间窗需要的每个 WAL segment，要能判断它当前位于：

- `pg_wal`
- `archive_dest`
- 两者都存在
- 两者都不存在

### 2. 优先级如何选

当同一个 segment 同时出现在：

- `pg_wal`
- `archive_dest`

时，需要明确：

- 优先读哪一侧
- 如何校验两侧是否一致
- 是否接受 partial / recycled / corrupted segment

当前建议：

- recent segment 优先尝试 `pg_wal`
- 历史 segment 优先尝试 `archive_dest`
- 对重叠 segment 保留一致性校验

### 3. `pg_wal` 已被覆盖怎么办

这是最关键的缺口。

如果查询窗口需要的 segment：

- 已经不在 `pg_wal`
- `archive_dest` 中也缺失或损坏

那就不能直接报一个模糊错误了，而应该进入“缺失 WAL 恢复”模型。

这一层后续需要参考：

- `/root/xman` 中的 `ckwal`

但要注意：

- 只参考恢复思路
- 不直接把 `pg_flashback` 绑定成外部工具驱动模型

## 当前结论

- `archive_dir=/path/to/pg_wal` 只是开发期基线配置，不是最终设计
- 最终产品必须有 `pg_wal + archive_dest` 双来源解析
- 被覆盖 WAL 的恢复策略必须进入正式路线图
