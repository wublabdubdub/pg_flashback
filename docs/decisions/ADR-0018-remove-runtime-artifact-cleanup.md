# ADR-0018 取消 `pg_flashback` 运行期产物自动删除

## 状态

已接受

## 背景

当前 `pg_flashback` 会自动维护：

- `DataDir/pg_flashback/runtime`
- `DataDir/pg_flashback/recovered_wal`
- `DataDir/pg_flashback/meta`

旧实现里，这些目录除了负责承载运行期产物外，还带有自动删除语义：

- runtime 初始化时扫描目录并做 cleanup
- 查询结束时再次触发 cleanup
- `runtime_retention` / `recovered_wal_retention` / `meta_retention` 控制按年龄淘汰
- 部分 query-owned 文件会在 destroy 路径上直接 unlink / rmtree

当前判断已经明确：

- 这些文件总体很小
- 正常路径下不会再反复拷贝大量 WAL
- 当前阶段更需要保留现场和缓存，而不是做目录保洁

## 决策

取消 `pg_flashback` 自身对 `DataDir/pg_flashback/*` 的自动删除逻辑。

具体固定为：

- 删除 GUC：
  - `pg_flashback.runtime_retention`
  - `pg_flashback.recovered_wal_retention`
  - `pg_flashback.meta_retention`
- 删除 runtime 初始化时的 cleanup
- 删除查询结束时触发的 cleanup
- 删除为 cleanup 服务的 debug SQL 入口与回归
- `runtime/`、`recovered_wal/`、`meta/` 只负责提供目录和承载产物
- 已生成文件默认保留，不再由扩展自动淘汰

## 后果

优点：

- 去掉查询前后不必要的目录扫描与删除动作
- 保留现场，便于复核缓存命中、WAL 恢复与 sidecar 行为
- 用户不需要再理解 retention 参数

代价：

- `DataDir/pg_flashback/*` 目录会持续积累历史产物
- 若后续出现极端长周期积累，需要运维侧手工清理
- 原先依赖“查询结束即删”的测试口径需要删除或重写

## 语义要求

本次决策不改变以下行为：

- 扩展仍自动初始化 `DataDir/pg_flashback/{runtime,recovered_wal,meta}`
- 现有文件命名和缓存复用语义保持不变
- 未定义的 `pg_flashback.*` 参数继续直接报错
