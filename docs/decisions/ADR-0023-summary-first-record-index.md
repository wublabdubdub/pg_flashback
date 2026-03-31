# ADR-0023 将 summary 提升为 3/9 build record index 主索引

## 状态

已接受

## 背景

`summary v3` 已经提供：

- relation spans
- xid outcomes

但当前查询侧仍保留较重的 `3/9 build record index` 主链：

- metadata 仍以 WAL 顺序扫描为主
- `xid fill` 虽然优先走 summary outcome，但实现仍按 `resolved_segment_count` 全量顺序读取 summary 文件
- 同一查询内会重复 `open/read/close` 相同 `summary-*.meta`

这使得 summary 目前更像“辅助过滤器”，而不是 `3/9` 的真正主索引。

## 决策

将 summary 提升为 `3/9 build record index` 的主索引层，并保持 `meta/summary` 继续走低存储原则。

本轮固定口径：

- `xid fill` 只读取命中 query window 的 summary segment，不再全量扫描 resolved segment
- 查询期新增 backend-local summary section cache，避免同一查询重复读相同 summary 文件
- 在现有 relation spans / xid outcomes 之外，新增两类低存储 section：
  - relation-scoped `touched xids`
  - relation-scoped `unsafe facts`
- `unsafe facts` 只记录 relation-scoped unsafe 事实，字段固定收敛到：
  - `reason`
  - `scope`
  - `storage_op`
  - `xid`
  - `record_lsn`
- metadata 主链改为：
  - summary-first 收敛 touched / unsafe
  - xid status 继续优先走 summary outcomes
  - 仅对 summary 缺失、损坏或覆盖不足的 uncovered window 回退 WAL 扫描
- 原有 WAL metadata/xact 路径继续保留，作为 meta 未及时生成或 sidecar 不可用时的安全回退
- checkpoint / anchor 继续复用现有 checkpoint sidecar，不并入 summary

## 为什么不把 checkpoint 一并塞进 summary

- 当前 checkpoint sidecar 已经满足 anchor hint 需求
- checkpoint 语义与 relation-scoped summary 不同，强行合并会放大 sidecar 体积与复杂度
- 先把 relation/touched/xid/unsafe 主链打下来，收益更直接

## 低存储原则

- 仍保持“一段 WAL 一个 sidecar”
- `touched xids` / `unsafe facts` 都只保留 relation-scoped最小事实，不复制可由其他 section 推导的字段
- `unsafe facts` 只记录 relation-scoped blocker 事实，不记录可由现有 spans/outcomes 推导的冗余信息
- 不引入查询期持久化 cache 文件
- 新增 cache 仅允许存在于 backend-local 内存

## 后果

优点：

- `3/9` 可从“summary 辅助过滤”升级为“summary-first 主索引”
- 去掉全量 summary xid scan 与大量重复文件 I/O
- metadata 串行 WAL pass 只剩 uncovered fallback，live case 更有机会显著下降
- 保留旧 WAL 路径后，meta 缺失/滞后场景不会阻塞查询正确性

代价：

- summary 文件格式需要再升版本
- 需要新增 `touched xids` / `unsafe facts` 编解码与回退逻辑
- 需要补充“删掉现有 meta 后重建仍正确”的专项验证

## 验证要求

- 变更完成后必须手动清空 `DataDir/pg_flashback/meta/summary`
- 重新预建 summary
- 再跑回归与 live 调试 case
- 不允许以旧 summary 兼容命中掩盖真实效果
