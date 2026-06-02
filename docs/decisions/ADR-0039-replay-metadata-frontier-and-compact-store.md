# ADR-0039：将 Replay 生命周期 / Prune 元数据前移到 3/9，并引入 Compact BlockReplayStore

## 状态

已采纳（2026-04-12）

## 背景

在 14pg live 现场：

```sql
select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents",
                           '2026-04-11 07:40:40.223683+00')
order by id;
```

当前已确认：

- `3/9` 主热点已从 `locator-only stub / stats sidecar` 转移出去
- 新主热点前移到：
  - `4/9 replay discover`
  - `6/9 replay final`
- 对应真实热点已经收口为：
  - `ValidXLogRecord/XLogReadRecord -> fb_wal_load_record_by_lsn`
  - `fb_replay_get_block() / hash_search()`
  - `fb_replay_build_prune_lookahead()`

现状问题不是单点函数常数，而是阶段间重复工作：

1. `4/9` / `6/9` 仍缺 per-block 生命周期元数据，导致 block state 常驻过久
2. `BlockReplayStore` 仍使用 `dynahash + RelFileLocator/fork/blkno`
   的重 key 和双 probe 热路径
3. `6/9` 仍会先 backward 全扫一次 cursor 构建 prune lookahead，
   再 forward 全扫一次 final replay
4. discover 虽然已是 skeleton-first，但进入 apply 后仍会把大量 record
   带进 block store 热路径

此前已经明确否决的一条错误路径是：

- 为 `last_use` 在 replay 前额外反扫一遍完整 cursor
- 该试验在 14pg 高内存对照里额外引入约 `10s+` 冷跑成本

因此下一阶段必须把 replay 需要的生命周期 / future 元数据前移到已有
`3/9 payload/index` 链路顺手产出，而不是在 replay 期再做第二遍全量扫描。

## 决策

下一阶段主线固定为四刀联动：

1. 将 block 生命周期元数据前移到 `3/9`
2. 用 query-local compact store 重写 `BlockReplayStore`
3. 删除 `6/9` backward prune prepass
4. 将 discover 收窄到 `frontier apply`

### 1. 生命周期元数据前移

`3/9` 在现有 payload/index 物化链路里顺手产出：

- per-block `last_replay_use_record_index`
- per-block `last_prune_guard_index`

约束：

- 只能 piggyback 当前 payload/index 物化
- 不允许再新增第二遍 WAL / cursor 扫描
- discover / warm / final 都按这套元数据即时 retire block state

### 2. Compact BlockReplayStore

`BlockReplayStore` 从现有 query-lifetime dynahash 改为 query-local compact store：

- 主表 / TOAST 分域
- key 压成 `(domain, blkno)` 的 64-bit 紧凑键
- page bytes 放到 slab/arena
- lookup / insert 目标为单 probe open addressing

### 3. 删除 final 的 backward prune prepass

`6/9` 不再现算：

- `fb_replay_build_prune_lookahead()`

改为由 `3/9 payload` 直接产出 compact future-block delta，
final 只做 dense vector 读取。

### 4. discover 收窄到 frontier apply

discover 只对当前 missing-block 闭包相关 record 做：

- materialize
- apply
- block store touch

其余记录保持 skeleton-only，不再进入当前热点路径。

## 预算

该决策附带明确预算约束：

- `3/9` wall time 可以上升，但首轮实现预算固定为 `<= 5s`
- 若同一条 14pg live SQL 的 `3/9` 增量超过 `5s`，
  则该实现形态直接判失败

也就是说：

- `3/9` 允许做更多“顺手产出”
- 但不允许把重复工作简单平移成新的主热点

## 为什么不继续做局部微优化

- 仅优化 `hash_create()` 桶数、memory context 或个别 helper，
  无法移除 `6/9` 的双遍扫描
- 仅继续压 `fb_wal_load_record_by_lsn()` 常数，
  无法消除 replay 期重复 block lifecycle 判断
- 只有把 replay 所需元数据前移并收窄 replay 实际触达范围，
  才能同时打到：
  - `4/9` 的首读 WAL 冷读
  - `4/9/6/9` 的 block store 哈希热路径
  - `6/9` 的 backward prepass

## 结果

预期收益固定为：

- `4/9 replay discover` 降低约 `60%`
- `6/9 replay final` 降低约 `60%`

同时接受：

- `3/9` 小幅上升

但必须满足：

- `3/9` 增量 `<= 5s`

## 后果

正向：

- replay 生命周期元数据改为一次产出、多阶段复用
- final 不再做独立 backward prepass
- compact store 同时降低 CPU cache miss 和 tracked bytes

代价：

- `fb_wal` / `fb_replay` 契约会新增一层 query-local replay metadata
- `record_index` / locator stream / head-tail spool 混合读取的一致性要求更高
- 需要补回归锁住：
  - 生命周期元数据正确性
  - final 不再构建 backward lookahead
  - compact store 的 retire / lookup 行为

