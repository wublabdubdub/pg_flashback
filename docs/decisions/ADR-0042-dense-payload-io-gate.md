# ADR-0042：为 dense streaming payload reader 增加全局 IO gate

## 状态

已采纳（2026-04-12）

## 背景

在 14pg `alldb` 上继续追
`scenario_oa_50t_50000r.users @ '2026-04-11 07:50:40.916510+00'`
的慢样本时，发现：

- clean restart / 单 backend 下，同一条 SQL 当前只有：
  - `3/9 metadata ~= 0.896s`
  - `3/9 payload ~= 0.484s`
  - 总耗时 `~= 4.359s`
- 但只要并发压上多条 dense `documents` flashback / `fb_recordref_debug`
  查询，同一版本下 `users` 就会被放大到：
  - `3/9 metadata ~= 13.717s`
  - `3/9 payload ~= 53.111s`
  - 总耗时 `~= 74.292s`

对慢 `users` backend 直接查看 `/proc/<pid>/stack`，
内核栈停在：

- `generic_file_buffered_read`
- `xfs_file_buffered_aio_read`
- `ksys_pread64`

这说明慢点不是 `users` 自身切到了新的错误 payload plan，
而是 dense `documents` 的 `3/9 payload`
并发占满了 shared WAL buffered IO，
把其他本来很轻的 flashback 查询一起拖慢。

## 决策

对满足以下条件的 payload reader，增加一个全局 advisory gate：

- `streaming` open pattern
- payload locator 数量达到 dense 大查询量级
- covered segment 数量达到 dense 大查询量级

当前 gate 只作用于这类 dense payload exact-reader：

- 进入 exact payload scan 前先获取 advisory lock
- 扫描结束后立即释放
- 稀疏 locator / 轻查询不进入该 gate

也就是说：

- 大 `documents` 查询在 `3/9 payload` 不再并发冲击 WAL 读带宽
- `users/meetings/approval_tasks` 这类轻查询不需要等自己同类，
  只需避开 dense payload flood

## 为什么这样做

- 这次问题的根因不是单条查询的算法退化，而是跨 backend IO 争用
- 继续只优化 `users` 自身 payload path，不能解决它被别的 dense query 拖死的问题
- 相比重新引入 windowed serial runtime 或大规模共享缓存，
  一个只限 dense case 的 gate 更小、更可控，也更容易 live 验证

## 结果

修复后在同一台 14pg 机器上：

- 并发压测下 `users @ 07:50`
  已回到：
  - `3/9 metadata ~= 1.061s`
  - `3/9 payload ~= 0.619s`
  - 总耗时 `~= 6.630s`
- 同时观察到 dense `documents` backend
  明确等待 `wait_event_type='Lock', wait_event='advisory'`

这说明：

- IO flood 已被 gate 串行化
- 轻查询不再被 dense payload 并发直接拖到几十秒

## 代价

- 多条 dense `documents` 查询会在 `3/9 payload` 排队
- 这是有意接受的代价；目标是避免它们一起把机器拖入更坏的全局退化

## 后续

- 继续观察 dense payload gate 的阈值是否需要更细化
- 若未来有更低成本的共享 WAL 读缓存 / 协同扫描方案，再评估是否替换该 gate
