# WAL 解码层

首版解码层目标：

- 在 `(target_ts, query_now_ts]` 内识别目标表相关 `INSERT/DELETE/UPDATE`
- 提取事务提交状态
- 报告 WAL 缺失
- 检测不支持场景：
  - DDL
  - truncate
  - rewrite
  - relfilenode 变化

首版不要求：

- 支持全部 rmgr
- 覆盖所有 PostgreSQL 版本
- 支持逻辑执行 undo

首个实现目标是 PG18。

## 外部参考边界

WAL 扫描与解析实现允许参考 `/root/pduforwm`，重点参考：

- `/root/pduforwm/docs/replayWAL.md`
- `/root/pduforwm/src/pg_walgettx.c`
- `/root/pduforwm/src/decode.c`

参考边界固定为：

- 参考 WAL 顺序扫描、事务提交提取、行像拼接思路
- 不直接移植其 CLI、配置模型、输出表结构或 replay 产品形态
- `pg_flashback` 仍以扩展内部 `reverse op stream` 为唯一主线

## 当前 PG18 基线实现

当前已落地的 PG18 扫描骨架只做轻量 pass：

- 选择单一来源目录中最高 timeline 的 WAL 段
- 对 WAL 段排序并做连续性检查
- 用 `XLogReader` 顺序扫描 record
- 记录时间窗内事务边界
- 检测目标 relation 的 `truncate / rewrite / storage change`
- 通过 `fb_scan_wal_debug(regclass, timestamptz)` 暴露稳定扫描摘要

当前阶段明确不做：

- 行像解码
- SQL 文本拼装
- undo SQL 渲染
- TOAST 文件回读
- 外部表/文件输出
- `pg_wal / archive_dest` 双来源解析
- 被覆盖 WAL 的恢复

关于后一部分的正式设计，见：

- `docs/architecture/wal-source-resolution.md`

## `/root/pduforwm` 为什么慢

已经确认，它慢的主要原因不是单纯 `XLogReadRecord()` 本身，而是热路径里塞了太多“扫描之外的工作”。

1. 扫描主循环里带着大量额外耗时统计，本身已经暴露出热点分布  
   参考 `/root/pduforwm/src/pg_walgettx.c:101-116` 与 `/root/pduforwm/src/pg_walgettx.c:185-297`

2. 扫描阶段直接把结果写入 `public.replayforwal`，还带 COPY 批量提交和断点续扫逻辑  
   参考 `/root/pduforwm/src/pg_walgettx.c:842-1308` 与 `/root/pduforwm/docs/replayWAL.md:47-92`

3. 扫描热路径里会把 decode 结果立刻物化成 SQL 字符串并缓存/输出  
   参考 `/root/pduforwm/src/pg_walgettx.c:2328-2405` 与 `/root/pduforwm/src/tools.c:1838-1969`

4. relation 上下文查找和 toast 检查发生在热路径里，且带额外索引/回退逻辑  
   参考 `/root/pduforwm/src/pg_walgettx.c:6588-6715`

5. UPDATE old/new tuple 的拼接依赖全局 `parray` 状态，扫描过程中有额外状态机和分配成本  
   参考 `/root/pduforwm/src/decode.c:246-294`

6. TOAST 恢复会继续回读外部文件和 FPW/数据文件内容，不再是纯 WAL 顺序扫描  
   参考 `/root/pduforwm/src/decode.c:2322-2621` 与 `/root/pduforwm/src/pg_walgettx.c:7841-7979`

## `pg_flashback` 的规避原则

为避免继承上述慢路径，后续实现固定遵守：

- 先做轻量 scan，再做深度 decode，不允许一遍扫描里做完所有事
- 在拿到 target relation 之前，不做 SQL 拼装和 tuple 物化
- 扫描层只产出结构化事实，不产出 SQL 字符串
- 扫描层不写数据库表、不写外部结果文件
- TOAST/大字段处理延后到真正的行像重建阶段
- relation 过滤尽量前置，避免“先全量 decode，后回头筛表”
