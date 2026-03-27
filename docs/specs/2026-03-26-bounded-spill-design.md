# 2026-03-26 bounded spill 主链设计

## 背景

当前 `pg_flashback()` 已经把 stage 8/9 的 apply 改成“小内存、变化集驱动”的 keyed / bag 流式执行，但前置主链仍然存在两类高水位：

- `fb_wal_build_record_index()` 需要为候选 WAL 记录维护 `RecordRef` / payload 索引
- `fb_replay` / `fb_reverse_ops` 仍有“先全量积累，再统一消费”的内存峰值

在 `12GB` 级别大表、时间窗较大、`archive_dest` 覆盖足够多 WAL 的场景下，即使 stage 8/9 已经不再按当前整表大小耗内存，整个查询仍然可能因为：

- `RecordRef array`
- 反向操作暂存
- TOAST / payload 保留

触发 `pg_flashback.memory_limit_kb` 超限。

## 目标

本轮 bounded spill 方案目标固定为：

- 保持现有公开 SQL 入口不变：
  - `SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);`
- 不恢复结果表物化老路径
- 不把 reverse op 主逻辑外包给外部表或数据库对象
- 在单次查询生命周期内，为：
  - WAL record index
  - replay 产物
  - reverse op
  提供统一的“内存上限 + 顺序 spill + 顺序回读”能力

## 非目标

- 不改变 `reverse op stream` 作为最终 apply 输入的主链定位
- 不引入新的用户可见 SQL 入口
- 不做跨查询持久化 cache
- 不在热路径里生成 SQL 字符串
- 不把中间结果落到普通 heap 表

## 设计原则

### 1. 单向顺序主链

bounded spill 只允许：

- 顺序追加
- 顺序回读

不允许：

- 依赖随机 seek 的对象布局
- 复杂回写
- 读写混杂导致的状态机膨胀

### 2. runtime 私有目录管理

所有 spill 文件统一放在：

- `DataDir/pg_flashback/runtime/<query-id>/`

由 query 级 session 负责创建和销毁，不暴露为用户对象。

### 3. 同一套 query 级内存记账

当前已有 `fb_memory` 负责 query 级 charge。bounded spill 继续沿用这一套口径：

- 内存内对象继续 charge
- 超出 soft budget 后转为 spill
- spill 后释放相应内存记账

### 4. 统一抽象，不让每个阶段各自造轮子

不为 `fb_wal`、`fb_replay`、`fb_reverse_ops` 分别造三套磁盘结构。

本轮统一抽象为：

- query 级 `fb_spool`
- append-only segment file
- typed record writer / reader

上层模块只关心：

- 写入什么 record
- 顺序读回什么 record

## 主体方案

## `fb_spool`

新增 query 级私有模块：

- `fb_spool`

职责：

- 为当前 query 创建 runtime 子目录
- 生成有序 spill 文件
- 提供 append / seal / sequential read API
- 管理临时文件名、生命周期与清理

## `FbReverseOpSource`

当前 reverse op 主链从“纯内存数组”改为统一 source 抽象：

- 内存内小数据：可直接保留在内存
- 超出阈值：写入 `fb_spool`
- apply 阶段通过统一 reader 顺序消费

这样可以保证：

- keyed / bag apply 不关心 reverse op 来自内存还是磁盘
- 后续 replay / wal index 也能逐步接入同一套 source 模型

## runtime spill SQL / 调试面

本轮增加：

- `sql/fb_spill.sql`

仅作为开发 / 回归期调试检查口径，用来确认：

- runtime 目录
- spill 文件
- 当前 query 是否触发 spill

不作为正式客户接口。

## 分阶段落地

### Stage A

先把：

- `fb_reverse_ops`
- `fb_apply`

切到 `FbReverseOpSource`，形成可编译、可回归的 bounded spill 骨架。

### Stage B

继续把：

- `fb_wal`
- `fb_replay`

逐步切到更稳定的 bounded spill / sidecar 方案，压缩前置阶段的高水位内存。

## 风险点

### 1. spill 后顺序回读与 progress 对齐

当前 progress 已固定为 9 段。spill 接入后不能破坏：

- scan / replay / reverse / apply / residual

的现有阶段语义。

### 2. TOAST 行像不能在 spill 热路径上再做额外回读

spill 只承接当前已经提取出的：

- record metadata
- reverse op
- 必要 tuple payload

不能把 TOAST 数据文件回读重新塞回热路径。

### 3. 文件数量与清理

必须保证：

- 单 query 退出后清理 runtime 子目录
- error path / abort path 也能清理
- 不污染 `archive_dest` / `recovered_wal`

## 当前结论

本轮拍板：

- bounded spill 走扩展私有 runtime 文件，不走 heap 表
- reverse op 仍是 apply 的统一输入模型
- 第一优先级先把 reverse op source 抽象立起来
- 之后再把 wal index / replay 主链往同一套 spool / sidecar 路径收敛
