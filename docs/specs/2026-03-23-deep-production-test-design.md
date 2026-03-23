# pg_flashback 深度生产化测试设计

## 目标

在当前 `P5.6` 完成、第一阶段开发基本闭合的前提下，对 `pg_flashback` 已支持能力做一次接近生产环境的深度验证。

本轮测试不再以“小表 + 小 WAL + 单次事务”的回归风格为主，而是转向：

- 大字段数
- 大数据量
- 大 WAL 时间窗
- 多事务边界
- 多目录 WAL 来源
- keyed / bag 双执行模型

目标是回答两个问题：

1. 当前已实现能力在大规模压力下是否仍然正确
2. 当前实现的性能、内存、WAL 来源、事务边界是否存在结构性问题

## 本轮测试只覆盖的能力

本轮深测只覆盖当前已经实现并宣称支持的能力：

- 普通持久化 heap 表
- `INSERT / DELETE / UPDATE / HOT UPDATE`
- `keyed` 模式
- `bag` 模式
- `archive_dest + pg_wal` 来源解析
- `ckwal` 恢复接口层
- 大时间窗下的页级重放与历史结果计算

## 本轮不纳入主验证目标的能力

以下能力不作为本轮深测通过标准：

- TOAST 重建正确性
- `multi_insert`
- 主键更新
- 时间窗内 DDL / rewrite
- 更早的可恢复锚点搜索
- `fb_export_undo`

这些内容会单独进入后续正确性阶段。

## 测试环境原则

深测环境应尽量接近生产而不是回归环境：

- 独立数据库，例如 `fb_deep_test`
- `full_page_writes=on`
- `archive_mode=on`
- `archive_dest` 指向稳定归档目录
- `pg_wal` 与 `archive_dest` 同时可用
- `pg_flashback.memory_limit_kb` 提高到适合长时间窗测试的上限
- `wal_compression=off`

`wal_compression=off` 的目的不是模拟所有生产配置，而是让 WAL 体量更稳定、更容易达到 “> 50 个 segment” 这一验收门槛。

## 数据模型要求

每轮深测至少包含两张核心测试表：

1. `keyed` 大表
2. `bag` 大表

共同要求：

- 字段数量 > 50
- 建议 64 列，便于固定生成脚本
- 数据量 > 5,000,000 行
- 类型以非 TOAST 类型为主：
  - `bigint`
  - `integer`
  - `numeric`
  - `boolean`
  - `timestamptz`
  - `char(n)` / `varchar(n)` 小字段

本轮不依赖大 `text/jsonb/bytea`，避免把 TOAST 正确性混入主验证。

## 时间窗设计

必须把“基线装载”和“flashback 验证窗口”分离。

标准流程：

1. 先装载 5,000,000 基线数据
2. 执行显式 `CHECKPOINT`
3. 记录 `target_ts`
4. 仅在 `target_ts -> now` 之间制造深度 DML

这样做的目的：

- 避免把基线装载阶段的大批量导入行为混入 flashback 主验证窗口
- 降低当前未覆盖 `multi_insert` 路径对本轮结果的干扰
- 让 “大 WAL > 50 segment” 主要来自验证窗口内的真实 DML

## 分批测试策略

### 批次 A：Keyed 大表高压 DML

目标：

- 验证当前最主干的 `keyed` 查询路径

建议场景：

- 表：`fb_deep_keyed_01`
- 列数：64
- 基线行数：5,000,000
- 窗口内 DML：
  - 大规模 `UPDATE`
  - 大规模 `DELETE`
  - 小批 `INSERT`
  - 显式事务提交
  - 显式事务回滚

要求：

- WAL segment > 50
- 至少验证 3 个时间点

### 批次 B：事务边界与回滚正确性

目标：

- 验证不是“按 SQL 回退”，而是按事务边界回退

建议场景：

- 多个显式事务交错执行
- target 前开始、target 后提交的事务
- target 后回滚事务
- 多事务反复修改同一批 key

验证重点：

- target 前未提交事务不可见
- target 后提交事务必须被撤销
- rollback 事务不得污染结果

### 批次 C：Bag 模式重复值大表

目标：

- 验证无主键表按 multiset 语义计算

建议场景：

- 表：`fb_deep_bag_01`
- 列数：64
- 行数：5,000,000
- 人为制造大量重复值
- 窗口内混合 `INSERT / DELETE / UPDATE`

验证重点：

- 结果集内容正确
- 重复次数正确

### 批次 D：WAL 来源跨目录

目标：

- 验证一次 flashback 可以跨 `archive_dest + pg_wal`

建议场景：

- 较老 WAL 已进入 `archive_dest`
- 最近尾部 WAL 仍留在 `pg_wal`
- 同一批验证跨越两个来源
- 单独构造一组 `pg_wal` 错配场景
- 使用 `ckwal_restore_dir` 提供恢复段

验证重点：

- 来源切换不影响结果正确性
- 错配 WAL 不被误信任
- 恢复目录可被接管

### 批次 E：长时间窗与内存压力

目标：

- 找出当前无 `spill/eviction` 模式下的工作极限

建议场景：

- 延长 `target_ts -> now` 窗口
- 将 WAL 规模提升到 `80-120` 个 segment
- 记录执行时间、内存上限触发点、失败模式

这一批可以失败，但必须产出边界数据。

## 真值校验方法

深测不能只看抽样结果，必须构建真值。

每个关键时间点都应保存一份快照表：

- `fb_truth_keyed_t0`
- `fb_truth_keyed_t1`
- `fb_truth_bag_t0`

### keyed 校验

对 flashback 结果与真值快照做双向校验：

- `flashback_result EXCEPT truth`
- `truth EXCEPT flashback_result`

只要任一方向非空，即判失败。

### bag 校验

对 flashback 结果与真值先做：

- `GROUP BY 全列`
- `count(*)`

再对聚合结果做双向 `EXCEPT`。

### 补充统计校验

除精确校验外，还应同时记录：

- `count(*)`
- 分布式聚合
- 关键列 checksum

这些指标有助于快速发现大规模偏差。

## 成功标准

本轮深测通过标准：

- 测试表字段数 > 50
- 每张核心测试表数据量 > 5,000,000
- flashback 验证窗口 WAL segment > 50
- `keyed` 场景双向 `EXCEPT = 0`
- `bag` 场景按全列聚合后双向 `EXCEPT = 0`
- 事务边界场景结果正确
- 跨目录来源解析场景结果正确
- `pg_wal` 错配 + `ckwal` 场景按预期恢复或报错
- 产出完整测试报告

## 输出物

本轮深测完成后，至少产出：

- 测试脚本
- 测试环境说明
- 每批执行日志
- 真值快照生成说明
- 结果校验 SQL
- 一份测试报告

## 当前建议执行顺序

1. 批次 A：Keyed 大表高压 DML
2. 批次 B：事务边界与回滚
3. 批次 C：Bag 模式重复值
4. 批次 D：WAL 来源跨目录
5. 批次 E：长时间窗与内存压力

原因：

- 先证明主链正确
- 再证明事务边界正确
- 再验证 bag 语义
- 再验证 WAL 来源解析
- 最后摸清当前实现极限
