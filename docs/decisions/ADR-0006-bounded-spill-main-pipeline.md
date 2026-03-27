# ADR-0006: bounded spill 作为大窗口 flashback 主链的一部分

## 状态

Accepted

## 决策

对大时间窗 / 大表 flashback 查询，扩展内部引入 bounded spill 主链。

具体约束：

- 用户 SQL 入口保持 `pg_flashback(anyelement, text)` 不变
- 不恢复结果表物化老模型
- 不把中间结果落到普通 heap 表
- query 生命周期内允许把：
  - reverse op
  - 后续 wal index / replay 相关中间产物
  写入扩展私有 runtime 文件

首阶段统一以 `FbReverseOpSource` 为 apply 上游抽象。

## 原因

- stage 8/9 已完成小内存化，但前置主链仍可能因大时间窗 WAL / reverse op 暂存触达 memory limit
- 单纯继续堆数组优化，收益有限且容易把复杂度散落到多个模块
- append-only 的 runtime spill 更符合现有“顺序扫描 -> 顺序 replay -> 顺序 apply”的链路特征

## 影响

- 新增 query 级 `fb_spool`
- reverse op 不再强制要求纯内存数组承载
- 后续 `fb_wal` / `fb_replay` 可以继续接入 sidecar / spill 方案

## 不做

- 不引入新的客户可见 SQL 接口
- 不做跨查询持久化 cache
- 不在热路径里生成 SQL
- 不把中间态落库到普通表
