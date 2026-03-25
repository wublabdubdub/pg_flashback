# 总体架构

## 模块划分

- `fb_entry`：真实 flashback / undo 的 SQL 入口与统一错误出口
- `fb_runtime`：扩展私有目录初始化、校验与状态摘要
- `fb_guc`：运行时 GUC 定义与来源配置选择
- `fb_progress`：`pg_flashback()` 的客户端进度上下文与 `NOTICE` 输出
- `fb_memory`：query 级内存跟踪与统一 charge 辅助函数
- `fb_catalog`：relation 校验、模式选择、TOAST 定位
- `fb_wal`：WAL 时间窗扫描、事务提交信息提取、来源解析、unsafe 判定，以及 segment 级并行预筛选 / 命中窗口跳段 / prefilter cache
- `fb_ckwal`：内嵌缺失 WAL 恢复逻辑与 recovered_wal 目录输出
- `fb_parallel`：opt-in 的 bgworker 并行 apply/write 编排、结果表 helper、DSM/shm_mq 协议与 stage `8/9` 进度聚合
- `fb_replay`：页级重放、行像提取与 `ForwardOp` 生成
- `fb_reverse_ops`：生成 `ReverseOp`
- `fb_apply_keyed`：有键表的反向应用
- `fb_apply_bag`：无键表的 bag 应用
- `fb_export`：undo SQL / reverse op 导出
- `fb_toast`：TOAST 解码
- `fb_compat_pg18`：PG18 基线兼容入口

## 当前阶段

当前仓库已经完成：

- `RecordRef`
- `BlockReplayStore`
- `ForwardOp / ReverseOp`
- `keyed / bag`
- `pg_flashback()`
- 速度优先的 `UNLOGGED` 结果直写
- `archive_dest + pg_wal + recovered_wal` 来源解析
- 内嵌 `fb_ckwal`
- `parallel_segment_scan`
- baseline 快照恢复式 deep full runner

当前仍未完成：

- `fb_export_undo()`
- batch B / residual `missing FPI` 收敛
- TOAST store 的内存硬上限覆盖
- 主键变更等剩余正确性补齐
- `fb_wal` 中 segment 级并行预筛选 / 命中缓存在真实 flashback 主路径上的端到端收益继续收敛
- stage `8/9` 真正的 bgworker 并行 apply/write 仍未完成

## 维护入口

- 源码导读：`docs/architecture/源码级维护手册.md`
- 代码优先入口导读：`docs/architecture/核心入口源码导读.md`
- 调试手册：`docs/architecture/调试与验证手册.md`
