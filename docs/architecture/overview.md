# 总体架构

## 模块划分

- `fb_entry`：SQL 入口与统一错误出口
- `fb_catalog`：relation 校验、模式选择、TOAST 定位
- `fb_wal_reader`：时间窗扫描、事务提交信息提取
- `fb_wal_source_resolver`：后续 `pg_wal / archive_dest` 双来源解析与缺失 WAL 恢复衔接
- `fb_decode`：从 WAL 生成 `ForwardOp`
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
- `fb_flashback_materialize()`

当前仍未完成：

- `fb_export_undo()`
- 稳定的 `ForwardOp` 调试出口
- `pg_wal / archive_dest` 双来源解析
- 被覆盖 WAL 的恢复层
