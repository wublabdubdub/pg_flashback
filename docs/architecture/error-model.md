# 错误模型

以下错误应直接失败：

- 目标 relation 不支持
- `target_ts` 晚于查询起点时间
- 归档 WAL 不完整
- 时间窗内发现 DDL / rewrite / truncate
- TOAST 无法重建
- 关键事务提交状态无法确定

禁止行为：

- 返回部分结果
- 降级成 best-effort
- 静默忽略 WAL 缺失

