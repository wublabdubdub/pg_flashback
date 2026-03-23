# 反向操作流

## ForwardOp

结构化表示 WAL 中解析出的正向 DML：

- `txid`
- `commit_ts`
- `commit_lsn`
- `op_type`
- `old_row`
- `new_row`
- `old_key`
- `new_key`

## ReverseOp

用于查询和导出的统一中间结构：

- `txid`
- `commit_ts`
- `commit_lsn`
- `reverse_type`
- `old_row`
- `new_row`
- `old_key`
- `new_key`

## 转换规则

- 正向 `INSERT new_row` -> 反向 `REMOVE new_row`
- 正向 `DELETE old_row` -> 反向 `ADD old_row`
- 正向 `UPDATE old_row -> new_row` -> 反向 `REPLACE new_row WITH old_row`

## 顺序规则

- 事务按提交顺序从新到旧
- 单事务内部按原执行顺序逆序

