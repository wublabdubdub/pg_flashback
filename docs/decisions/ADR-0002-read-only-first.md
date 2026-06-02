# ADR-0002: 首版严格只读

## 状态

Superseded by ADR-0016

## 决策

首版只支持：

- 返回历史结果集
- 导出 undo SQL / reverse op

明确不支持：

- 自动执行 undo SQL
- 修改业务表
- 任何带副作用的恢复动作

## 原因

- 保证首版风险可控
- 避免触发器、权限、RLS、副作用问题
- 保持 `pg_flashback` 的查询型扩展定位
