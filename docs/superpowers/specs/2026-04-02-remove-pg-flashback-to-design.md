# 删除 `pg_flashback_to` 设计稿

## 目标

将 `pg_flashback_to(regclass, text)` 从 `pg_flashback` 项目中彻底删除，只保留查询式 `pg_flashback(anyelement, text)` 作为正式产品入口。

## 已确认边界

- 这是破坏兼容性的删除。
- 删除后不保留兼容别名、stub、NOTICE 或迁移提示。
- 删除范围包含：
  - 公开安装面
  - 后端实现代码
  - 回归与构建入口
  - 文档、架构说明、ADR、README
- 删除后产品路线固定为：
  - `SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);`

## 方案

采用硬删除方案：

1. 从扩展安装脚本中移除 `pg_flashback_to(regclass, text)`。
2. 删除 `fb_export` 中仅为原表回退服务的实现代码与头文件。
3. 删除 `fb_flashback_to` 回归以及所有仍将 `pg_flashback_to` 视为现行能力的用户面断言。
4. 更新产品文档和架构文档，把“原表闪回 / inplace rewind”改为历史废弃路线，不再作为当前能力描述。

## 风险

- 文档残留会继续误导后续 agent 和维护者。
- 若删除时误伤查询式 `pg_flashback()` 共用逻辑，会造成主链回归。

## 验证

- `fb_user_surface` 必须改为断言 `pg_flashback_to(regclass, text)` 不存在。
- 扩展安装后，`to_regprocedure('pg_flashback_to(regclass,text)')` 必须返回 `NULL`。
- 删除 `fb_flashback_to` 回归后，其余查询式主链回归继续通过。
