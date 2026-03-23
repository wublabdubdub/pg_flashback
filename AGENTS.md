# AGENTS.md

本文件面向接手 `pg_flashback` 项目的 agent。

## 先读顺序

1. `STATUS.md`
2. `TODO.md`
3. `PROJECT.md`
4. `docs/specs/2026-03-22-pg-flashback-design.md`
5. `docs/architecture/overview.md`
6. 你即将修改的源码文件

## 本机 PG18 登录方式

本机 PostgreSQL 18 的本地登录方式固定记为：

```bash
su - 18pg
psql postgres
```

需要进入 PG18 环境做手工验证、建测试 role、跑 SQL 时，先按这个方式进入。

## 项目已拍板约束

- 路线固定为 `reverse op stream`，不走物理页回放主路径。
- 当前已修正为：
  - `reverse op stream` 仍然是最终查询/导出的逻辑层
  - 但其来源必须是 `checkpoint + FPI + block redo` 的页级重放内核
- 当前用户接口也新增了一个务实层：
  - `fb_flashback_materialize(regclass, timestamptz, text)` 从数据字典自动生成列定义并落临时表
  - 这是当前阶段“免手写 `AS t(...)`”的实际可用方案
- 扩展首版严格只读。
- 同时支持：
  - 历史结果集查询
  - undo SQL / reverse op 导出
- 无主键表按 `bag/multiset` 语义处理。
- `target_ts -> query_now_ts` 期间归档 WAL 必须完整，否则直接报错。
- 时间窗内若检测到 DDL / rewrite / truncate / relfilenode 变化，直接报错。
- TOAST 首版支持，但重建失败直接报错。
- 开发阶段文档用中文，代码和标识符用英文。
- 函数名前缀统一用 `fb`，不要引入 `pfb`。
- `/root/pduforwm` 仅作为 WAL 扫描与行像拼接思路参考：
- `/root/pduforwm` 尤其要参考这些页级重放路径：
  - `heap_xlog_delete`
  - `heap_xlog_update`
  - `restoreDEL`
  - `redoDEL`
- 重点参考 `docs/replayWAL.md`
- 重点参考 `src/pg_walgettx.c`
- 重点参考 `src/decode.c`
- 以上参考只用于理解扫描/解析思路，不直接继承它的接口、目录设计、输出协议或产品行为。
- 已确认 `/root/pduforwm` 的扫描偏慢，后续实现必须继续规避这些模式：
  - 扫描主循环内生成 SQL 字符串
  - 扫描阶段落库 `public.replayforwal`
  - 先全量 decode、后按 target relation 过滤
  - 在热路径里做 TOAST 文件/数据文件回读
  - 在热路径里做事务分组输出、SQL cache 和大量上下文查找
- 当前 `pg_flashback.archive_dir` 只是 PG18 基线开发配置。
- 后续要升级为：
  - `archive_dest` 主配置
  - 对 `pg_wal` / `archive_dest` 的双来源解析
  - 以及被覆盖 WAL 的恢复策略
- 被覆盖 WAL 的恢复思路后续参考 `/root/xman` 的 `ckwal`，但只作参考，不直接绑定其实现

## 修改规则

- 任何新需求，先更新文档记录在案，再开始实现。至少要落到：
  - `STATUS.md`
  - `TODO.md`
  - 必要时同步 `PROJECT.md` / 架构文档
- 任何架构变更，先更新：
  - `STATUS.md`
  - `TODO.md`
  - 对应 `docs/decisions/ADR-*.md`
- 任何新增模块，先在 `docs/architecture/overview.md` 和相关架构文档中登记职责。
- 不要把“待验证想法”直接写进 `PROJECT.md`，那里面只放已经确定的产品边界。
- 不要实现自动执行 undo SQL 的能力；导出可以做，执行不在首版范围。
- 如果改了 `include/*.h` 中的结构体或函数声明，不要只跑增量 `make install`。
  - 当前 PGXS 增量构建对头文件依赖不可靠
  - 这种情况下统一使用：
    - `make clean`
    - `make install`
    - 再跑回归

## 当前首要目标

`P5` 已完成。当前主线进入：

- `P6`：`fb_export_undo`
- 开发期 `ForwardOp/ReverseOp` 调试出口重建
- TOAST / `multi_insert` / 主键变更等正确性补齐

## 交接要求

结束当前工作前至少更新：

- `STATUS.md` 中的“已完成 / 进行中 / 下一步”
- `TODO.md` 中对应任务勾选状态
- 若有新决策，新增或更新 `ADR`
