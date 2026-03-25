# 2026-03-23 TOAST 规模化测试报告

## 目标

验证 `pg_flashback` 当前 TOAST 历史值重建主链在生产化负载下的正确性与稳定性，覆盖：

- 大字段外置存储
- 同一时间窗内的 `INSERT / UPDATE / DELETE`
- 回滚事务不污染历史结果
- 主表闪回时不读取 live TOAST，而是读取扩展内部的历史 TOAST store

## 测试环境

- PostgreSQL 18
- 登录方式：`su - 18pg; psql postgres`
- WAL 来源：`current_setting('data_directory') || '/pg_wal'`
- 运行脚本：
  - `tests/deep/sql/80_toast_scale.sql`

## 测试场景

测试表：

- `fb_toast_scale_src`
- 主键：`id`
- TOAST 列：
  - `payload_a text`
  - `payload_b text`
- 两列都显式设置为 `STORAGE EXTERNAL`

基线规模：

- `4000` 行
- 两列均为大文本

时间窗内工作负载：

- `UPDATE 2000` 行的 `payload_a`
- `DELETE 500` 行
- `INSERT 500` 行
- 一轮 `UPDATE 1001` 行后 `ROLLBACK`

校验方式：

- `pg_flashback('fb_toast_scale_result', 'public.fb_toast_scale_src', target_ts)`
- 对 `truth` 与 flashback 结果做：
  - `count(*)` 对比
  - `md5(payload_a)` / `md5(payload_b)` 对比
  - `length(payload_a)` / `length(payload_b)` 对比
  - 双向 `EXCEPT` 聚合后的 `diff_count`

## 发现的问题

### 问题 1：旧 TOAST chunk 在主表 `HOT_UPDATE` 之前已被删除

现象：

- 小回归 `fb_toast_flashback` 初版失败
- 错误为：
  - `missing toast chunk 0 for toast value ... in historical toast store`

根因：

- 通过 `pg_waldump` 观察到 PG18 下这类更新事务的 WAL 顺序是：
  1. 先插入新 TOAST chunk
  2. 再删除旧 TOAST chunk
  3. 最后主表 `HOT_UPDATE`
- 因此仅维护“当前 live TOAST 状态”不够
- 当主表 old row 在 `HOT_UPDATE` 前被解析时，旧 chunk 已不在 live store

修复：

- 为 TOAST store 增加 `retired_chunks`
- TOAST delete 不再直接丢弃 chunk，而是先转存到历史区
- 主表 row image 重建时：
  - 先查 live store
  - 再查 retired store

### 问题 2：规模化 CTAS 闪回触发 backend `SIGSEGV`

现象：

- 规模化脚本第一次运行时，后端在：
  - `SELECT pg_flashback(...)`
  处崩溃
- 日志表现为 backend `signal 11`

根因判断：

- 初版 TOAST 重写采用了 `VARTAG_INDIRECT`
- 这种做法在小回归可工作，但在大结果集 + `CREATE TEMP TABLE AS SELECT ...` 路径下，生命周期更脆弱
- 放大场景下更容易演化为悬挂引用

修复：

- 不再把重建值包装成 `INDIRECT` 指针
- 直接将重建后的 varlena 作为 inline datum 放入重写 tuple
- 让 `HeapTuple` 自身持有完整值，避免后续路径依赖外部间接指针

## 测试结果

规模化 TOAST 脚本复跑通过，关键结果：

- `truth_count = 4000`
- `result_count = 4000`
- `diff_count = 0`

抽样结果：

- `id = 1`
- `id = 1500`
- `id = 2500`
- `id = 3999`

均恢复为基线值，`note = 'baseline'`。

## 结论

- 当前 `pg_flashback` 已具备基础 TOAST 历史值重建能力
- 对当前已支持的主链：
  - heap `INSERT`
  - heap `DELETE`
  - heap `UPDATE/HOT_UPDATE`
  - `HEAP2_MULTI_INSERT`
  - 主表 + TOAST 同事务变更
  已能在历史查询中给出正确的 TOAST 结果

当前仍需继续跟进的不是“能否工作”，而是：

- 将内存上限覆盖扩展到 TOAST live / retired chunk store
- 把 TOAST 深测纳入 full 模式
- 和目录清理策略一起处理自建文件增长问题
