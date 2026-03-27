# TODO.md

## 当前公开接口

- [x] 公开安装面只保留 `pg_flashback(anyelement, text)`
- [x] 调用形态固定为 `NULL::schema.table`
- [x] 不创建结果表
- [x] 不返回结果表名
- [x] 不走 `tuplestore`

## 本轮已完成

- [x] 删除旧公开入口 `pg_flashback(text, text, text)`
- [x] 删除旧结果表物化逻辑
- [x] 删除 `fb_parallel` 模块
- [x] 删除 `pg_flashback.parallel_apply_workers`
- [x] 将 `pg_flashback()` 改成直接查询型 SRF
- [x] 将 keyed apply 改为变化 key 驱动
- [x] 将 bag apply 改为 delta 驱动
- [x] 迁移回归到新接口
- [x] 迁移 deep SQL 主线到新接口
- [x] 重写核心维护文档到当前实现口径

## 当前待办

### P3 / P4 补齐

- [ ] 将 `fb_decode_insert_debug` 改成基于 `ForwardOp` 的开发期调试出口
- [ ] 增加 reverse-op / row-image 开发期调试出口

### P5 查询执行

- [x] apply 主链改为变化集驱动的小内存流式执行
- [x] 历史结果集改为直接查询型 SRF 返回
- [ ] keyed 主键变化场景补齐
- [ ] apply / replay / TOAST 交界处继续补更多宽表与极端场景验证

### P5.5 用户接口与来源模型

- [x] 安装脚本改为创建 `pg_flashback(anyelement, text)`
- [x] 删除旧的结果表名调用形态
- [x] 删除旧的 `pg_flashback(text, text, text)` 公开暴露
- [x] 迁移回归与 deep 脚本调用点
- [x] 同步 README / STATUS / TODO / PROJECT / 维护文档 / ADR
- [ ] `fb_export_undo` 对外安装与实现决策

### P5.6 内存与效率

- [x] apply 不再按当前表大小线性占用内存
- [x] 从本机会话日志恢复 bounded spill Stage A 代码基线（`fb_spool` / `fb_spill` / `FbReverseOpSource`）
- [x] 继续从本机会话日志恢复 `2026-03-26 21:10` 后半段到 `2026-03-27` 的 spill follow-up / `fb_wal` sidecar / materialized SRF 主链
- [ ] WAL 索引 / replay 主链继续向 bounded spill 演进
- [ ] deep full 的 `parallel_segment_scan on/off` 端到端验证继续推进
- [ ] 按恢复后的代码重新补跑 installcheck / deep / 冷缓存场景验证

## 恢复记录

- [x] 已将当前可编译恢复点同步回当前工作区
- [x] 已记录稳定临时树 `/tmp/pgfb_stageA_clean`
- [x] 已将恢复记录继续同步到设计 / ADR / 架构文档

### P5.7 版本兼容与文档

- [x] 将 `README.md` 改写为正式客户使用手册
- [x] 抽取 `fb_compat` 并去掉当前 `PG18` 专属构建假设
- [x] 收敛到源码/构建目标 `PG10-18`
- [x] 跑通本机 `PG12-18` 编译矩阵
- [x] 在文档中明确 `PG10/11` 为“待补环境复验”

### deep / 正确性

- [ ] 收敛 batch B / residual `missing FPI`
- [ ] 持续补齐 TOAST full 与大时间窗验证

## 完成前检查

- [x] `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
- [x] `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`
- [ ] 与本轮改动相关的 deep 验证按需补跑
