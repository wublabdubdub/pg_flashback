# TODO.md

## P0 基础

- [x] 初始化 git 仓库
- [x] 创建长期项目文档结构
- [x] 固化首版范围与不支持场景
- [x] 选定 `reverse op stream` 主路线
- [x] 完成最小扩展骨架可编译
- [x] 完成最小扩展安装 SQL
- [x] 完成最小回归测试通过

## P1 关系与执行模式

- [x] 实现目标 relation 校验
- [x] 实现 `keyed` / `bag` 模式选择
- [x] 覆盖 system catalog 拒绝测试
- [x] 实现 TOAST relation 定位
- [x] 实现不支持对象类型的统一报错
- [x] 增加其他 relkind 的拒绝测试

## P2 WAL 时间窗扫描

- [x] 实现归档目录配置读取
- [x] 实现 `target_ts -> query_now_ts` 时间窗扫描框架
- [x] 实现事务 commit / abort 提取
- [x] 实现 WAL 完整性检查
- [x] 实现 DDL / rewrite 风险检测
- [x] 增加开发期 `fb_scan_wal_debug()` 调试出口

## P3 RecordRef 与页级重放

- [x] 删除旧的 `fb_decode_delete_debug` 错误前提链路
- [ ] 将 `fb_decode_insert_debug` 改成基于 `ForwardOp` 的开发期调试出口
- [x] 实现 `checkpoint -> now` 的目标 relation `RecordRef` 索引层
- [x] 在索引层中记录：
  - checkpoint 锚点
  - 目标 relation heap DML refs
  - 相关事务 commit / abort
  - unsafe window 标记
- [x] 实现 `BlockReplayStore`
- [x] 实现最小 heap redo：
  - `INSERT`
  - `DELETE`
  - `UPDATE/HOT_UPDATE`
- [x] 补齐 `xid -> commit_lsn` 元信息
- [x] 处理 `INSERT + INIT_PAGE` 与缺失 FPI 的错误分支
- [x] 处理 `UPDATE` 的 old/new page 同页与跨页场景
- [x] 从重放前/后页面提取 `ForwardOp`

## P4 ForwardOp / ReverseOp
- [x] 定义 `ForwardOp` 结构：
  - [x] old row image
  - [x] new row image
  - [x] row key identity
  - [x] commit metadata
- [x] 定义 `ReverseOp` 结构：
  - [x] `REMOVE`
  - [x] `ADD`
  - [x] `REPLACE`
- [x] 从页级重放提取：
  - [x] `INSERT -> new_row`
  - [x] `DELETE -> old_row`
  - [x] `UPDATE/HOT_UPDATE -> old_row + new_row`
- [x] 构建按 `commit_lsn DESC, record_lsn DESC` 排序的 `reverse op stream`
- [ ] 增加 reverse-op / row-image 开发期调试出口

## P5 查询执行

- [x] 扩展 relation catalog，记录稳定唯一键属性列表
- [x] 实现当前表基线装载
- [x] 实现 `keyed` 模式应用算法：
  - [x] 当前行按 key 建工作集
  - [x] `REMOVE`
  - [x] `ADD`
  - [x] `REPLACE`
  - [x] 覆盖主键值变化
- [x] 实现 `bag` 模式应用算法：
  - [x] 当前结果集聚合为 `row -> count`
  - [x] `REMOVE`
  - [x] `ADD`
  - [x] `REPLACE`
- [x] 实现历史结果集 `SRF` 返回
- [x] 增加 `keyed` 模式回归测试
- [x] 增加 `bag` 模式回归测试
- [x] 增加基于数据字典自动物化临时结果表的免列定义接口

## P5.5 用户接口与 WAL 来源模型

- [x] 记录“`SETOF record` 无法在当前调用形态下自动去除 `AS t(...)`”的 PostgreSQL 约束
- [x] 提供实用接口 `fb_flashback_materialize(regclass, timestamptz, text)`
- [x] 增加 `fb_flashback_materialize` 回归测试
- [x] 记录新的文本参数用户接口：
  - [x] `fb_create_flashback_table(text, text, text)`
  - [x] 默认创建 `TEMP TABLE`
  - [x] 目标表已存在时直接报错
- [x] 实现 `fb_create_flashback_table(text, text, text)`
- [x] 增加 `fb_create_flashback_table` 回归测试
- [ ] 重新设计最终用户接口，评估是否还需要保留 `pg_flashback(regclass, timestamptz)` 作为直接用户入口
- [ ] 用文档固定 `archive_dir` 只是基线开发配置，不是最终产品模型
- [ ] 设计 `archive_dest + pg_wal` 的 WAL 来源解析层
- [ ] 设计 segment 选择规则：
  - [ ] recent WAL 优先从 `pg_wal` 读取
  - [ ] 历史 WAL 从 `archive_dest` 读取
  - [ ] 两端都存在时的优先级与一致性判断
- [ ] 设计 `pg_wal` 中 segment 已被覆盖时的恢复策略
- [ ] 参考 `/root/xman` 的 `ckwal` 设计缺失 WAL 复原流程

## P5.6 效率与内存模型

- [x] 检查当前 FPI 是否在 replay 热路径中频繁文件回读
- [x] 确认当前实现为：
  - [x] WAL 扫描阶段顺序读 archive/pg_wal
  - [x] FPI 在 `RecordRef` 建立时复制进内存
  - [x] replay 阶段通过内存态 `BlockReplayStore` 读写 page state
- [x] 记录查询级内存硬上限策略：
  - [x] 新增 `pg_flashback.memory_limit_kb`
  - [x] 先跟踪并限制 `RecordRef` / FPI / block data / main data / `BlockReplayStore`
  - [x] 命中上限时直接报错
- [x] 实现热路径内存统计
- [x] 实现查询级内存上限校验
- [x] 增加低上限触发失败的回归测试
- [ ] 设计大时间窗下的 spool / spill / eviction 策略
- [ ] 将内存上限覆盖扩展到 `ForwardOp` / `ReverseOp` / apply 工作集

## P6 导出能力

- [ ] 实现 reverse op 调试导出
- [ ] 实现 undo SQL 渲染
- [ ] 实现导出接口

## P7 正确性与兼容

- [ ] 覆盖 TOAST 场景
- [ ] 覆盖主键更新场景
- [ ] 覆盖无主键重复行场景
- [ ] 建立 PG18 基线测试集
- [ ] 从 PG18 抽取版本兼容层
- [ ] 扩展到 PG10-18
