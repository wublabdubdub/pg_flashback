# TODO.md

## 文档校准说明

- 以下勾选以当前主线代码和安装 SQL 为准。
- 当前对外安装面只保留 `pg_flashback(text, text, text)`。
- `fb_flashback_materialize()` / `fb_internal_flashback()` 相关旧条目保留在 TODO 中，仅表示“曾有设计或仍待决定/恢复”，不表示当前代码仍已提供该接口。

## 文档与交接

- [x] 产出源码级维护手册：
  - [x] 解释主链调用顺序、关键结构体和模块边界
  - [x] 解释 WAL 扫描 / replay / TOAST / apply 的真实实现口径
- [x] 产出调试与验证手册：
  - [x] 覆盖常见错误、排障步骤、回归入口与 deep 入口
  - [x] 覆盖本机 PG18 手工验证、目录副作用与运行时产物
- [x] 在 README / 架构总览中补充维护文档入口
- [x] 产出“代码优先”的核心入口导读：
  - [x] 直接展示 `fb_entry` / `fb_wal` / `fb_replay` / `fb_reverse_ops` / `fb_apply_*` / `fb_toast` 的关键入口代码
  - [x] 为每段代码补最少但够用的源码级讲解
  - [x] 扩写“用户参数进入 -> 主链执行 -> 结果返回”的整链路
  - [x] 对选取的核心代码逐行加注释

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

- [x] 将公开用户入口从 `fb_create_flashback_table(text, text, text)` 迁移为 `pg_flashback(text, text, text)`
  - [x] 安装脚本改为创建 `pg_flashback(text, text, text)`
  - [x] 删除旧的 `fb_create_flashback_table(text, text, text)` 公开暴露
  - [x] 迁移回归与 deep 脚本调用点
  - [x] 同步 README / STATUS / TODO / PROJECT / ADR / 设计与报告文档
- [x] 记录“`SETOF record` 无法在当前调用形态下自动去除 `AS t(...)`”的 PostgreSQL 约束
- [ ] 是否恢复中间层 `fb_flashback_materialize(regclass, timestamptz, text)` 需要按当前产品面重新决策
- [ ] 若恢复 `fb_flashback_materialize`，补回安装 SQL 与回归测试
- [x] 记录新的文本参数用户接口：
  - [x] `pg_flashback(text, text, text)`
  - [x] 默认创建结果表
  - [x] 目标表已存在时直接报错
- [x] 实现 `pg_flashback(text, text, text)`
- [x] 增加 `pg_flashback` 回归测试
- [x] `pg_flashback` 输出 WAL 诊断 info：
  - 当前时间对应的 LSN
  - 该 LSN 所在 WAL 段
  - 起始 WAL 段
  - 终点 WAL 段
- [x] `pg_flashback` 的 WAL 诊断 info 改为多行 `NOTICE`
  - [x] 在确定 segment 范围后立即输出
  - [x] 不再等结果表创建完成后再输出
- [x] 重新设计最终用户接口，评估是否还需要保留 `pg_flashback(regclass, timestamptz)` 作为直接用户入口
- [x] 删除旧的 `pg_flashback(regclass, timestamptz)` 公开暴露
  - [x] 安装脚本中不再创建用户可直接执行的 `pg_flashback(regclass, timestamptz)` SQL 接口
  - [x] 正式入口当前已收敛到 `pg_flashback(text, text, text)`
  - [x] 迁移旧签名 `pg_flashback()` 的回归用例
- [x] 用文档固定 `archive_dir` 只是基线开发配置，不是最终产品模型
- [x] 设计 `archive_dest + pg_wal` 的 WAL 来源解析层
- [x] 实现最小 `archive_dest + pg_wal` 双来源 segment 解析
- [x] 设计 segment 选择规则：
  - [x] `archive_dest` 缺失时才从 `pg_wal` 读取
  - [x] 两端都存在时一律优先 `archive_dest`
  - [x] `pg_wal` 只承接 archive 尚未覆盖的 recent tail
- [x] 设计 `pg_wal` 中 segment 已被覆盖时的恢复策略
- [x] 参考 `/root/xman` 的 `ckwal` 设计缺失 WAL 复原流程
- [x] 实现 `pg_wal` segment 名称/头部/pageaddr 错配检测
- [x] 实现 overlap 一律优先 `archive_dest`
- [x] 接入 `ckwal` 恢复接口层与错误模型
- [x] 将外露 `ckwal` GUC 收缩为扩展内部实现：
  - [x] 移除 `pg_flashback.ckwal_restore_dir`
  - [x] 移除 `pg_flashback.ckwal_command`
  - [x] 在扩展库加载时自动初始化 `DataDir/pg_flashback/`
  - [x] 固定子目录：`runtime/`、`recovered_wal/`、`meta/`
  - [x] 将 `fb_try_ckwal_segment()` 改为调用内嵌 `fb_ckwal`
  - [x] 将恢复后的 segment 固定写入 `recovered_wal/`
  - [x] 当 `pg_wal` 段发生错配/覆盖时，将其转换为 `ckwal` 结果并立即回灌到本轮 segment 候选集合
  - [x] 让 resolver 持续消费 `archive_dest` / `pg_wal` / `recovered_wal` 三路候选，直到拼出连续可扫描 segment 集
  - [x] 为恢复引擎增加可复用的坏段识别、转换、复制与落盘流程
  - [x] 将“目录契约已完成”与“恢复引擎已完成”分开验收
  - [x] 固定能力边界：`fb_ckwal` 不负责凭空重建真正缺失的 segment
  - [x] 固定错误模型：`archive_dest` / `pg_wal` / `recovered_wal` 都没有可用段时直接报 `WAL not complete`

## P5.6 效率与内存模型

- [x] 检查当前 FPI 是否在 replay 热路径中频繁文件回读
- [x] 确认当前实现为：
  - [x] WAL 扫描阶段顺序读 archive/pg_wal
  - [x] FPI 在 `RecordRef` 建立时复制进内存
  - [x] replay 阶段通过内存态 `BlockReplayStore` 读写 page state
- [x] 设计并实现 WAL 扫描阶段的并行扫描方案，在不破坏时间窗与顺序语义的前提下缩短长时间窗扫描耗时
  - [x] 新增用户显式开关；默认关闭，只有参数设置为 `on` 才启用
  - [x] 首版只并行做 segment 级命中预筛选
  - [x] 最终 `XLogReader` 解析与顺序归并继续保持单线程
  - [x] 为 `on/off` 行为补回归和调试摘要
- [x] 完成 `pg_flashback.parallel_segment_scan` 的三组速度对比
  - [x] `small / medium / large` 三组时间窗各自对比 `off/on`
  - [x] 记录 wall clock、命中 segment 数与相对加速比
  - [x] 输出正式测试报告
  - [x] 结论：当前首版并行预筛选显著慢于顺序扫描，暂不应作为性能优化默认推荐
- [x] 修正 `pg_flashback.parallel_segment_scan` 的性能回归
  - [x] 替换现有“分块 + 逐字节匹配”的 segment 预筛选热路径
  - [x] 将正式扫描改为只访问命中窗口，而不是对 miss segment 再做一次细扫
  - [x] 为 segment 预筛选增加 file cache + backend cache
  - [x] 在同一套 `small / medium / large` 基准上重新复测
  - [x] 目标：`on` 明确快于 `off`
- [x] 将命中窗口跳段与 prefilter cache 扩展到 `fb_wal_build_record_index()`
  - [x] 真实 flashback 的 `RecordRef/index` 路径已改走命中窗口访问
  - [x] `parallel_segment_scan=on` 不再额外做一遍 prefilter 后整窗细扫
- [x] 收口产品面到真实 flashback 主路径
  - [x] 删除 `fb_scan_wal_debug`
  - [x] 删除 `fb_recordref_debug`
  - [x] 删除 `fb_replay_debug`
  - [x] 删除 `fb_decode_insert_debug`
  - [x] 删除相关 SQL 安装、回归和主链测试暴露
  - [x] 删除对外 `AS t(...)` 形态及相关文档/测试暴露
  - [x] 对外只保留真实 flashback 用户入口
- [ ] 完成主表 deep `--full` 的 `parallel_segment_scan on/off` 速度对比
  - [x] 处理 2026-03-24 的 archive 容量 blocker：用户已将 archive 迁到 `/walstorage/18waldata`
  - [ ] 处理 2026-03-24 新暴露的窗口 blocker：现成 `10M + 7.7GB WAL` 单表夹具被判定为 `unsafe=storage_change`
  - [ ] 先修掉 full deep 当前 `memory_limit_kb` 越界导致的假 blocker
  - [x] 将 full runner 改成“baseline 只导入一次 + 单份快照恢复”
  - [x] 创建 baseline 快照前检查磁盘空间是否足够容纳一份 PGDATA 副本
  - [x] 为 full runner 增加状态文件，支持断连后从未完成 batch 续跑
  - [ ] `off` 跑一轮新的主表 full，并记录总耗时与各 batch 耗时
  - [ ] `on` 跑一轮新的主表 full，并记录总耗时与各 batch 耗时
  - [ ] 持续监测 `/isoTest`、`/`、`/tmp` 文件系统使用率
  - [ ] 使用后台日志/轮询方式执行，避免终端失去连接
  - [ ] 输出正式结论与残余风险
- [x] 完成一轮放宽口径的主表并行扫描验证
  - [x] 复用现成 `fb_parallel_speed_final.bench_main` 多 WAL 窗口
  - [x] 使用可跑通的调试扫描入口验证 `parallel_segment_scan off/on`
  - [x] 连续执行 `3` 轮并给出效果结论
  - [x] 输出独立报告，明确它不是“真实 flashback 入口”基准
- [ ] 完成一轮真实 flashback 的安全单表并行验证
  - [ ] 构造不含 `storage_change` 的单表多 WAL 时间窗
  - [x] 处理当前新确认的实现级 blocker：即使目标表在窗口中不变、仅由独立噪声表制造多 WAL，真实 flashback 主路径仍会误报 `storage_change`
  - [x] 修正 unsafe 判定边界：不要把 checkpoint anchor 回看区间里、发生在 `target_ts` 之前的 relation `Standby LOCK / Storage CREATE` 直接算成当前 flashback 窗口的 `storage_change`
  - [ ] 用当前真实 flashback 主入口实际执行 `off/on` 各 `3` 轮
  - [ ] 产出可复现的手工执行场景

## P5.7 客户端进度可视化

- [x] 为 `pg_flashback()` 设计并实现 `psql` 可见的流式 progress `NOTICE`
- [x] 新增 `pg_flashback.show_progress`
  - [x] 默认 `on`
  - [x] 用户可显式 `off`
- [x] 固定 `9` 段执行模型
  - [x] 阶段 `1/2` 只输出阶段进入 `NOTICE`
  - [x] 阶段 `3/4/5/6/7/8/9` 输出百分比
  - [x] 百分比输出统一按 `20%` 桶节流
- [x] 新增 `fb_progress` 模块并在架构总览登记职责
- [x] 为 stage `3` 接入 WAL 索引百分比
- [x] 为 stage `4/5/6` 接入 replay `DISCOVER/WARM/FINAL` 百分比
- [x] 为 stage `7` 接入 `ReverseOp` 构建百分比
- [x] 为 stage `8` 接入 apply 百分比
- [x] 为 stage `9` 接入结果物化百分比
- [x] 新增 `fb_progress` 回归
- [x] 让非 progress 回归默认显式关闭 `show_progress`，避免污染现有 expected

## P5.8 速度优先的结果落表重构

- [x] 去掉 `tuplestore -> TEMP TABLE` 二次物化
- [x] 将默认结果表改为 `UNLOGGED heap`
- [x] 在 apply 层新增统一 result sink
- [x] 将 stage `9` 改为 apply 末尾直写结果表
- [x] 为后续并行 apply/write 预留 sink 边界
- [x] 为 `UNLOGGED` 结果表语义补回归与文档

## P5.9 真并行 apply/write

- [x] 确认 `ParallelContext` / parallel mode 不能直接承担结果表写入
- [x] 确认普通 bgworker 不能直接写 leader 当前事务里新建但未提交的结果表
- [x] 将真并行方案收敛为“默认串行，显式开启并行”
- [x] 写并行 apply/write 设计文档
- [x] 写并行 apply/write 实现计划
- [x] 新增 `pg_flashback.parallel_apply_workers`
- [x] 新增 `fb_parallel` 模块，负责 helper/apply worker 生命周期与协议
- [x] 实现自主事务结果表创建 helper
- [x] 实现 keyed/bag primitive item 的分区派发
- [x] 实现 apply worker 本地 keyed/bag 状态机构建
- [x] 实现 worker 并行写 `UNLOGGED` 结果表
- [x] 实现 stage `8/9` 的并行进度聚合
- [x] 增加并行 apply/write 回归
- [x] 记录并行路径结果表生命周期独立于调用方事务
- [ ] 为并行路径补速度基准与故障注入清理验证

## P5.8 速度优先的结果落表重构

- [x] 去掉 `tuplestore -> TEMP TABLE` 的二次物化
- [x] 将结果表默认改为速度优先的 `UNLOGGED heap`
- [x] 保持 `pg_flashback(text, text, text)` 用户入口不变
- [x] 将 stage `9` 改成 apply 末尾直接写结果表，而不是入口层二次搬运
- [x] 为后续 background worker 并行 apply/write 预留 sink / 分区边界
- [x] 为结果表持久化语义变化补回归与文档
- [x] 记录查询级内存硬上限策略：
  - [x] 新增 `pg_flashback.memory_limit_kb`
  - [x] 先跟踪并限制 `RecordRef` / FPI / block data / main data / `BlockReplayStore`
  - [x] 命中上限时直接报错
- [x] 实现热路径内存统计
- [x] 实现查询级内存上限校验
- [x] 增加低上限触发失败的回归测试
- [ ] 设计大时间窗下的 spool / spill / eviction 策略
- [x] 按已拍板顺序推进内存优化：
  - [x] 第一优先级：将 `RecordRef` 的 `main_data / block data / FPI image` 改为 arena/顺序分配，减少热路径分散复制
  - [x] 第二优先级：将 TOAST `retired_chunks` 迁到 `runtime/` 冷数据落盘，live chunk 继续保留内存热路径
  - [x] 第三优先级：去掉 replay 到 `ForwardOp` 之间多余的 `heap_copytuple`
- [x] 复跑 pilot `massif`，对比本轮内存优化前后的峰值与热点分布
  - 新样本：`/tmp/pg_flashback_valgrind/massif_toast_pilot_after_memfix.out`
  - 旧样本：`/tmp/pg_flashback_valgrind/massif_toast_pilot.out`
  - 结果：arena / spill / 去重 tuple copy 已命中真实热路径，但端到端峰值未降，仍需继续压缩 `ForwardOp/ReverseOp/apply` 与 TOAST live store
- [x] 将内存上限覆盖扩展到 `ForwardOp` / `ReverseOp` / apply 工作集
  - [x] TOAST full 曾实测在 flashback 阶段触发 backend OOM；当前虽已连续两次 full 无 blocker，但这部分仍需补齐内存跟踪/限制
- [x] 为主入口补一条“`RecordRef` 已过但 apply 仍应受限”的回归
- [x] keyed/bag apply 去掉不必要的 tuple/identity 复制
  - [x] keyed 模式不再保留无用的 `row_identity`
  - [x] bag 模式不再保留无用的 `key_identity`
  - [x] apply 载入当前表与 reverse stream 时尽量复用现有 tuple/identity，而不是再次 `heap_copytuple` / `pstrdup`
- [x] 基于第二轮收敛再次复跑 pilot `massif`
  - [x] 新样本：`/tmp/pg_flashback_valgrind/massif_toast_pilot_after_applyfix.out`
  - [x] 对比样本：`/tmp/pg_flashback_valgrind/massif_toast_pilot_after_memfix.out`
  - [x] 结果：峰值从约 `1.769GB` 下降到约 `1.231GB`；`fb_row_image_set_identities()` 与 `fb_keyed_upsert_row()` 已退出主要热点
- [ ] 将内存上限覆盖扩展到 TOAST live / retired chunk store
  - [ ] 当前 TOAST full 已连续两次通过，但 TOAST 相关内存增长仍未纳入硬上限或 spool
- [x] 使用 `valgrind` / `massif` 对当前 flashback 查询做内存分析
  - [x] 区分真实泄漏、长生命周期 MemoryContext 占用与峰值工作集
  - [x] 输出可复现的分析报告与命令
  - [x] 根据证据给出内存优化方案并排序

## P6 导出能力

- [ ] `fb_export_undo` 明确放到当前主线最后实现
- [ ] 实现 reverse op 调试导出
- [ ] 实现 undo SQL 渲染
- [ ] 实现导出接口

## P7 正确性与兼容

- [x] 覆盖 TOAST 场景
- [x] 实现 TOAST 历史值重建主链：
  - [x] 将 TOAST relation 记录纳入同一套页级重放路径
  - [x] 为 TOAST chunk 建立历史 store
  - [x] 主表行像构造时对 external toast pointer 做读取重定向
  - [x] 用历史 TOAST store 将 `old_row/new_row` 物化为 inline 值
  - [x] 缺失 chunk 时给出明确错误，而不是回退到错误数据
- [x] 为 TOAST 历史值重建增加回归测试
- [x] 完成规模化 TOAST 测试：
  - [x] 构造带大字段、多轮 `INSERT/UPDATE/DELETE` 的生产化场景
  - [x] 记录 TOAST 历史问题到测试报告
  - [x] 对发现的问题逐项修复并回归
- [x] 将 TOAST 深测扩展到 full 模式
  - [x] 以“连续两次 TOAST 测试都没有 blocker”为当前阶段收口门槛
  - [x] 为 `80_toast_scale.sql` 增加 pilot/full 参数化规模
  - [x] 增加 `tests/deep/bin/run_toast_scale.sh`
  - [x] 完成 TOAST full 深测执行
  - [x] 产出 TOAST full 测试报告
  - [x] 解决 TOAST full 当前 `missing FPI` blocker
    - [x] 实现共享的按-block 更早 FPI 回溯
    - [x] 一次回扫同时服务多个缺页基线 block，而不是逐 block 单独回扫
    - [x] 让主表与 TOAST relation 共用同一套回溯与补锚逻辑
    - [x] 将补锚结果回灌到 replay，避免同一查询内重复回溯同一 block
    - [x] 继续扩大 shared backtracking 的收敛范围，解决 TOAST full 中残留的 `missing FPI`
  - [x] 解决 TOAST full 当前 flashback 阶段 OOM blocker
    - [x] 复现 full 场景并确认 first blocker 不再是 `missing FPI`
    - [x] 复现“未变更 live TOAST 列在 finalize 时缺 chunk”的正确性问题
    - [x] 增加小回归覆盖“更新一列 TOAST，另一列 TOAST 保持不变”
    - [x] 修复该正确性问题
    - [x] 连续两次复跑 fresh full，确认当前 head 不再出现该 blocker
    - [ ] 继续为 `ForwardOp/ReverseOp/apply` / TOAST 内存增长补齐硬上限与收敛手段
- [ ] 设计并实现自建目录清理逻辑：
  - [ ] `runtime/` 中查询级 spool / 临时文件 / 调试产物必须在成功、失败、取消三种路径下都有清理规则
  - [ ] `recovered_wal/` 中自动校正或复制出的 segment 必须有保留、复用、淘汰规则，防止无限增长
  - [ ] `meta/` 中状态文件、诊断文件、锁文件必须有生命周期和清理规则
  - [ ] 所有会导致 `DataDir/pg_flashback/` 增长的文件类型都要纳入统一清理策略
- [x] 记录当前 TOAST 深测的运维约束：
  - [x] 每次完成 TOAST 测试后，必须手动清空归档目录 `/isoTest/18waldata`
- [ ] 覆盖主键更新场景
- [ ] 覆盖无主键重复行场景
- [ ] 将当前“target 前最近 checkpoint”锚点策略按 block 升级为“共享的更早可恢复 FPI 回溯”
  - [x] 先收集本轮 replay 中全部 `missing FPI` block
  - [x] 共享回扫一次更早 WAL 记录，为待补 block 集查找可用 `FPI/INIT_PAGE`
  - [ ] 对无法找到可恢复基线的 block 继续明确报错，不做 best-effort
  - [ ] 让 shared backtracking 在 full 场景下持续迭代直至 discovery/final 残余 block 收敛
  - [x] 对 residual `missing FPI` block 做人工 `pg_waldump` 核查：
    - [x] 复现 full 场景并固定具体 `checkpoint`、`segno`、`blkno`
    - [x] 手动检查 `checkpoint -> failing record` 之间是否存在可复用 `FPI/INIT_PAGE`
    - [x] 若存在基线，解释为何当前 `RecordRef/backtracking` 未命中
    - [x] 若不存在基线，确认当前“无-FPI delete 前应有可恢复页基线”的判断边界
  - [ ] 将 `RM_XLOG_ID` 的目标 relation `FPI/FPI_FOR_HINT` 纳入 `RecordRef` 与 shared backtracking：
    - [x] 覆盖 `XLOG_FPI`
    - [x] 覆盖 `XLOG_FPI_FOR_HINT`
    - [x] 明确这类记录只建立页基线并推进页状态，不直接生成 `ForwardOp`
  - [ ] 处理接入 `RM_XLOG_ID` 之后暴露出的新 replay blocker：
    - [ ] 复现 `WARNING: will not overwrite a used ItemId`
    - [ ] 定位是 image-applied insert、warm/final 重放边界，还是页状态推进过度
    - [ ] 修复 `failed to replay heap insert`
- [ ] 按 PG18 源码补齐所有可能携带 main-fork block image / main-data 的 heap WAL 记录接入：
  - [x] `XLOG_HEAP_CONFIRM`
  - [x] `XLOG_HEAP_INPLACE`
  - [x] `XLOG_HEAP2_VISIBLE`
  - [x] `XLOG_HEAP2_MULTI_INSERT`
  - [x] `XLOG_HEAP2_LOCK_UPDATED`
  - [x] 重新审视 `XLOG_HEAP_LOCK` / `XLOG_HEAP2_PRUNE_*`，将当前最小 no-op 升级为对后续页状态安全的最小重放
- [x] 基于 PG18 源码整理 WAL record 覆盖矩阵
  - [x] 已妥善处理并产出 `ForwardOp`：
  - [x] `XLOG_HEAP_INSERT`
  - [ ] 为 relation 级 `standby lock / relfilenode change` 补专项 unsafe 检测与回归，覆盖 `TRUNCATE/VACUUM FULL` 一类 PG18 路径
    - [x] `XLOG_HEAP_DELETE`（不含 `XLH_DELETE_IS_SUPER`）
    - [x] `XLOG_HEAP_UPDATE`
    - [x] `XLOG_HEAP_HOT_UPDATE`
    - [x] `XLOG_HEAP2_MULTI_INSERT`
  - [x] 已处理，但职责限定为“推进页状态 / 建立页基线，不生成 `ForwardOp`”：
    - [x] `XLOG_HEAP_CONFIRM`
    - [x] `XLOG_HEAP_INPLACE`
    - [x] `XLOG_HEAP2_VISIBLE`
    - [x] `XLOG_HEAP2_LOCK_UPDATED`
    - [x] `XLOG_FPI`
    - [x] `XLOG_FPI_FOR_HINT`
  - [x] 已按产品边界显式拒绝 / 标 unsafe：
    - [x] `XLOG_HEAP_TRUNCATE`
    - [x] `XLOG_HEAP2_REWRITE`
    - [x] `RM_SMGR` create / truncate 触发 storage change
  - [x] 当前只做最小处理，已增强为对页状态安全的最小 replay：
    - [x] `XLOG_HEAP_LOCK`
    - [x] `XLOG_HEAP2_PRUNE_ON_ACCESS`
    - [x] `XLOG_HEAP2_PRUNE_VACUUM_SCAN`
    - [x] `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP`
  - [x] speculative 内部记录处理收口：
    - [x] `XLOG_HEAP_DELETE` with `XLH_DELETE_IS_SUPER` 纳入 page replay
    - [x] `XLH_INSERT_IS_SPECULATIVE` / `XLH_DELETE_IS_SUPER` 不得产出用户可见 `ForwardOp`
  - [x] `XLOG_HEAP2_NEW_CID`
    - [x] 基于 PG18 原生 `heap2_redo()` 明确归类为 redo no-op
    - [x] 不作为 page replay 缺口继续跟踪
- [ ] 为“可能带 image 但当前未做完整逻辑提取”的记录增加明确分类：
  - [x] 仅建立页基线并推进页状态
  - [x] 需要完整 page mutation，但不生成 `ForwardOp`
  - [x] 需要生成 `ForwardOp`
- [ ] 建立 PG18 基线测试集
- [ ] 从 PG18 抽取版本兼容层
- [ ] 扩展到 PG10-18

## P8 深度生产化测试

- [x] 完成深度生产化测试设计
- [x] 完成深度生产化测试实施计划
- [x] 建立 `tests/deep/` 目录结构
- [x] 完成环境 bootstrap 脚本
- [x] 完成大表 schema 与真值表 schema
- [x] 完成 500 万级基线装载脚本
- [x] 完成 truth snapshot 捕获脚本
- [x] 完成批次 A：keyed 大表高压 DML（pilot）
- [ ] 完成批次 B：事务边界与回滚
  - 当前 pilot blocker：
    - `target_ts` 前最近 checkpoint 之后，某些 block 首条相关记录不带 `FPI/INIT`
    - 已确认真实样例：`PRUNE_VACUUM_CLEANUP` / `VISIBLE` -> `UPDATE/HOT_UPDATE`
    - 见 `docs/reports/2026-03-23-deep-pilot-report.md`
  - 处理方案分层：
    - [x] 第一层：补齐可能携带 main-fork image 的 heap/heap2 记录进入 `RecordRef` 与 replay，避免漏掉可用页基线
    - [ ] 第二层：对 `missing FPI` 增加更细诊断，区分“锚点不足”与“record 集合漏接”
    - [ ] 第三层：实现共享的按-block 更早 FPI 回溯
      - [x] 不逐 block 单独回扫
      - [x] 主表与 TOAST 共用一套补锚逻辑
      - [ ] 回扫后复跑 batch B / TOAST full
      - [ ] 解决当前 residual `heap_delete` `missing FPI`
        - [x] 已用 `pg_waldump` 证明最新复现不是“无基线”，而是漏掉了 `RM_XLOG_ID` 的 `FPI_FOR_HINT`
- [x] 完成批次 C：bag 重复值大表（pilot）
- [x] 完成批次 D：WAL 来源跨目录（pilot）
- [x] 完成批次 E：长时间窗与内存压力（pilot）
- [ ] 完成 pilot 模式联调
- [x] 产出 pilot 阶段报告
- [x] 重定义 `full` 模式深测
  - [x] 删除旧的超长 `full` 语义
  - [x] 将新 `full` 固定为“单轮 WAL 预算约 `10GB`”
  - [x] 保持主表 `keyed/bag` 双表模型与 TOAST 表模型不变
  - [x] 让 deep 脚本在运行中实时监测 `/isoTest`、`/`、`/tmp` 使用率
  - [x] 任一文件系统使用率超过 `85%` 时，自动清理 deep 产物与 `/isoTest/18waldata` 并 fresh 重跑当前轮次
  - [x] 每轮结束后自动清理 deep 产物与 `/isoTest/18waldata`
- [ ] 以新 `full` 为标准完成连续两轮主表 deep full 通过
- [ ] 以新 `full` 为标准完成连续两轮 TOAST full 通过
- [ ] 产出最终深测报告
- [x] 将 `tests/deep/` 从已移除的 `ckwal` GUC 模型迁移到当前内嵌恢复模型
- [x] 新增 TOAST 规模化深测脚本
- [x] 产出 TOAST 规模化测试报告
