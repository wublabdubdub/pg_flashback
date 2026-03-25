# pg_flashback 设计规格

## 背景

`pg_flashback` 的目标是提供类似 Oracle Flashback Query 的只读历史查询能力，但不依赖长期维护页级历史副本，也不走“当前文件物理倒放”路线。

经过多轮方案比较，最终选定：

- 以当前表数据为逻辑基线
- 从 WAL 中抽取目标时间窗内已提交 DML
- 构建结构化 `reverse op stream`
- 在扩展内部应用反向操作得到 `target_ts` 历史结果

## 已确认约束

- 首版严格只读
- 需要同时支持历史结果查询和 undo SQL / reverse op 导出
- 无主键表按 `bag/multiset` 语义处理
- `target_ts -> query_now_ts` 归档 WAL 必须完整
- 时间窗内 DDL / rewrite 直接报错
- TOAST 首版支持，重建失败直接报错
- 开发阶段文档用中文，代码标识符用英文
- 代码与内部标识符前缀统一用 `fb`
- 公开用户 SQL 主入口保留为 `pg_flashback(...)`

## 总体架构

### Facade 层

对外暴露 SQL 接口：

- `pg_flashback(text, text, text)`：自动从数据字典生成列定义并创建速度优先的历史结果表
- `fb_export_undo(regclass, timestamptz)`：导出 undo SQL / reverse op
- `fb_version()`：返回扩展版本

### Engine 层

分成五部分：

1. relation 校验与模式选择
2. WAL 时间窗扫描与事务边界提取
3. 行像解码与 `ForwardOp`
4. `ForwardOp -> ReverseOp` 转换
5. 反向应用与结果输出

## 查询执行流程

1. 建立 `FlashbackQueryContext`
2. 校验 relation、TOAST、执行模式
3. 固定 `query_now_ts`
4. 扫描 `(target_ts, query_now_ts]` 归档 WAL
5. 提取目标表相关已提交 DML
6. 生成 `reverse op stream`
7. 以当前表数据为基线应用反向操作
8. 返回历史结果集
9. 如用户请求导出，同步渲染 undo SQL / reverse op 日志

## 用户接口约束

当前已经确认：

- 仅凭 `pg_flashback(regclass, timestamptz)` 这一类动态 `SETOF record` 函数，无法让 PostgreSQL 解析器在 `SELECT * FROM pg_flashback(...)` 时自动推断表结构
- 因此当前阶段的免列定义方案不是继续公开该 SRF，而是改为文本参数入口：
  - `pg_flashback(text, text, text)`
  - 由它从数据字典提取列定义并创建临时表

新增已拍板用户接口：

```sql
SELECT pg_flashback(
  'fb1',
  'fb_live_minute_test',
  '2026-03-23 08:09:30.676307+08'
);

SELECT * FROM fb1;
```

其设计约束为：

- 参数全部为 `text`
- 默认创建 `UNLOGGED` 结果表
- 默认仍走当前 backend 内的串行直写
- 若显式设置 `pg_flashback.parallel_apply_workers > 0`，则切到 bgworker 并行 apply/write
- 并行路径下结果表由独立 worker 事务创建并提交，不再跟随调用方事务回滚
- 目标表已存在时直接报错

最终用户接口分层也已拍板：

- 主用户入口：`pg_flashback(text, text, text)`
- `fb_flashback_materialize(regclass, timestamptz, text)` / `fb_internal_flashback(regclass, timestamptz)` 当前不对外安装
- `pg_flashback(regclass, timestamptz)` 不再作为公开用户入口

## keyed 模式

适用于有主键或稳定唯一键的表。

核心结构：

```text
RowKey -> ReverseOpList
```

执行方式：

- 扫描当前表
- 按 key 匹配反向操作链
- 依次应用 `REMOVE / REPLACE / ADD`
- 对当前已不存在、但历史上存在的行再补回

## bag 模式

适用于无主键表。

核心结构：

```text
CurrentRowBag: RowImage -> count
DeltaBag: RowImage -> delta_count
FinalRowBag: RowImage -> final_count
```

执行方式：

- 当前表扫描构建 `CurrentRowBag`
- 反向流聚合成 `DeltaBag`
- 最终合并得到 `FinalRowBag`
- 按次数展开结果

## 风险处理

以下情况直接报错，不返回部分结果：

- WAL 不完整
- 检测到 DDL / rewrite / truncate
- TOAST 值无法重建
- relation 类型不支持
- 关键元数据无法确认

## WAL 来源模型的后续修正

当前基线实现仍使用单目录 `archive_dir`。  
后续要升级为：

- `archive_dest` 作为主配置语义
- 同时判断需要的 WAL 当前位于：
  - `pg_wal`
  - `archive_dest`
  - 或两者都不完整
- segment 选择规则：
  - recent WAL 优先 `pg_wal`
  - 历史 WAL 优先 `archive_dest`
  - 两端同时存在时做一致性校验
- 对 `pg_wal` 中已被覆盖的 segment，引入缺失 WAL 恢复层
- 这一层将参考 `/root/xman` 中的 `ckwal`

## 首个可交付目标

先完成 PG18 基线最小骨架：

- 扩展可编译、可安装
- 核心接口占位完成
- 文档与任务体系稳定
- 回归测试入口存在
- 为后续实现 WAL decode core 做准备
