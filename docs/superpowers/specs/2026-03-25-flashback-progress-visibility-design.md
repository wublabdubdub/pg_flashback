# Flashback Progress Visibility Design

**日期**：2026-03-25

## 目标

为 `pg_flashback(text, text, text)` 增加面向 `psql` 客户端的执行进度可视化：

- 默认开启
- 用户可关闭
- 通过 `NOTICE` 流式输出
- 固定为 `9` 个阶段
- 其中阶段 `3/4/5/6/7/8/9` 输出百分比
- 其中阶段 `1/2` 只在进入阶段时输出一条 `NOTICE`

## 约束

- 不新增新的 SQL 用户入口
- 不改变 flashback 结果语义
- 首版只覆盖 `pg_flashback()` 主链，不为 `fb_export_undo()` 设计独立进度面
- 首版不做后台轮询表、视图或 shared memory 进度槽
- 首版不承诺“真实剩余时间”，只输出阶段与百分比

## 用户可见行为

新增布尔 GUC：

- `pg_flashback.show_progress`
  - 默认：`on`
  - 用户可通过 `SET` / `ALTER DATABASE` / `ALTER ROLE` 关闭

当 GUC 为 `on` 且客户端允许显示 `NOTICE` 时，执行：

```sql
SELECT pg_flashback('fb1', 'public.t1', '2026-03-25 10:00:00+08');
```

应看到类似输出：

```text
NOTICE:  [1/9] validating target relation and runtime
NOTICE:  [2/9] preparing wal scan context
NOTICE:  [3/9 20%] scanning wal and building record index
NOTICE:  [4/9 40%] replay discover round=2
NOTICE:  [5/9 100%] replay warm
NOTICE:  [6/9 60%] replay final and build forward ops
NOTICE:  [7/9 40%] building reverse ops
NOTICE:  [8/9 80%] applying reverse ops
NOTICE:  [9/9 60%] materializing result table
```

首版节流规则：

- 进入阶段时必打一次
- 百分比阶段默认只在 `0/20/40/60/80/100` 这些整 `20%` 边界输出
- 同一阶段不重复输出相同百分比

## 阶段定义

固定 `9` 段：

1. 前置校验
2. 准备 WAL 扫描上下文
3. 扫描 WAL 并建立 `RecordRef` 索引
4. replay `DISCOVER`
5. replay `WARM`
6. replay `FINAL` 并生成 `ForwardOp`
7. 构建并排序 `ReverseOp`
8. 应用 `ReverseOp` 得到历史结果
9. 创建结果表并物化写入

## 百分比口径

### 阶段 3：WAL 索引

口径：

- 分母：本轮实际访问的 segment 总数
- 分子：已打开/访问的 segment 数

原因：

- `RecordRef` 尚未构建完成前，无法用最终 `record_count` 作为稳定分母
- segment 维度在进入扫描前已经确定，最适合作为首版稳定口径

### 阶段 4：replay DISCOVER

口径：

- 分母：`index->record_count`
- 分子：当前 discover pass 已推进到的 `record_index`

补充信息：

- 额外输出 `round=N`
- 若一次 discover 即收敛，则从 `0%` 直接推进到 `100%`

### 阶段 5：replay WARM

口径：

- 分母：`index->record_count`
- 分子：当前 warm pass 已推进到的 `record_index`

说明：

- warm 只重放 backtrack 相关记录，但首版仍以“扫描到的 record 位置”表示阶段推进
- 这样能复用统一进度口径，避免额外预计算 warm 子集

### 阶段 6：replay FINAL / ForwardOp

口径：

- 分母：`index->record_count`
- 分子：当前 final pass 已推进到的 `record_index`

说明：

- `ForwardOp` 真正产出发生在 FINAL
- 因此阶段名中直接体现 “build forward ops”

### 阶段 7：ReverseOp

口径：

- 分母：`forward->count`
- 分子：已处理的 `ForwardOp` 数

说明：

- 排序成本不单独拆段
- 当循环结束后再统一补一个 `100%`

### 阶段 8：apply

口径采用两段合成：

- `0% ~ 40%`：扫描当前表并建立 keyed/bag 工作集
- `40% ~ 100%`：遍历并应用 `ReverseOp`

细则：

- 当前表扫描优先使用 `pg_class.reltuples` 作为近似分母
- 若 `reltuples <= 0` 或不可用，则阶段进入时先输出 `0%`，当前表扫描完成后直接推进到 `40%`
- `ReverseOp` 应用使用 `stream->count` 作为稳定分母，将进度映射到 `40% ~ 100%`

### 阶段 9：materialize

口径：

- 分母：最终结果总行数
- 分子：当前已写入结果表的行数

说明：

- `pg_flashback()` 在 apply 结束后已经持有完整结果集，因此首版直接复用最终结果行数作为稳定分母
- 若最终结果为空，则阶段进入后直接推进到 `100%`

## 模块设计

新增模块：

- `fb_progress`

职责：

- 维护 backend-local 的当前 progress context
- 维护 stage 编号、阶段文本、百分比阶段集合与上次已输出百分比
- 统一输出 `NOTICE`
- 实现 `20%` 节流
- 负责在成功完成或错误路径中清理当前上下文

首版不做 shared memory，也不把 progress context 挂到 queryDesc 或 executor hook。

## 集成点

### `fb_entry`

- 在 `pg_flashback()` 顶层创建/销毁 progress context
- 负责阶段 `1/2/9`
- 用 `PG_TRY/PG_CATCH` 确保报错时清空当前 progress context，避免 backend 复用时残留旧状态
- 第 `9` 段按结果表已写入行数推进百分比

### `fb_wal`

- 在 `fb_wal_build_record_index()` 接入阶段 `3`
- 在 WAL segment 打开/切换点推进百分比

### `fb_replay`

- 在 `fb_replay_execute_internal()` 接入阶段 `4/5/6`
- discover 阶段额外上报 `round`

### `fb_reverse_ops`

- 在 `fb_build_reverse_ops()` 接入阶段 `7`

### `fb_apply_*`

- 在 `fb_apply_reverse_ops()` / `fb_apply_scan_current_relation()` / keyed/bag 主循环接入阶段 `8`

## 错误模型

- progress 输出失败不应降级为静默忽略；仍沿用 PostgreSQL `NOTICE` 机制
- 若 flashback 主链报错：
  - 已输出的进度保留
  - 不再强行输出“失败阶段完成”
  - 只保证 progress context 被清理

## 回归策略

新增专门回归：

- `fb_progress`

覆盖：

- 默认开启时 `pg_flashback()` 会输出 9 段进度，其中 `3/4/5/6/7/8/9` 含百分比
- `SET pg_flashback.show_progress = off` 后不再输出这些 progress `NOTICE`

为了不污染既有回归：

- 现有会调用 `pg_flashback()` 但不关心 progress 的 SQL 回归，统一显式 `SET pg_flashback.show_progress = off`

## 非目标

- 不做 ETA / 剩余秒数估计
- 不做单条 `NOTICE` 原地刷新
- 不做 `\watch` / 轮询视图式进度
- 不做更细的 TOAST 专用子阶段
