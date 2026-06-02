# pg_flashback 内嵌 ckwal 与扩展私有目录设计

## 背景

当前 `pg_flashback` 已实现 `archive_dest + pg_wal` 双来源 WAL 解析，并能在检测到 `pg_wal` 中 segment 文件名与实际内容不一致时，将其判定为 recycled / mismatched。

当前问题在于：

- 旧实现曾要求用户配置 `pg_flashback.ckwal_restore_dir`
- 旧实现曾允许通过 `pg_flashback.ckwal_command` 间接依赖外部恢复流程
- 当前目标是把这类恢复流程完全收回到扩展内部，并统一落到 `DataDir/pg_flashback/recovered_wal/`

因此需要把“缺失/错配 WAL 恢复”收回到扩展内部。

当前代码已完成其中一部分：

- 已移除用户侧 `ckwal` GUC
- 已自动初始化 `DataDir/pg_flashback/{runtime,recovered_wal,meta}`
- 已实现对可信 segment 的复制/复用
- 已实现对错配 `pg_wal` 段按页头识别实际 timeline/segno 后的转换与回灌

当前仍未完成的是：

- 对识别、校正、复制、回灌路径继续增强
- 当 `archive_dest` / `pg_wal` / `recovered_wal` 都没有可直接复制的可信源时，直接报 `WAL not complete`

## 目标

将 `/root/xman` 中 `ckwal` 的核心思路内嵌进 `pg_flashback`，形成以下产品语义：

- 用户只需要关心 `archive_dest`
- 用户不再配置任何 `ckwal` 相关参数
- `CREATE EXTENSION pg_flashback` 时自动初始化扩展私有工作目录
- 运行时若发现 `pg_wal` segment 错配或被覆盖，扩展自动校正或提取仍可用的 segment
- 恢复出的 segment 进入扩展私有目录，再继续后续 WAL 回放

## 非目标

- 不把 `/root/xman` 当作运行时依赖
- 不要求用户手工准备 `ckwal` 输出目录
- 不要求用户传入恢复命令
- 不在这一阶段改变 `archive_dest + pg_wal` 的主来源模型

## 总体方案

### 1. 扩展私有目录

在当前实例 `DataDir` 下固定创建：

```text
DataDir/pg_flashback/
  runtime/
  recovered_wal/
  meta/
```

职责划分：

- `runtime/`
  - 单次查询临时文件
  - 后续 spool / spill / lock 文件
- `recovered_wal/`
  - 内嵌 `fb_ckwal` 恢复出的 segment
  - 命名与 WAL segment 原名一致
  - 可被后续查询复用
- `meta/`
  - 扩展内部状态文件
  - 诊断输出
  - 版本标记与清理标记

### 2. 创建时机

当前代码是在扩展库加载时立即：

- 创建 `DataDir/pg_flashback/`
- 创建三个子目录
- 校验存在性与可写性

若失败，扩展初始化直接失败。

这样可以尽早暴露权限或路径问题，而不是等到第一次 flashback 查询时再失败。

### 3. WAL 来源模型调整

当前双来源模型保留：

- 主来源仍是 `archive_dest`
- `pg_wal` 只补 archive 尚未覆盖的 recent tail
- overlap 时一律优先 `archive_dest`

新增的变化是：

- 若 `pg_wal` 中某个 segment 通过文件名枚举被发现，但 header / `xlp_pageaddr` / timeline 校验失败：
  - 不再报“请用户配置 ckwal”
  - 直接进入内嵌 `fb_ckwal` 恢复流程

- 若 `archive_dest` 缺段：
  - 也先尝试内嵌 `fb_ckwal`

恢复成功后：

- 将恢复出的 segment 落到 `DataDir/pg_flashback/recovered_wal/`
- 并入当前查询的 `resolved_segments`
- 后续继续 WAL 扫描和回放

只有在三处都拿不到可用段、校正/复制仍失败时，才报真正的：

- `WAL not complete`

## 新模块划分

### `src/fb_runtime.c`

负责：

- 扩展私有目录初始化
- 目录存在性与权限校验
- 返回 `runtime/`、`recovered_wal/`、`meta/` 路径

对应头文件：

- `include/fb_runtime.h`

### `src/fb_ckwal.c`

负责：

- 根据需要恢复指定 timeline/segno 的 WAL segment
- 恢复输出统一写入 `recovered_wal/`
- 对外暴露“尝试恢复一个 segment”的接口

对应头文件：

- `include/fb_ckwal.h`

### `src/fb_wal.c`

改动：

- 删除对用户配置型 `ckwal` GUC 的依赖
- 将 `fb_try_ckwal_segment()` 从“外部恢复目录/命令包装器”改为“调用内嵌 `fb_ckwal`”

### `src/fb_guc.c`

改动：

- 删除：
  - `pg_flashback.ckwal_restore_dir`
  - `pg_flashback.ckwal_command`
- 保留：
  - `pg_flashback.archive_dest`
  - `pg_flashback.archive_dir`（兼容回退）
  - `pg_flashback.memory_limit_kb`

## 与 `/root/xman` 的关系

`/root/xman` 继续只作为参考来源。

可参考的部分：

- 错配 segment 的识别思路
- 从覆盖/混乱 WAL 中提取可恢复内容的策略
- 恢复输出组织方式

不可照搬的部分：

- 运行时依赖外部程序
- 独立工具式调用模型
- 依赖用户手工管理输出目录

## 运行时流程

一次查询的相关流程变为：

1. 用户调用 `pg_flashback()`
2. resolver 收集 `archive_dest + pg_wal`
3. 对 `pg_wal` segment 做 header / `pageaddr` / timeline 校验
4. 若发现 mismatch / recycled：
   - 调用 `fb_ckwal_restore_segment()`
5. `fb_ckwal` 将恢复出的 segment 写入 `recovered_wal/`
6. resolver 将该 segment 纳入统一 WAL 视图
7. 后续继续 checkpoint、RecordRef、BlockReplayStore、page redo

## 错误模型

新增错误分层：

- `could not initialize pg_flashback runtime directory`
  - `CREATE EXTENSION` 阶段报

- `could not recover required WAL segment`
  - 运行时自动恢复失败

- `WAL not complete`
  - 只有在 archive / pg_wal / recovered_wal 都无法提供可信 segment 时才报

## 兼容与迁移

为了不让已有测试瞬间失效，这次改造分两阶段：

### 第一阶段

- 新增 runtime 目录模块
- 新增内嵌 `fb_ckwal` 骨架
- 保留旧 `ckwal` GUC 代码路径，但改为 deprecated

### 第二阶段

- 删除旧 `ckwal` GUC
- 全部切换到内嵌恢复
- 更新 README / PROJECT / TODO / STATUS / regression

## 测试要求

至少覆盖：

- `CREATE EXTENSION` 自动创建目录成功
- 目录不可写时扩展创建失败
- overlap segment 仍优先 `archive_dest`
- `pg_wal` 错配时自动进入内嵌恢复
- 恢复成功后 flashback 成功
- 恢复失败后报 `WAL not complete`

## 实现优先级

1. 扩展私有目录初始化
2. GUC 收缩，去除用户侧 `ckwal` 参数
3. 内嵌 `fb_ckwal` 接口骨架
4. `fb_wal` 切换到内嵌恢复
5. 再移植 `/root/xman` 的核心识别/校正思路，增强已有可用段的自动恢复
