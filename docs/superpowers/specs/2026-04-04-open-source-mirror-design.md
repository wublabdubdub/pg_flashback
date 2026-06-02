# pg_flashback 开源镜像目录设计规格

## 背景

当前仓库同时承载了三类内容：

- PostgreSQL 扩展本体源码、安装脚本、基础回归资产
- 内部研发过程文档、状态记录、设计稿、实验报告
- 构建产物、运行日志、性能采样和临时输出

用户希望保留当前内部研发仓库不动，同时在仓库内整理出一个可直接同步到 GitHub 的开源版本目录，并要求后续主仓库改动能够稳定同步到该开源目录。

## 目标

- 在仓库内新增长期维护的开源镜像目录
- 开源镜像目录只保留“作为开源 pg 插件必须存在”的最小集合
- 根仓库继续保留内部研发文档和开发过程资产
- 提供可重复执行的同步机制，避免手工维护导致目录漂移

## 非目标

- 不重构当前根仓库的研发目录结构
- 不清理根仓库中的内部资料
- 不把 `tests/deep/`、`tests/release_gate/` 纳入首版开源镜像
- 不引入软链接、`git subtree` 或历史拆分流程

## 已确认决策

### 1. 目录模型

- 新增 `open_source/pg_flashback/` 作为长期维护的开源镜像目录
- 根仓库仍是研发源头；`open_source/pg_flashback/` 不作为首改目录
- 以后对功能或文档的正式修改仍然发生在根仓库，再通过同步脚本刷新开源镜像

### 2. 同步模型

- 使用白名单同步，不做“扫描后猜测哪些文件该公开”
- 每次同步时先清理开源镜像目录中的受管内容，再按规则重建
- 同步过程必须可重复、可审计，不依赖上一次残留文件

### 3. 开源镜像白名单

开源镜像首版保留：

- `Makefile`
- `README.md`
- `pg_flashback.control`
- `include/`
- `src/`
- `sql/`
- `expected/`
- 基础回归测试最小子集
- 面向公开用户仍有价值的精简文档

开源镜像首版排除：

- `STATUS.md`
- `TODO.md`
- `PROJECT.md`
- `docs/reports/`
- `docs/superpowers/`
- `tests/deep/`
- `tests/release_gate/`
- `.worktrees/`
- `.codex/`
- `results/`
- `perf.data` / `perf.data.old`
- `cron_daily_update.log`
- `pg_flashback.so`
- `src/*.o`
- 其他日志、临时输出、构建中间产物

### 4. 文档策略

- 根仓库文档继续服务内部研发
- 开源镜像中的文档只保留公开用户需要的内容
- 需要为开源镜像补一份独立 README 和同步说明，避免把内部流程说明直接公开

## 实现方案

### 目录布局

首版采用如下结构：

```text
open_source/
  README.md
  manifest.txt
  pg_flashback/
    .gitignore
    Makefile
    README.md
    pg_flashback.control
    include/
    src/
    sql/
    expected/
    tests/
    docs/
```

其中：

- `open_source/README.md` 说明该目录的用途和同步方式
- `open_source/manifest.txt` 记录同步白名单规则
- `open_source/pg_flashback/` 是最终对外镜像根目录

### 同步脚本

新增 `scripts/sync_open_source.sh`，职责如下：

1. 创建或刷新 `open_source/pg_flashback/`
2. 清理镜像目录中的受管内容
3. 复制白名单内的文件和目录
4. 过滤明显不应公开的构建产物和日志
5. 写入镜像目录专属 `.gitignore`
6. 对同步结果执行最小完整性检查

### 基础回归测试最小子集

由于当前仓库没有单独的 `test/` 目录，基础回归 SQL 资产位于根目录的 `sql/` 与 `expected/`。首版开源镜像保留：

- `Makefile` 中 `REGRESS` 列出的基础回归 SQL 与期望输出
- 如需补充镜像内说明，则新增简短 `tests/README.md`，但不复制 deep / release gate 资产

### 验证策略

同步脚本首版至少验证：

- 必需路径存在：
  - `Makefile`
  - `README.md`
  - `pg_flashback.control`
  - `include/`
  - `src/`
  - `sql/`
  - `expected/`
- 明确排除的路径不存在：
  - `docs/superpowers`
  - `docs/reports`
  - `tests/deep`
  - `tests/release_gate`
  - `results`

## 风险与约束

### 文档双份维护风险

根仓库 README 和开源镜像 README 可能逐步漂移。首版通过同步脚本显式写入镜像 README，后续若差异扩大，再考虑拆分更细的公开文档模板。

### 回归资产位置特殊

当前基础回归 SQL 与期望文件不在 `tests/` 目录下，而是分布在 `sql/` 和 `expected/`。开源镜像不会强行重构目录，只保留现有 PGXS 兼容布局。

### 镜像不是首改源

为避免双向编辑冲突，必须明确：

- 根仓库是唯一权威源
- `open_source/pg_flashback/` 只接受同步生成，不接受手工长期改写

## 验收标准

满足以下条件视为本次整理完成：

- 仓库新增 `open_source/pg_flashback/` 目录
- 可以通过一次命令稳定重建该目录
- 镜像目录只包含公开必需部分
- 镜像目录中不含内部研发状态文档、实验报告、运行日志与构建产物
- 根仓库 `STATUS.md`、`TODO.md` 与必要架构文档已登记本次决策
