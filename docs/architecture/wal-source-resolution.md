# WAL 来源解析 / WAL Source Resolution

[中文](#中文) | [English](#english)

## 中文

本文只讲当前代码里的 WAL resolver，不再沿用“用户手填一个目录，所有 WAL 都从那里读”的旧口径。

相关代码：

- `src/fb_guc.c`
- `src/fb_wal.c`
- `src/fb_ckwal.c`
- `src/fb_runtime.c`

## 一、当前真实来源模型

当前 flashback 查询会综合三类实际来源：

1. archive 目录
2. `pg_wal`
3. `DataDir/pg_flashback/recovered_wal`

其中 archive 目录本身又有三种解析方式：

1. 显式 `pg_flashback.archive_dest`
2. 兼容回退 `pg_flashback.archive_dir`
3. 未显式设置时，从 PostgreSQL `archive_command` 自动发现

最终不是“用户决定读哪个目录”，而是 resolver 统一决定每个 segment 应该从哪里来。

## 二、优先级顺序

### 配置优先级

在 `src/fb_guc.c` 中，archive 目录解析顺序固定为：

1. `pg_flashback.archive_dest`
2. `pg_flashback.archive_dir`
3. `archive_command` 自动发现

只要上层已有明确值，下层就不再参与。

### 读取优先级

同一个 segment 如果 archive 和 `pg_wal` 都存在，当前规则是：

- 优先 archive
- `pg_wal` 只承接 archive 尚未覆盖的 recent tail

也就是：

- 不按“谁更新”来猜
- 不按“pg_wal 看起来更新鲜”来抢优先级
- archive 一旦存在，就默认是更可信的长期来源

## 三、`archive_command` 自动发现的边界

当前实现只识别可安全解析的本地模式。

### 已支持

1. `cp %p /path/%f`
2. `test ! -f /path/%f && cp %p /path/%f`
3. 本地 `pg_probackup archive-push -B backup_dir --instance instance_name ...`

第三种会自动映射到：

```text
backup_dir/wal/instance_name
```

### 明确不做

1. 解析复杂 shell 脚本
2. 推断远程归档
3. 推断 `archive_library`
4. 猜测任意自定义 wrapper 命令

当 `archive_library` 非空，或 `archive_command` 不属于当前可识别模式时，直接要求用户显式设置 `pg_flashback.archive_dest`。

## 四、`pg_wal` 为什么不能单独当最终来源

因为它只可靠覆盖 recent tail。

单独使用 `pg_wal` 的问题包括：

1. 老 segment 可能已被 recycle
2. 时间窗较大时，历史段通常只在 archive 里
3. 文件名和真实页头可能错配

因此当前模型里：

- `pg_wal` 是参与者
- 但不是唯一真相来源

## 五、resolver 当前怎么判定 segment 是否可信

### 1. 常规检查

resolver 会检查：

- 是否是标准 WAL 文件名
- 文件是否存在且是常规文件
- 页头中的 timeline / segno / segsize 是否可解析

### 2. `pg_wal` 错配检查

这是当前实现里的一个关键点。

对于来自 `pg_wal` 的段，系统不仅看文件名，还会读首个 long header。

如果：

- 文件名表示的 `timeline + segno`
- 与页头真实 `xlp_pageaddr` 推导出来的 segment

不一致，就认为这个文件可能已经 recycled / 覆盖 / 命名与内容错配。

这类文件不会直接拿去 flashback。

## 六、错配段如何处理

当前不是简单丢弃，而是进入内嵌 `fb_ckwal` 处理。

流程：

1. 读出页头真实 `timeline + segno`
2. 在 `DataDir/pg_flashback/recovered_wal/` 下按真实名字重新物化
3. 让 resolver 在同一轮中重新消费这个恢复结果

对应代码：

- `src/fb_ckwal.c` 中的 `fb_ckwal_materialize_segment()`

这就是“先转换为 `recovered_wal/<actual-segname>` 再回灌”的当前代码实现。

## 七、`fb_ckwal` 当前到底做什么

当前内嵌 `fb_ckwal` 不是外部恢复工具壳子，只做最小闭环。

当前收紧后的目标口径：

1. archive 有目标 segment 时，resolver 直接消费 archive，不再复制到 `recovered_wal`
2. `recovered_wal` 只复用“历史上已经恢复好的真实 segname 文件”
3. 只有 `pg_wal` 文件名与真实内容不一致、但页头可读，且 archive 无法提供对应真实 segment 时，才按真实 segno 物化到 `recovered_wal`
4. archive、`pg_wal` 修复路径、`recovered_wal` 复用都不可用时，失败

它当前不做：

- 从备份系统远程拉取
- 调外部命令执行复杂恢复
- 维护单独的产品级恢复协议

## 八、运行时目录在来源模型里的作用

`src/fb_runtime.c` 会在扩展加载时确保这几个目录存在：

- `pg_flashback/runtime`
- `pg_flashback/recovered_wal`
- `pg_flashback/meta`

它们分别承担：

- query 级 spool
- 恢复出来的可信 WAL 段
- sidecar / metadata

当前新增补充：

- `meta/summary`
  - 承载 segment 通用 summary
  - 由后台预建服务与查询侧共同消费
  - 当前与 archive / `pg_wal` / `recovered_wal` 解析结果保持同一份 segment 身份口径

所以来源解析并不是“只在用户配置目录里找”，而是自带扩展私有工作区。

## 九、当前 resolver 与 scan 的配合关系

resolver 的职责不是直接读 record，而是准备好：

- 连续
- 可信
- 可覆盖目标时间窗

的 segment 集合。

后续 `fb_wal_prepare_scan_context()` 会把这些结果写入 `FbWalScanContext`，再交给顺序扫描。

`FbWalScanContext` 里与来源模型最相关的字段有：

- `resolved_segments`
- `resolved_segment_count`
- `pg_wal_segment_count`
- `archive_segment_count`
- `ckwal_segment_count`
- `using_archive_dest`
- `using_legacy_archive_dir`
- `ckwal_invoked`

## 十、当前实现与文档里容易混淆的几点

### 1. `archive_dir` 不是最终产品主语义

它现在只是兼容回退项，不应再在新文档里写成主配置。

### 2. `archive_command` 自动发现不是通用 shell 解析器

只识别少数可安全匹配的本地模式。

### 3. `pg_wal` 不是“比 archive 更新所以优先”

当前优先级正相反：archive 优先，`pg_wal` 只补 tail。

### 4. `fb_ckwal` 不是未来想法

它已经在当前代码里真实参与 resolver 闭环。

## 十一、失败条件

当前来源层失败的典型原因有：

1. archive 目录无法解析
2. archive 目录不存在或不可访问
3. 所需 segment 在 archive、`pg_wal`、`recovered_wal` 三处都不可得
4. `pg_wal` 段可见但页头损坏/错配且无法物化恢复
5. 最终拼不出覆盖时间窗的连续段集合

最终错误通常体现为：

- archive autodiscovery error
- contains no WAL segments
- `WAL not complete`

## 十二、当前最短结论

当前来源模型可以压成一句：

```text
archive 是主来源，pg_wal 是 recent tail 补源，recovered_wal 是被纠正/恢复后的可信回灌区，三者由统一 resolver 决定如何拼成可扫描窗口
```

## English

This document explains how `pg_flashback` resolves WAL sources for one query.

Key points:

- The resolver can combine archive storage, `pg_wal`, and
  `DataDir/pg_flashback/recovered_wal`.
- Archive resolution prefers `pg_flashback.archive_dest`, then
  `pg_flashback.archive_dir`, then safe local `archive_command` autodiscovery.
- If both archive and `pg_wal` contain the same segment, archive wins.
- `pg_wal` only serves as the recent tail beyond archive coverage.
- Missing or mismatched segments are handled through `recovered_wal`.

In short, archive is the primary source, `pg_wal` is the recent-tail fallback,
and `recovered_wal` stores corrected segments selected by the same resolver.
