# Release Gate

本目录用于 `pg_flashback` 的发布前阻断式功能/性能 gate，不属于 `make installcheck` 常规回归。

当前首批骨架约定：

- 支持版本：`PG14-18`
- 测试数据库：`alldb`
- 归档根目录：`/walstorage`
- 按版本归档子目录：
  - `PG14 -> /walstorage/14waldata`
  - `PG15 -> /walstorage/15waldata`
  - `PG16 -> /walstorage/16waldata`
  - `PG17 -> /walstorage/17waldata`
  - `PG18 -> /walstorage/18waldata`

当前入口：

- 总入口：`tests/release_gate/bin/run_release_gate.sh`
- 环境准备：`tests/release_gate/bin/prepare_empty_instance.sh`
- 自检：`tests/release_gate/bin/selftest.sh`

当前规则：

- 启动前会检查归档是否开启
- 要求当前实例 `archive_command` 至少显式引用本版本对应的归档子目录
- 每轮测试前后都清理对应归档子目录
- 当前只落第一批骨架；`alldbsimulator` 驱动、truth snapshot、flashback 场景、报告判定会在后续任务继续补齐
