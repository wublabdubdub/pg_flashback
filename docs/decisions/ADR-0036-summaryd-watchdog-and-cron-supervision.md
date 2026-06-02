# ADR-0036: summaryd 采用 watchdog + cron 双层保活，缺 WAL 只等待

## 状态

Accepted

## 背景

`ADR-0035` 已将 summary 服务从 preload/bgwoker 模式迁移到库外
daemon，并固定由脚本管理生命周期。

但当前脚本 runner 仍只有“直接拉起一个 child daemon”这一层语义：

- `summaryd` 进程异常退出后，需要人工再执行一次 `start`
- bootstrap 当前不会安装自动保活机制
- runner 仍要求用户显式传 `--config` 等细节参数
- `summaryd` 遇到缺 WAL / 缺 decode-tail segment 时，
  仍可能把状态打成失败或让生命周期依赖异常路径

这与当前要求冲突：

- 用户侧不应手工盯进程是否消失
- 不再引回 systemd/service 依赖
- 若 WAL 暂时缺失，daemon 必须等待而不是退出
- bootstrap 必须提供自动保活

## 决策

summary 后台服务承载模型收口为：

- 一层 shell watchdog
- 一层 `pg_flashback-summaryd` child
- 一层 per-user `cron` 保活 watchdog

具体语义固定如下：

- `scripts/pg_flashback_summary.sh` 对外只保留：
  - `start`
  - `stop`
  - `status`
- runner 固定读取：
  - `~/.config/pg_flashback/pg_flashback-summaryd.conf`
- runner 固定管理：
  - `~/.config/pg_flashback/pg_flashback-summaryd.watchdog.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.log`
- `start` 的真实动作不是直接起 daemon，
  而是拉起 watchdog：
  - watchdog 每 `1s` 检查 child daemon
  - child 不存在则自动重启
- `stop` 必须同时停止：
  - watchdog
  - child daemon
  - stale pid 记录
- `status` 必须分别暴露：
  - watchdog 是否存在
  - child 是否存在
  - 当前配置路径
- `scripts/b_pg_flashback.sh setup`
  必须安装当前数据库 OS 用户的 `crontab` block：
  - `* * * * * /abs/path/to/scripts/pg_flashback_summary.sh start >/dev/null 2>&1`
- `cron` 的职责固定为：
  - 保证 watchdog 自身存在
  - 不直接承担 `1s` 级轮询
- `scripts/b_pg_flashback.sh --remove`
  必须删除 bootstrap 写入的 cron block

缺 WAL 的 daemon 语义固定为：

- 候选收集阶段若发现源段暂时缺失：
  - 进入等待重试
  - 不把 `service_enabled` 打成 `false`
- build 阶段若缺 next segment / 打开段命中 `ENOENT`：
  - 记录等待态 `last_error`
  - 本轮结束后 sleep，等待下轮
  - 不把服务生命周期交给异常退出

## 后果

优点：

- 用户侧不再需要手工盯 `summaryd` 是否消失
- 不重新引入 systemd 依赖
- 缺 WAL 现场不会再表现为“进程突然没了”
- bootstrap 具备最小可依赖的自动恢复语义

代价：

- runner 需要维护 watchdog / child 两层 pid 与状态
- bootstrap 需要额外维护可幂等的 `crontab` block
- `status` / README / smoke test 都要同步收口到新语义

## 语义要求

- 对外控制脚本保持单文件入口
- 用户面不再暴露 `run-once`
- `cron` 最小粒度仍是 `1 min`，不能伪装成 `1s`
- `1s` 级检查只能由 shell watchdog 实现
- `summaryd` 的缺 WAL 语义必须是等待重试，而不是退出
