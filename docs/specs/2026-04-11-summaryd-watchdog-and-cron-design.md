# 2026-04-11 summaryd watchdog / cron 自恢复设计

## 背景

2026-04-11 现场已经确认：

- `summaryd` 并不是被手工停止
- 进程会因为候选段/源段缺失现场触发异常路径而消失
- daemon 消失后，后续新生成的 `meta/summary/*.meta`
  可能退化为 query-side 按需补建

用户新增要求固定为：

- 无论如何 `summaryd` 进程都不能靠异常退出结束生命周期
- 找不到 WAL 时要等待，而不是退出
- bootstrap 要自动装一层自恢复机制
- summary 控制面只保留一个 shell 文件：
  - `start`
  - `stop`
  - `status`

## 目标

把当前 summary 服务承载模型从“单脚本直起单 daemon”
收口为“watchdog + child daemon + cron 保活 watchdog”。

## 设计

### 1. runner 对外接口

`scripts/pg_flashback_summary.sh` 对外只保留：

- `start`
- `stop`
- `status`

不再要求用户显式传：

- `--config`
- `--summaryd-bin`
- `--pid-file`
- `--log-file`

固定路径：

- config
  - `~/.config/pg_flashback/pg_flashback-summaryd.conf`
- watchdog pid
  - `~/.config/pg_flashback/pg_flashback-summaryd.watchdog.pid`
- child pid
  - `~/.config/pg_flashback/pg_flashback-summaryd.pid`
- log
  - `~/.config/pg_flashback/pg_flashback-summaryd.log`

脚本内部允许保留隐藏模式给 watchdog 自调用，但不作为用户面文档接口。

### 2. watchdog 行为

`start` 启动 shell watchdog，而不是直接把 `pg_flashback-summaryd`
留成唯一后台进程。

watchdog 语义：

- 每 `1s` 检查 child daemon 是否存活
- child 不在时，按同一份 config 重新拉起
- `stop` 时优先结束 watchdog，再同步清理 child
- `status` 需要分别显示：
  - watchdog pid
  - child pid
  - 当前 config 路径

### 3. daemon 缺 WAL 语义

缺 WAL 不是服务退出条件，而是等待条件。

需要覆盖的典型现场：

- 候选收集时扫到的源段，在真正打开时已被清理
- build 期需要 decode-tail next segment，
  但下一段暂时不存在
- 打开候选段命中 `ENOENT`

收口方式：

- iteration 记录等待态错误文本
- `service_enabled` 仍保持服务可用语义
- iteration 返回后进入 sleep，等待下一轮
- 不能让这类现场再把生命周期交给 crash / abort / 一次性失败退出

### 4. bootstrap / cron

`scripts/b_pg_flashback.sh setup` 负责：

- 写固定 config
- 执行 `scripts/pg_flashback_summary.sh start`
- 为当前数据库 OS 用户安装一段固定 `crontab` block

cron entry：

```cron
* * * * * /abs/path/to/scripts/pg_flashback_summary.sh start >/dev/null 2>&1
```

语义：

- cron 只负责保证 watchdog 存在
- watchdog 负责 `1s` 级 child 自恢复
- 重复 setup 必须幂等，不得安装重复 block

`--remove` 负责：

- `stop`
- 删除 bootstrap 写入的 cron block
- 删除 config / pid / log

### 5. 测试

先补失败测试，再实现：

- runner smoke：
  - help 不再出现 `run-once`
  - `start` 能拉起 watchdog + child
  - 杀死 child 后 watchdog 能自动拉起
- bootstrap smoke：
  - dry-run / setup / remove 输出反映 cron 安装与清理
  - 重复 setup 不写重复 cron block
- missing-wal smoke：
  - 缺段现场不退出
  - 状态文件持续发布
  - `last_error` 显示等待态而不是 crash 态

## 非目标

- 不恢复 systemd/service 注册模型
- 不提供多实例多 config 用户面
- 不把 cron 冒充成 `1s` 级调度器
- 不把 query-side 按需补建逻辑删除；本次只解决 daemon 存活与前置构建语义
