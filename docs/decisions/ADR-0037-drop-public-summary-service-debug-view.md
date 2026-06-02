# ADR-0037: 删除公开 summary service 调试视图

## 状态

已接受

## 背景

当前公开安装面除了用户真正会用的：

- `pg_flashback(anyelement, text)`
- `pg_flashback_debug_unresolv_xid(regclass, timestamptz)`
- `pg_flashback_summary_progress`

之外，还保留了：

- `pg_flashback_summary_service_debug`
- `fb_summary_service_debug_internal()`

这组对象主要暴露 launcher / worker / queue / cleanup 等 summary 服务内部调度细节。

当前用户确认：

- 这组接口“不看也看不懂”
- 不属于首版必须面向用户保留的能力
- 应从公开安装面删除，避免继续把开发期自用观测面带给用户

## 决策

1. 删除公开视图 `pg_flashback_summary_service_debug`
2. 删除其直调底座 `fb_summary_service_debug_internal()`
3. 用户侧 summary 观测只保留 `pg_flashback_summary_progress`
4. 这次变更按公开安装对象集变化处理：
   - 前滚扩展版本
   - 提供升级脚本
   - 同步 README / 架构文档 / 开源镜像

## 结果

- 用户对 summary 的默认观测面收口为：

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

- `summaryd` 仍继续发布：
  - `state.json`
  - `debug.json`

  但这些文件只作为扩展内部读取与研发排查参考，不再通过公开 SQL 调试视图直接暴露。

- `fb_summary_service.c` 中内部统计收集逻辑仍可继续服务：
  - `pg_flashback_summary_progress`
  - daemon 状态文件读取

  但不再保留给用户直接调用、也不再保留给研发手工挂载的
  service debug SQL / fmgr 导出面。
