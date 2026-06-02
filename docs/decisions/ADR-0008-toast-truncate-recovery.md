# ADR-0008: TOAST truncate 不再作为 relation 级直接拒绝边界

## 状态

Accepted

## 决策

对主表继续保持严格 storage boundary：

- truncate
- rewrite
- relfilenode / storage change

仍在 WAL 扫描阶段直接报错。

但对目标主表关联 TOAST relation 的 `SMGR TRUNCATE`，不再 relation 级直接拒绝 flashback。

同时，standalone `STANDBY LOCK` 也不再作为直接拒绝边界：

- 它只表示 Hot Standby 所需的 `AccessExclusiveLock` WAL 记录
- 若没有被更具体的 `truncate/create/rewrite` 事件具体化，不应单独阻塞 flashback

新的判定改为：

- 如果现有 replay + TOAST chunk store 足以重建目标历史值，则查询成功
- 如果目标历史 tuple 需要的 TOAST chunk 最终缺失，则在 TOAST finalize 阶段报错

## 原因

- TOAST truncate 常见于 `VACUUM` / autovacuum 回收尾部空页，不等价于“主表历史值不可恢复”
- 当前 `fb_toast` 以 `valueid + chunk_seq` 保存历史 chunk，本身不依赖 live TOAST relation 当前文件长度
- 一刀切拒绝会把本来可恢复的历史查询误判为不支持

## 影响边界

- 不改变用户 SQL 入口
- 不放宽主表 truncate / rewrite 的边界
- 不引入 TOAST 列降级语义
- 继续保证“要么返回完整正确历史值，要么明确报错”

## 明确不做

- 本轮不实现完整的跨 truncate relation-size 时态恢复
- 不为 TOAST truncate 新增 GUC 开关
- 不因为 TOAST truncate 而回退到 live TOAST relation 取值
