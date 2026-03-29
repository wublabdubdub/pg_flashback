# keyed fast path 实施计划

1. 文档与状态登记

- 更新 `STATUS.md`
- 更新 `TODO.md`
- 新增设计稿与 ADR

2. 回归先行

- 新增 `sql/fb_keyed_fast_path.sql`
- 先覆盖 `EQ`、`IN`、`ORDER BY ... LIMIT`
- 用 `EXPLAIN (VERBOSE, COSTS OFF)` 固定 planner/executor 形状

3. 元数据补齐

- `FbRelationInfo` 增加稳定 key 索引 oid
- 新增统一 `FbFastPathSpec`

4. planner / CustomScan 接线

- 在 `fb_custom_scan` 里识别三类查询
- 将 fast-path 规格写入 `custom_private`
- executor 反序列化后传给 `fb_flashback_query_begin()`

5. entry / apply 接线

- `fb_flashback_query_begin()` / `fb_apply_begin()` 接受 `FbFastPathSpec`
- apply 根据模式选择：
  - 全表 scan
  - key lookup
  - ordered top-N

6. keyed apply 实现

- 实现 key 点查与 key 集合扫描
- 实现按 key 顺序的 top-N 早停
- 加入保守回退条件

7. 验证

- `make clean`
- `make install`
- 跑新回归与相关旧回归
- 手工 `EXPLAIN` 验证 `ORDER BY ... LIMIT` 不再出现 `Sort`
