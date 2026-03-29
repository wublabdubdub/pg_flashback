# ADR-0016: `pg_flashback_to` 原表闪回

## 状态

Accepted

## 决策

将原表闪回公开入口统一为 `pg_flashback_to(regclass, text)`，用于直接修改原表，把 keyed 大表回退到目标时间点。

当前范围固定为：

- 仅 keyed relation
- 仅单列稳定键
- 执行时拿 `AccessExclusiveLock`
- 检测到外键或用户触发器直接报错
- 当前不支持 bag relation 与多列键

执行路径固定为：

1. 构建一次 `WAL scan -> replay -> ReverseOpSource`
2. keyed apply 汇总“变化 key 在目标时间点的终态”
3. 不扫描当前整表
4. 将终态分为：
   - `update/insert` 集
   - `delete` 集
5. 通过批量 `UPDATE / INSERT / DELETE` 直接改写原表

## 原因

- 对大表全表闪回，`pg_flashback_to()` 需要“扫当前整表 + 重写新表 + 建索引”，后半段成本过高
- 原表闪回只需要处理变化 key，更接近 keyed 模式的真实最小工作集
- 当前产品边界需要为“大表 keyed 全表闪回优先优化速度”而调整

## 影响

- 产品边界不再是“首版严格只读”
- 新能力会真实改写业务表，必须显式持有强锁
- 失败时依赖调用方事务回滚
- 含外键/用户触发器的表当前不进入该路径
- 旧的 `pg_flashback_rewind(regclass, text)` 名称下线
- 旧的“创建 `table_flashback` 新表”公开入口下线
