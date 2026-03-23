# ADR-0003: 最终用户入口采用创建临时结果表

## 状态

Accepted

## 决策

用户态主入口定为：

```sql
SELECT fb_create_flashback_table(
  'fb1',
  'public.t1',
  '2026-03-22 10:00:00+08'
);

SELECT * FROM fb1;
```

同时保留：

- `pg_flashback(regclass, timestamptz)` 作为底层 `SRF`
- `fb_flashback_materialize(regclass, timestamptz, text)` 作为中间层 helper

三者职责固定如下：

- `fb_create_flashback_table(text, text, text)`
  - 主用户入口
  - 纯文本参数
  - 自动解析表名和时间
  - 自动从数据字典生成列定义
  - 创建 `TEMP TABLE`
- `fb_flashback_materialize(regclass, timestamptz, text)`
  - 面向熟悉 PostgreSQL 类型系统的高级用户或测试
  - 仍然自动生成列定义
- `pg_flashback(regclass, timestamptz)`
  - 保留为底层能力
  - 主要供内部调用、调试、测试和高级场景使用
  - 不再作为首推用户入口

## 原因

- PostgreSQL 无法对运行时决定列结构的 `SETOF record` 自动展开 `SELECT *`
- `fb_create_flashback_table()` 能消除：
  - `::regclass`
  - `::timestamptz`
  - `AS t(...)`
- `TEMP TABLE` 结果最符合“闪回后直接查”的使用习惯
- 保留底层 `SRF` 仍然有利于回归测试、内核验证和高级调试

## 明确不做

- 不把 `pg_flashback(regclass, timestamptz)` 改造成唯一用户入口
- 不为首版引入解析器级语法扩展
- 不自动覆盖已存在的目标结果表
