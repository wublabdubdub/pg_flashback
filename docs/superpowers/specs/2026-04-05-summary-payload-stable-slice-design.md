# Summary Payload Stable Slice Design

## 目标

把当前 `summary payload locator-first` 从“查询时现拼现排”推进到
“summary build/query cache 期稳定 slice + payload plan 期 segment 去重”，
先消除 live case
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
在 `3/9` 的新热点。

## 当前问题

当前 payload locator 虽然已经能把 payload decode 降到精确 record 入口，
但 query 期仍存在两类重复工作：

1. plan 层重复 lookup
   - `payload_base_windows` 碎片很多时，同一 segment 会被重复命中
2. lookup 层重复排序
   - query cache 只缓存 summary 文件，不缓存 relation-scoped public slice
   - 每次 `fb_summary_segment_lookup_payload_locators_cached()` 仍要重新复制、
     重新排序、重新去重

因此，旧热点“无关 WAL decode”被压下去后，新热点直接转移成
locator 规划与排序。

## 方案

### 1. summary build 期稳定 slice

- 对每个 relation entry 的 payload locators 在 build 期按：
  - `record_start_lsn`
  - `kind`
  - `flags`
  排序并去重
- sidecar 继续使用 relation entry 的：
  - `first_payload_locator`
  - `payload_locator_count`
  来描述 slice
- 但其语义从“append 出来的原始序列”升级为“稳定有序 slice”

### 2. query cache 期 public slice 复用

- 首次加载 summary 后，为每个 relation entry 延迟物化一个 public locator slice：
  - 直接把 segment-relative offset 转成 public `record_start_lsn`
  - 不再在每次 lookup 时重新 `qsort`
- 命中 relation entry 时直接返回该 cached slice
- 若目标 relation 同时命中 main/toast 两个 entry，则做轻量 merge，而不是
  “拷平后总排序”

### 3. payload plan 期 segment 去重

- `fb_build_summary_payload_locator_plan()` 不再让碎片 `base_windows`
  逐窗口驱动重复 lookup
- 先收集去重后的 segment 集合及该 segment 对应的 clipped LSN 范围
- 每个 segment 最多执行一次 locator slice 读取
- 对无 locator / old summary / recent tail 缺覆盖的 segment，仍保留原有 fallback

## 查询期预期复杂度变化

从当前近似：

- `O(fragmented_windows * repeated_segment_lookup * qsort(locators_per_segment))`

收敛到：

- `O(unique_segments + matched_relation_slices + merged_locator_records)`

这次 live case 的预期收益不是再减少 payload decode，而是去掉
payload locator 规划阶段内部的重复 work。

## 回归与观测

补充两类观测：

- `summary_payload_locator_segments_read`
  - 当前 query payload plan 实际读取了多少 unique summary segment
- `summary_payload_locator_public_builds`
  - 当前 query 中实际物化了多少次 public locator slice

回归要锁住：

- 新字段存在
- locator-first 仍命中
- 对高碎片 synthetic case，观测值不再随碎片窗口数线性放大

## 风险

- main/toast 双 entry merge 的排序稳定性
- query cache 生命周期下的内存释放
- summary 旧版本与新 stable slice 语义之间的兼容回退
- live case 可能在消除重复排序后暴露下一个 `3/9` 次热点，需要继续迭代
