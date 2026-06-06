# FP Token 模块技术文档（v3）

> Markdown 便于 Git/IDE 阅读；深色排版完整版见 [HTML](fp-token-design_20260517.html)。

包路径：`cn.lxdb.plugins.muqingyu.fptoken` · 基于 Lucene BlockTree 的二进制指纹索引与块级合并。

**文档版本**：2026-06（对齐 v3 倒排 bucket 索引 + selective 查询）

---

## 目录

1. [代码解读](#1-代码解读)
2. [基本原理](#2-基本原理)
3. [设计思路](#3-设计思路)
4. [代码结构](#4-代码结构)
5. [数据流与生命周期](#5-数据流与生命周期)
6. [持久化格式（v3）](#6-持久化格式v3)
7. [与 Lucene 的集成点](#7-与-lucene-的集成点)
8. [术语表](#8-术语表)
9. [运维与扩展建议](#9-运维与扩展建议)

---

## 1. 代码解读

### 1.1 模块在做什么

FP Token 为 LXDB/Lucene 提供**按二进制 ngram 子串**检索的能力：

- 索引阶段：字段字节流滑窗 → 带**列名前缀 + 14B FP 头 + ngram 载荷**的词项。
- 写段阶段：按 **列名 + index_id + group_id** 聚合，组内热词挖掘、**sorted bucket 倒排**、闭块判定，落盘倒排 + termsbit 侧车。

### 1.2 核心类职责

| 类 | 包 | 职责 |
|----|-----|------|
| `FpTokenAnalyzer` / `BinarySlidingWindowApi` | token | 分析链：滑窗分词 |
| `FPBlockTreeTermsWriter` | api | 写段入口，`iterator_fp()` 驱动编排 |
| `FpTokenBlockOrchestrator` | api | 分流 Original / Rebuild，换组 flush |
| `FpGroupDataOriginal` | block | 高级别达标组：复用 `fpBits` 透传写出 |
| `FpGroupDataRebuild` | block | 可合并组：热词重建 + 位图 + writefp |
| `FpGroupHotNgramRebuild` | block | common → hot 挖掘、`hotDownTierBudget` |
| `FpGroupHotNgramBitIndex` | block | **v3**：`bucketIndex → order[]` sorted posting + skip 表 |
| `FpSearch` / `FpTokenQuery` | api | 查询：selective `fpBits` → order → seek 倒排 |
| `FpTokenTermLayout` | common | 列名前缀 + 14B 头 + payload |
| `FpBlockInfo` | common | 每组 hot/common tier 偏移与统计 |

### 1.3 词项布局（`FpTokenTermLayout`）

```
[4B columnNameLen][UTF-8 columnName][14B FP头][ngram payload]
```

FP 头 14 字节：`index_id(2) + group_id(4) + level(1) + hot(1) + termIndex(4) + isDel(1) + hotDownTierBudget(1)`。

列名**不参与** ngram 滑窗与 bucket 统计；查询时按列名 seek + 校验。

---

## 2. 基本原理

### 2.1 二进制指纹

字段视为原始字节序列，ngram 长度 `NGRAM_MIN..NGRAM_MAX`（当前 **1~6**）。相似内容共享子串，支持部分匹配类检索。

### 2.2 组与级别

同一 **列 + index_id + group_id** 共享 `distinctDocUnion` 与一块 `FpGroupHotNgramBitIndex`（hot/common 两 tier）。

`FpTokenBlockLevelPolicy` 按 doc/term 规模判定闭块；未达标高级别 term **降级**进 Rebuild。

### 2.3 热词 vs 普通词

- **热词**：common 中高频 ngram 升格；posting 更短，查询先查 hot tier。
- **普通词**：其余载荷；common tier **跳过**已是 hot 的 slice。

### 2.4 Doc 列表

`FPDocList`：默认递增 `int[]`；乱序/重复/超限时升 `SparseFixedBitSet`。

---

## 3. 设计思路

### 3.1 双路径：透传 vs 重建

- **透传**：`FpGroupDataOriginal` + `terms.fpBits(null,null)` 读旧图原样 writefp。
- **重建**：`FpGroupHotNgramRebuild` → `FpGroupHotNgramBitIndex.execute` → 新 tier 写 bitOut。

### 3.2 v3 bucket 索引（替代 v2 的 8×256 FixedBitSet）

每个 tier 含 6 个 **LenRow**（ngram 长度 1~6）：

```
LenRow: sortedKeys[] + entryMeta[] + orderArena（vint 压缩多 order）
skip 表：每 128 条 posting 一条 (anchorKey, keysPtrRel)
```

**bucketIndex** 规则：

| ngram 长度 | bucketIndex |
|------------|-------------|
| 1 | `b0 & 0xFF` |
| 2 | `(b0<<8) \| b1` 大端 |
| 3 | 3 字节大端拼 int |
| 4 | 4 字节大端拼 int |
| 5~6 | `murmurhash3_x86_32` |

> **索引兼容性**：2~4 字节 bucket 算法自 2026-06 起改为直接拼 int；旧段需重建。

### 3.3 查询 selective 读（`FpSearch.loadBitIndex`）

```text
selectiveKeysForSlices(slices) → bucketKeys[]
terms.fpBits(indexId, groupId, bucketKeys, bucketKeys)
  → readfromBanksSelective
      → readSelective：skip 跳跃读盘，预取 orderList → sparseOrders
lookupHotOrders / lookupCommonOrders → 纯内存 HashMap
```

- **不**在实例上持有 `IndexInput`（避免 Lucene 关闭流后 `AlreadyClosedException`）。
- **不**做全量 `fpBits(null,null)` 回退（避免掩盖 selective IO 问题）。
- 无 bit 索引的小组：`searchSparseNoBitIndexTerms` 从 `maxGroupId+1` seek 补充。

---

## 4. 代码结构

```text
cn.lxdb.plugins.muqingyu.fptoken/
├── token/          FpTokenAnalyzer, BinarySlidingWindowApi
├── config/         Lucene80FPSearchConfig, FpTokenBlockLevelPolicy
├── api/            FPBlockTreeTermsWriter, FpSearch, FpTokenQuery
├── dataset/common/ FpTokenTermLayout, FpTermKey, FpBlockInfo, FPDocList
└── dataset/block/  FpGroupData*, FpGroupHotNgramRebuild, FpGroupHotNgramBitIndex
```

依赖方向：`token → common`；`api → block + common`；Lucene 补丁 → `api + dataset.*`。

---

## 5. 数据流与生命周期

**写段**：`iterator_fp` → `acceptTerm` → flush →（透传 | 重建）→ 倒排 + termsbit + `fpblock_list`。

**查**：`FpTokenQuery` → `FpSearch.search` → 按组 `loadBitIndex` → slice 查 order → `iterator_fp` seek term → 合并 doc（多 slice AND）。

---

## 6. 持久化格式（v3）

### 6.1 `FpBlockInfo`（`FORMAT_VERSION = 3`）

| 字段 | 说明 |
|------|------|
| `fpBanksHot` | hot tier 在 termsbit 文件中的偏移 |
| `fpBanksCommon` | common tier 偏移 |
| `hotCount` / `commonCount` | 组内 term 数 |
| `targetLevel` | 块级别 |
| `fieldInfo` | 列名（BytesRef） |

### 6.2 Tier 布局（magic `'FPTR'`）

```text
magic + lenRowOffset[6]
LenRow × 6:
  entryCount
  skip[(anchorKey, keysPtrRel)] × ceil(N/128)
  sortedKeys[N] + entryMeta[N] + arenaLen + orderArena
```

### 6.3 entryMeta 编码

- 低 bit = 0：单 order 内联 `(order << 1) | 0`
- 低 bit = 1：多 order，高 31 位为 orderArena 内 vint 列表偏移

---

## 7. 与 Lucene 的集成点

| 扩展点 | 说明 |
|--------|------|
| `Terms#iterator_fp()` | FP 词项迭代 |
| `Terms#fpBits(..., loadHotKeys, loadCommonKeys)` | 全量或 selective 读 tier |
| `TermsWriter#writefp` | 写出 FP term + posting |
| `FieldReader.bitIn` | termsbit 侧车 `IndexInput` |
| 段 meta `fpblock_list` | group_id → `FpBlockInfo` |

补丁源码参考：`export/src/main/java/org/apache/lucene/codecs/blocktree/FieldReader.java`。

---

## 8. 术语表

| 术语 | 含义 |
|------|------|
| bucketKey | `packBucketKey(lenIdx, bucketIndex)`，64 bit |
| lenIdx | ngram 长度 - 1，0..5 |
| order | term 在组内序号（1-based 写 header，0-based 查 bit 索引） |
| sparse | selective 加载实例，仅含部分 bucket 的 order |
| hotDownTierBudget | 热词向下扩展 merge / 查询档数 |

---

## 9. 运维与扩展建议

- 验证：`.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag`（当前 48 项）。
- 灌数据：默认关闭查询 trace；flush 摘要用 INFO。
- 升级 v3 bucket 规则或 selective 逻辑后：**重建段**或全量 reindex。
- 生产排障：查 `selectiveIoBroken`、列名 mismatch、`AlreadyClosed`（应已在 selective 预取修复）。

---

## 相关文档

- [写段流程图 MD](fp-token-write-path-detailed.md) · [HTML](fp-token-write-path-detailed.html)
- [写查对齐报告 MD](fp-token-write-search-alignment-report.md)
- [审查与测试报告 MD](fp-token-review-and-test-report_20260517.md)
- [仓库 README](../README.md)
