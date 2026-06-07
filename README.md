# FPToken

LXDB 上的 **二进制指纹（Fingerprint）检索插件**。在 Lucene BlockTree 写段阶段，把字段字节流切成 ngram 词项，按「列 + 组」聚合，构建 **hot/common 双套 bucket 倒排**，查询时按子串切片快速定位 doc，而不是扫全段倒排。

包名：`cn.lxdb.plugins.muqingyu.fptoken`  
字段后缀：`*_bfp`（二进制）、`*_sfp`（字符串）

本仓库是 **2026-05 重写** 后的独立源码，需与 **补丁版 Lucene 8.9 + LXDB `lib/`** 联编后部署为 `mqy_fptoken_*.jar`。

---

## 项目介绍

### 在 LXDB 里扮演什么角色

LXDB 把 Lucene 当作存储与检索引擎。普通文本字段走分词倒排；FP 字段（`*_bfp` / `*_sfp`）走 **本模块提供的专用 pipeline**：

- **灌数据 / 建索引**：Analyzer 产出带列名前缀的 FP 词项 → BlockTree 写段时由 `FPBlockTreeTermsWriter` 按组聚合 → 写出倒排 + termsbit 侧车索引。
- **查询**：SQL / FDW 层构造 `FpTokenQuery`，传入 **列名** 与 **查询字节**；`FpSearch` 用 bucket 索引缩小候选 term，再 seek 倒排取 doc，多子串之间 **AND**。

FPToken **不替代** Lucene 倒排，而是在倒排之上加一层 **「ngram → 组内 term 序号（order）」** 的辅助索引，把「这段字节可能出现在哪些 term 里」从 O(全段词项) 降到 O(命中 bucket 的 order 数)。

### 典型业务场景

**1. JSON / 宽表稀疏列**

一张表可能有百万级 JSON key，每列只在少量 doc 出现。若不为列名单独分区，不同列上相同的 4 字节片段会混在同一倒排里，查询 `WHERE txt4_sfp LIKE '%abcd%'` 无法区分是哪一个 JSON 列命中。

FPToken 在每个词项最前写入 **可变长列名**（UTF-8），倒排按「列名字典序 + 组内序」自然分区；查询只 seek 目标列的前缀，ngram 统计与 bucket 也只针对该列的 payload。

**2. 二进制 / 日志 / 协议载荷**

内容不是自然语言，传统 IK/HanLP 分词无效。FP 把字段当作 **原始字节序列**，用长度 1~6 的 ngram 覆盖子串；相似二进制文件会共享大量 ngram，从而支持 **子串包含式** 检索（具体 SQL 算子由上层封装）。

**3. 段内大规模 term 与 posting**

一个 FP 组内可能有成千上万条 distinct payload，每条 payload 又展开成大量 ngram 词项。若全部平等对待，posting 体积与查询 fan-out 都很大。

写段时在组内做 **热词挖掘**：在多个 common 词里反复出现的 ngram 升格为 hot，doc 合并到 hot posting；查询 **先查 hot tier 的 bucket**，再查 common tier，且 common 侧 **跳过** 已是 hot 的 slice，减少重复 work。

### 与「暴力 ngram / 全表扫描」的差异

| 方式 | 问题 |
|------|------|
| 灌索引时对全文做全量 ngram 倒排 | 词项爆炸，段合并慢，查询 fan-out 大 |
| 查询时逐 doc 暴力匹配 | CPU 与 IO 不可接受 |
| 仅 Lucene 通配 / Regex | 难以利用段结构，无法按列隔离 |
| **FPToken** | 写段时 **闭块 + 热词 + bucket 索引**；查询时 **selective 读 bucket**，只对少量 order seek 倒排 |

### 模块边界

- **本 repo 负责**：BlockTree 写段编排、组内热词重建、v3 bucket 索引、查询侧 `FpSearch` / `FpTokenQuery`。
- **不在本 repo**：互斥频繁项集、Pre-merge hint、行级采样挖掘（`docs/` 里旧文档仅作历史参考）；Lucene 补丁本体在 `lib/` 与 `export/`。

更完整的设计说明：[docs/fp-token-design_20260517.md](docs/fp-token-design_20260517.md)

---

## 技术原理

### 设计直觉：层次前缀、共享存储与有限扫描

**与树状数组（Fenwick Tree）相近的思路，实现并不相同。**

树状数组把区间和拆成 **O(log n) 个有父子关系的层次节点**——更新/查询只碰少数几个下标，而不是扫整个数组。FPToken 里也有类似动机：若 payload 是 `terminator…` 这类字节串，则 `t → te → ter → term → termi → termin → …` **在字节上是前缀嵌套**，再叠加上组内 **termIndex**，它们天然属于同一「家族」。若每个前缀 ngram 都各存一份完整 doclist，存储会膨胀，查询也要对段内全部 term 做 fan-out。

因此采用 **层次结构 + 共享 posting**：

| 维度 | 做法 | 效果 |
|------|------|------|
| **层次** | ngram 按长度 1~6 字节分 **LenRow**（长度档）；hot/common 双 tier；热词带 `hotDownTierBudget` 控制向下扩展几档 | 前缀关系用「档」表达，而不是扁平一张大表 |
| **共享 doclist** | 写段 **热词 merge**：多个 common 词里反复出现的 ngram 升格为 hot，把 common 的 doc **并入** hot posting；同一 doc 集合不必为每个短前缀各存一份全量副本 | 类似树状数组「父层汇总、子层复用」的压缩直觉（这里是 **倒排合并**，不是数值前缀和） |
| **有限扫描** | 索引：payload 按 **32 字节窗、26 字节步** 滑窗（`BITSET_WINDOW_SIZE=32`，步进 `32-NGRAM_MAX`），长文本 window 数 ≈ O(长度/26)。查询：输入切成 **1~6 字节 anchor slice**（步进 4），再经 bucket → **order[]** → seek 少量倒排 | 查询 **不扫全段 FP 词典**，只碰「少量 slice × 每组少量 order」 |

```text
字节前缀：  t ── te ── ter ── term ── termi ── termin ── …  (+ 组内 termIndex)
存储层次：  LenRow[1..6]  +  hot/common tier  +  热词 merge 共享 doc 并集
查询路径：  少量 anchor slice  →  bucket  →  order[]  →  seek 倒排（非全段扫描）
```

与经典树状数组的差异：**不是**在数组下标上做 `lowbit` 跳跃，而是在 **字节前缀 + 长度档 + bucket 索引** 上组织；查询代价由 **slice 个数**（通常个位数）和 **bucket 内 order 数** 决定，而不是段内 term 总数。下文 §4、§5 是具体算法与落盘格式。

### 1. 二进制指纹（Fingerprint）是什么

索引阶段，`FpTokenAnalyzer` / `BinarySlidingWindowApi` 对字段字节做滑窗（**32 字节窗、26 字节步**），每个窗口作为 **payload** 写入倒排。两个 doc 若在某段字节上相同，就会共享对应的 ngram 词项。

查询阶段，把用户输入同样切成若干 **anchor slice**（长度在 1~6 字节之间，由 `FpTokenQuery` 生成）。**只有 doc 的 payload 同时包含全部 slice**（AND 语义）才命中——等价于「查询串作为子串出现在字段指纹中的某段窗口里」。

列名不参与滑窗：ngram 只来自 payload，避免 `user.name` 的列名 bytes 污染 `user.age` 的统计。

### 2. 词项在磁盘上的布局

每条 FP 倒排词项 = **列名前缀 + 14 字节定长头 + ngram 载荷**：

```text
┌──────────────┬─────────────────┬──────────────┬──────────────────┐
│ 4B 列名长度   │ UTF-8 列名       │ 14B FP 头     │ ngram payload    │
└──────────────┴─────────────────┴──────────────┴──────────────────┘
```

FP 头主要字段：

| 字段 | 作用 |
|------|------|
| `index_id` | 段内分片号（MultiTerms 合并时可替换） |
| `group_id` | 逻辑组号，对应 `fpblock_list` 与 termsbit 里的一块索引 |
| `group_level` | 该 term 来自哪一级闭块 |
| hot 标记 | 1=热词，0=普通词 |
| `termIndex` | 组内序号，与 bucket 索引里的 **order** 对应 |
| `hotDownTierBudget` | 查询/写段时允许向下扩展的子档层数 |

查询时用 `make_fp_search_prefix(columnName, …)` 构造 seek 前缀，先对齐列名与组头，再比对 payload 是否与 slice 一致。

### 3. 组（Block Group）与写段双路径

**组** = 同一 `列名 + index_id + group_id` 下的所有 term。它们共享：

- 组级 doc 并集 `distinctDocUnion`（判定块是否「够大」可以闭块写出）；
- termsbit 文件中一段 **hot tier + common tier**（`FpBlockInfo.fpBanksHot` / `fpBanksCommon`）。

写段编排器 `FpTokenBlockOrchestrator` 对每个词项看其 **level 是否 ≥ 当前列的 targetLevel**：

- **≥ targetLevel** → 进入 `FpGroupDataOriginal`（高级别路径，保留原 term 头，热词+普通都收）。
- **< targetLevel** → 进入 `FpGroupDataRebuild`（只攒 common 的 posting，等待合并重建）。

换列或换组时 flush 旧缓冲。flush 时分叉：

```text
flushHighGroup
├── 组够大（doc 数 / term 数达阈值）→ 透传：terms.fpBits 读已有 tier，Original.flushto 原样 writefp
└── 不够大 → mergeIntoRebuild，并进小组重建路径

flushCommonGroup → Rebuild.flushto（热词挖掘 + 新建 bucket 索引 + writefp）
```

**透传**避免对已经稳定的大组重复跑 ngram 重建；**重建**保证小组在合并后热词表与 bucket 索引一致。

闭块阈值由 `FpTokenBlockLevelPolicy` 控制（低级别约千级 doc/term，高级别约十万级，可乘 rate）。

### 4. 热词挖掘（FpGroupHotNgramRebuild）

在 **Rebuild 路径**内，对组内所有 common 载荷做 1~6 字节滑窗，统计每个 ngram 在 **多少个 distinct common 词** 中出现（词内去重）。出现次数 ≥ `HOT_TIER_TERM_COUNT_THRESHOLD`（当前 **64**）的 ngram 升格为 **hot term**。

随后：

1. **merge common doc 到 hot**：按 ngram **从长到短**，把 common 的 doc 列表并入匹配的热词 posting。
2. **hotDownTierBudget**：限制从锚点热词向下扩展拼子档的层数；写进 term 头，查询侧据此决定能否用更短前缀替代长 ngram。
3. **父前缀 skip**：较长 ngram merge 后，在**同一 common 词内**标记短前缀已处理，避免重复 merge；但不物理删除父热词 posting 里已存在的 doc（深档查询需要）。

热词表是扁平 `TreeMap<FpTermKey, …>`，层级由 **字节前缀 + 长度档** 表达，而不是一棵显式 Trie。

### 5. v3 bucket 索引（FpGroupHotNgramBitIndex）

v2 曾用 8×256 的 `FixedBitSet` 矩阵；v3 改为 **整型域 bucket + 升序 posting 表**，支持更大桶空间且 **selective 跳跃读盘**。

#### 5.1 逻辑结构

每组 termsbit 含 **两个 tier**（hot / common），结构相同：

```text
Tier
├── LenRow[0]  … ngram 长度 = 1
├── LenRow[1]  … ngram 长度 = 2
│     …
└── LenRow[5]  … ngram 长度 = 6

LenRow = sortedKeys[bucketIndex] + entryMeta + orderArena（多 order 的 vint 池）
         + skip 表：每 128 条 posting 一条 (anchorKey, keysPtrRel)
```

**bucketIndex** 如何把 ngram 字节映射到 int：

| ngram 长度 | 算法 |
|------------|------|
| 1 | 单字节 `0x00..0xFF` |
| 2~4 | 大端直接拼进 int（不 hash） |
| 5~6 | `murmurhash3_x86_32` |

**order** = 该 bucket 下命中 term 的组内序号列表（升序、vint 增量压缩）。一个 bucket 可对应多个 term（不同 payload 共享同一 4 字节片段时）。

#### 5.2 构建期（execute）

`FpGroupHotNgramBitIndex.execute` 扫描组内 hot/common 载荷的所有 ngram 窗口，写入对应 LenRow 的 `buildMap`；`finalizeRow` 排序、压缩 orderArena；`flushto` 整 tier 顺序写入 termsbit，并在 `FpBlockInfo` 记录偏移。

Original 透传路径则 **整组 load 旧 tier 再写出**，不做 CPU 密集的重新聚合。

#### 5.3 查询期（selective 读）

查询不会加载整组 tier（可能数 MB）。流程：

```text
1. FpTokenQuery 把查询字节切成 slices[]
2. selectiveKeysForSlices → 去重 bucketKey[]（packBucketKey(lenIdx, bucketIndex)）
3. terms.fpBits(indexId, groupId, keys, keys)
     → readfromBanksSelective
         → 对每个 key：skip 定位 LenRow 段 → 读 orderList → 放入 sparseOrders
4. lookupHotOrders(slice) / lookupCommonOrders(slice)  →  纯内存 HashMap
5. 对每个 order：iterator_fp().seek(term) → 读 posting → 合并 doc
6. 多个 slice 的结果 FixedBitSet AND
```

要点：

- IO 发生在 **fpBits 调用内**，用 `IndexInput.clone()`，读完即关，**不在位图对象上持有文件句柄**。
- 不做 `fpBits(null,null)` 全量回退（避免掩盖 selective 问题且内存过大）。
- 没有 bit 索引的极小组：`searchSparseNoBitIndexTerms` 从 `maxGroupId+1` seek 倒排补充。

### 6. 查询语义小结

| 维度 | 语义 |
|------|------|
| 多个 slice | **AND**：每个 slice 至少命中一个 doc |
| 同一 slice 多个 order | **OR**：任一 term 命中即可 |
| 多个 group | 同 slice 在组间 **OR**，再参与 slice 级 AND |
| 列名 | 必须匹配；与索引列不一致则整段无命中 |

### 7. 落盘文件一览

| 产物 | 内容 |
|------|------|
| BlockTree 倒排 | FP 词项 + posting |
| termsbit 侧车 | 各组 hot/common tier 二进制 |
| 段 meta `fpblock_list` | `group_id → FpBlockInfo`（tier 偏移、hot/common 计数、列名、级别） |

`FORMAT_VERSION = 3`。升级 bucket 规则或 selective 逻辑后需 **重建 FP 段**。

---

## 解决什么问题（速查）

| 场景 | 做法 |
|------|------|
| JSON / 宽表有海量稀疏列 | 每个词项带 **列名前缀**，查询只扫目标列，ngram 不会跨列混桶 |
| 二进制 / 半结构化内容 | 不做中文分词，直接对 **原始字节** 做 1~6 字节 ngram |
| 段内词项很多、posting 大 | 组内 **热词挖掘**，高频 ngram 单独一路；common 路跳过已是 hot 的切片 |
| 查询不能一次读整组位图 | **Selective 读盘**：只按本次 query 的 bucket 用 skip 表跳跃 IO，order 预取进内存 |

---

## 一分钟看懂数据流

**索引（写段）**

```text
FpTokenAnalyzer  →  term = [列名][14B头][ngram载荷]
       ↓
FPBlockTreeTermsWriter.writeTerms
       ↓
FpTokenBlockOrchestrator  →  大组透传 / 小组重建
       ↓
FpGroupHotNgramRebuild + FpGroupHotNgramBitIndex  →  hot/common tier
       ↓
倒排 (writefp)  +  侧车 termsbit  +  段 meta (fpblock_list)
```

**查询**

```text
FpTokenQuery(列名, 查询字节)
       ↓
滑窗切片 → bucketKeys
       ↓
terms.fpBits(..., bucketKeys, bucketKeys)   // selective，预取 order
       ↓
lookupHotOrders / lookupCommonOrders  →  seek 倒排  →  多 slice AND
```

写段流程图（PNG / 逐步说明）：[docs/fp-token-write-path-detailed.md](docs/fp-token-write-path-detailed.md)

---

## 仓库结构

```text
src/cn/lxdb/plugins/muqingyu/fptoken/
├── token/           分析链：FpTokenAnalyzer、BinarySlidingWindowApi
├── config/          字段识别、ngram 范围、热词阈值、闭块策略
├── api/             写段入口、查询：FPBlockTreeTermsWriter、FpSearch、FpTokenQuery
├── dataset/common/  词项布局 FpTokenTermLayout、FpBlockInfo、FPDocList
└── dataset/block/   组数据、热词重建、FpGroupHotNgramBitIndex (v3)

src/test/java/.../fptoken/tests/   单测 / 功能测 / 性能测
docs/                              设计文档（HTML + 同名 MD）
scripts/run-fptoken-tests.ps1      编译 + 跑测（canonical）
lib/                               从 LXDB 工程同步的 JAR（补丁 Lucene 等）
export/                            补丁 Lucene 源码参考（FieldReader#fpBits 等）
```

源码入口速查：

| 主题 | 类 |
|------|-----|
| 词项布局 | `FpTokenTermLayout` |
| 写段编排 | `FpTokenBlockOrchestrator`、`FPBlockTreeTermsWriter` |
| 热词 + bucket | `FpGroupHotNgramRebuild`、`FpGroupHotNgramBitIndex` |
| 查询 | `FpSearch`、`FpTokenQuery` |
| 配置 | `Lucene80FPSearchConfig`、`FpTokenBlockLevelPolicy` |

---

## 快速开始

### 1. 准备依赖

从 LXDB 工程拷贝完整 `lib/`（含补丁 Lucene）。可校验：

```powershell
.\scripts\sync-lib-from-classpath.ps1
```

### 2. 编译并测试

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

- 默认 **48** 项测试（不含 `@Tag("performance")`）。
- HTML 报告：`build/test-results/junit-html/index.html`
- IDE 已全量编译时可加 `-SkipCompile`

### 3. 打包部署

编译产物在 `bin/`，与 LXDB 一起打成 `export/mqy_fptoken_v202605.jar`（以你们构建脚本为准），替换 DataNode 上的插件 jar 后 **重建或 reindex FP 段**。

---

## 文档

| 想了解… | 去看 |
|---------|------|
| 完整技术设计（v3） | [docs/fp-token-design_20260517.md](docs/fp-token-design_20260517.md) |
| 写段逐步流程 | [docs/fp-token-write-path-detailed.md](docs/fp-token-write-path-detailed.md) |
| 写 / 查对齐审查 | [docs/fp-token-write-search-alignment-report.md](docs/fp-token-write-search-alignment-report.md) |
| 测试与审查记录 | [docs/fp-token-review-and-test-report_20260517.md](docs/fp-token-review-and-test-report_20260517.md) |
| 全站索引（每个 HTML 都有 .md） | [docs/index.md](docs/index.md) · [docs/README.md](docs/README.md) |
| Agent / 贡献约定 | [AGENTS.md](AGENTS.md) |

浏览器版导航：[docs/index.html](docs/index.html)

---

## 与 Lucene 的接点

本模块依赖 Lucene 侧扩展（源码参考 `export/src/.../blocktree/`）：

| 扩展 | 用途 |
|------|------|
| `Terms#iterator_fp()` | 遍历 FP 词项 |
| `Terms#fpBits(..., loadHotKeys, loadCommonKeys)` | 全量或 selective 读 tier |
| `TermsWriter#writefp` | 写出 FP term + posting |
| 段 meta `fpblock_list` | `group_id → FpBlockInfo` |
| 侧车 `termsbit` | hot/common tier 二进制 |

---

## 本仓库不包含什么

`docs/` 里仍有 **互斥频繁项集、Pre-merge hint、Beam 挖掘** 等旧方案文档，**源码已不在本 repo**。当前 FPToken 只做 **段内 BlockTree FP 写段 + ngram bucket 检索**；行级项集能力在 LXDB 其它模块，可产品内组合使用。

---

## 许可

各源文件 `@author` 为准；与 LXDB / Lucene 补丁的分发策略以宿主工程为准。
