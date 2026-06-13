# FPToken

LXDB 上的 **二进制指纹（Fingerprint）检索插件**。在 Lucene BlockTree 写段阶段，把字段字节流切成 ngram 词项，按「列 + 组」聚合，构建 **hot/common 双套 bucket 倒排**，查询时按子串切片 **selective 读盘** 定位 doc，而不是扫全段 FP 词典。

| 项 | 值 |
|----|-----|
| 包名 | `cn.lxdb.plugins.muqingyu.fptoken` |
| 字段后缀 | `*_bfp`（二进制）、`*_sfp`（字符串） |
| bitindex 格式 | **v7**（`FpBlockInfo.FORMAT_VERSION = 7`） |
| ngram 范围 | **1~5 字节**（`NGRAM_MAX = 5`） |
| 部署 | 与补丁版 Lucene 8.9 + LXDB `lib/` 联编 → `mqy_fptoken_*.jar` |

**业务介绍（非技术）**：[docs/fp-token-binary-search-presentation.html](docs/fp-token-binary-search-presentation.html)

---

## 1. 解决什么问题

| 痛点 | FPToken 做法 |
|------|--------------|
| JSON 宽表百万稀疏列，子串查询跨列污染 | 每个词项带 **UTF-8 列名前缀**，查询只扫目标列 |
| 二进制/日志/协议，传统分词无效 | 对 **原始字节** 做 1~5 字节 ngram，子串 AND 检索 |
| 段内 term/posting 爆炸 | 组内 **热词挖掘** + doc 合并；common 跳过已是 hot 的 slice |
| 查询不能一次读整组 MB 级索引 | **Selective 读盘**：skip1→skip2→keys→order，只 IO 本次 query 的 bucket |

---

## 2. 在 LXDB 中的位置

```text
灌数据 / Analyzer          查询 / SQL·FDW
       │                          │
       ▼                          ▼
FpTokenAnalyzer          FpTokenQuery(列名, 查询字节)
BinarySlidingWindowApi            │
       │                          ▼
       ▼                    FpSearch（多 slice AND）
FPBlockTreeTermsWriter              │
       │                          ▼
FpTokenBlockOrchestrator     terms.fpBits → seek 倒排
       │
       ├── 倒排 (writefp)
       ├── termsbit 侧车 (hot/common tier, v7)
       └── 段 meta fpblock_list (group_id → FpBlockInfo)
```

FPToken **不替代** Lucene 倒排，而是在倒排之上加一层 **「ngram bucket → 组内 term 序号（order）」** 辅助索引，把候选 term 从 O(全段词项) 降到 O(命中 bucket 的 order 数)。

---

## 3. 核心概念

### 3.1 词项布局（`FpTokenTermLayout`）

```text
┌──────────────┬─────────────────┬──────────────┬──────────────────┐
│ 4B 列名长度   │ UTF-8 列名       │ 14B FP 头     │ ngram payload    │
└──────────────┴─────────────────┴──────────────┴──────────────────┘
```

FP 头字段：`index_id` · `group_id` · `group_level` · hot/common 标记 · `termIndex`（= bitindex 里的 **order**）· `hotDownTierBudget`

列名 **不参与** ngram 滑窗，避免 `user.name` 污染 `user.age` 的桶统计。

### 3.2 索引滑窗 vs 查询切片

| 阶段 | 参数 | 说明 |
|------|------|------|
| **建索引** | 窗宽 32B，步进 27B | `BITSET_WINDOW_SIZE=32`，`BITSET_STEP_SIZE=32-NGRAM_MAX` |
| **查询** | 最长 5B，步进 3B | `FpTokenQuery` 用 `NGRAM_MAX` 与 `NGRAM_MAX-2` 切 anchor slice |

查询语义：**多个 slice AND**（子串必须同时出现）；同一 slice 多个 order **OR**；多 group 同 slice **OR**。

**边界**：查询串 ≤5 字节通常 1 个 slice；≥6 字节会切成 2+ slice，必须全部命中。

### 3.3 组（Block Group）

同一 `列名 + index_id + group_id` 下的 term 共享：

- doc 并集 `distinctDocUnion`（判定是否闭块写出）
- termsbit 中一段 **hot tier + common tier**

写段编排（`FpTokenBlockOrchestrator`）：

```text
词项 level ≥ 列 targetLevel  →  FpGroupDataOriginal（高级候选，达标则透传）
词项 level <  targetLevel    →  FpGroupDataRebuild（攒 common，flush 时热词重建）

flush 时
├── 大组达标 → Original 透传 + 复用/写出已有 bitindex
└── 小组     → Rebuild：热词挖掘 → merge doc → 新建 v7 bitindex → writefp
```

闭块阈值（`FpTokenBlockLevelPolicy`）：L1≈512 · L2≈4096 · L3≈8192 · L4≈16384 term/doc（可乘 overRate）。

---

## 4. 写段：热词重建（`FpGroupHotNgramRebuild`）

Rebuild 路径四阶段（日志 `phasesMs=count+build+budget+merge`）：

| 阶段 | 做什么 |
|------|--------|
| **count** | 统计每个 ngram 在多少个 distinct common 词中出现（词内去重） |
| **build** | 频次 ≥ 阈值 → 升格 **hot term**（阈值随 L1~L4：4/8/16/32） |
| **budget** | 计算每个热词锚点的 `hotDownTierBudget`（向下可扩展几档子 ngram） |
| **merge** | 长 ngram 优先，把 common doc 并入匹配 hot posting；父前缀 skip 防重复 merge |

写段日志示例：

```text
commonHotHitMax=800 commonHotHitTier=hit1_50=16,hit51_100=10671,hit101_150=7763
```

表示每个 common payload 内命中 hot 的 ngram **窗口次数**按每 50 一档统计 term 个数（`commonHotHitTierCnt`），仅用于观测。

---

## 5. v7 bucket 索引（`FpGroupHotNgramBitIndex`）

### 5.1 逻辑结构

每组 termsbit 含 **hot / common 两个 tier**，结构相同：

```text
Tier
├── LenRow[0]  … 1 字节 ngram
├── LenRow[1]  … 2 字节
│     …
└── LenRow[4]  … 5 字节

LenRow = sortedKeys[bucket] + entryMeta + orderBytes（多 order 的 vint 池）
         + skip1（每 256 条一段）+ skip2（每 16 条一段）
```

**bucketIndex**（高 8 位 = 长度，低 24 位 = payload）：

| ngram 长度 | 算法 |
|------------|------|
| 1~4 | 大端拼进低 24 位 |
| ≥5 | `murmurhash3_x86_32` 低 24 位 |

**order** = 组内 term 序号（1..N），升序 vint 增量压缩；单 order 可内联在 meta。

### 5.2 写段落盘（`FpBitIndexSegmentStaging`）

v7 按 lenIdx **分池暂存** 到本地目录，再合并进 termsbit：

```text
{hot|common}_{skip1|skip2|keys|order}_{lenIdx}.dat
{hot|common}_tier_dir.dat   ← 164B = magic + 5×4×8 偏移
```

日志 `bytesHot=164 bytesCommon=164` 是 **tier_dir 固定大小**，不是整段体积；看 `hotBits` / `commonBits`。

### 5.3 查询 selective 读盘

```text
1. selectiveKeysForSlices(slices) → packed bucketKey[]
2. 跨 group 三趟读盘（利于 page cache）：
     全体 skip1 → 全体 skip2 → 全体 keys/order
3. lookupHotOrders / lookupCommonOrders → order[]
4. 对每个 order：iterator_fp().seek(term) → posting
5. 多 slice FixedBitSet AND
```

不做 `fpBits(null,null)` 全量回退。极小组（≤128 term）走 `searchSparseNoBitIndexTerms` 暴力 seek。

---

## 6. 查询语义速查

| 维度 | 语义 |
|------|------|
| 多个 slice | **AND** |
| 同一 slice 多个 order | **OR** |
| hot vs common | **先 hot**，hot 未完全覆盖时再扫 common（跳过 hot slice） |
| 列名 | 必须匹配，否则整段无命中 |

调试：JVM `-Dfptoken.search` 或 `Lucene80FPSearchConfig.LOG_FP_SEARCH=true`（查询 trace）；写段摘要始终 INFO `[write]` / `[rebuild]` / `[bitindex]`。

---

## 7. 源码地图

```text
src/cn/lxdb/plugins/muqingyu/fptoken/
├── token/           FpTokenAnalyzer, BinarySlidingWindowApi
├── config/          Lucene80FPSearchConfig, FpTokenBlockLevelPolicy
├── api/             FPBlockTreeTermsWriter, FpTokenBlockOrchestrator,
│                    FpSearch, FpTokenQuery
├── dataset/common/  FpTokenTermLayout, FpBlockInfo, FPDocList, FpStatNgram
└── dataset/block/   FpGroupDataOriginal/Rebuild, FpGroupHotNgramRebuild,
                     FpGroupHotNgramBitIndex, FpBitIndexSegmentStaging
```

| 主题 | 入口类 |
|------|--------|
| 词项布局 | `FpTokenTermLayout` |
| 写段编排 | `FpTokenBlockOrchestrator` |
| 热词 + merge | `FpGroupHotNgramRebuild` |
| bitindex v7 | `FpGroupHotNgramBitIndex` |
| 查询 | `FpSearch` · `FpTokenQuery` |

---

## 8. 构建与测试

### 依赖

从 LXDB 工程同步 `lib/`（含补丁 Lucene）。可选：

```powershell
.\scripts\sync-lib-from-classpath.ps1
```

### 测试

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

- HTML 报告：`build/test-results/junit-html/index.html`
- IDE 已编译：`.\scripts\run-fptoken-tests.ps1 -SkipCompile`

### 部署

编译产物在 `bin/`，打包进 DataNode 插件 jar 后 **重建或 reindex FP 段**（v7 格式变更需全量重建）。

---

## 9. 与 Lucene 的接点

补丁 Lucene（参考 `export/`）：

| 扩展 | 用途 |
|------|------|
| `Terms#iterator_fp()` | 遍历 FP 词项 |
| `Terms#fpBits(..., hotKeys, commonKeys)` | selective / 全量读 tier |
| `TermsWriter#writefp` | 写出 FP term + posting |
| 段 meta `fpblock_list` | `group_id → FpBlockInfo` |
| 侧车 `termsbit` | hot/common tier 二进制 |

---

## 10. 模块边界

**本 repo 负责**：BlockTree FP 写段、组内热词重建、v7 bucket 索引、查询侧 `FpSearch` / `FpTokenQuery`。

**不在本 repo**：互斥频繁项集、Pre-merge hint、行级 Beam 挖掘（历史方案见 `archive/`）；Lucene 补丁本体在 `lib/`。

---

## 11. 文档

| 文档 | 说明 |
|------|------|
| [docs/fp-token-binary-search-presentation.html](docs/fp-token-binary-search-presentation.html) | 业务介绍（HTML 幻灯片） |
| [docs/README.md](docs/README.md) | 文档索引 |
| [AGENTS.md](AGENTS.md) | Agent / 贡献约定 |

---

## 许可

各源文件 `@author` 为准；与 LXDB / Lucene 补丁的分发策略以宿主工程为准。
