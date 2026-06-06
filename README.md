# FPToken

LXDB 上的 **二进制指纹（Fingerprint）检索插件**。在 Lucene BlockTree 写段阶段，把字段字节流切成 ngram 词项，按「列 + 组」聚合，构建 **hot/common 双套 bucket 倒排**，查询时按子串切片快速定位 doc，而不是扫全段倒排。

包名：`cn.lxdb.plugins.muqingyu.fptoken`  
字段后缀：`*_bfp`（二进制）、`*_sfp`（字符串）

本仓库是 **2026-05 重写** 后的独立源码，需与 **补丁版 Lucene 8.9 + LXDB `lib/`** 联编后部署为 `mqy_fptoken_*.jar`。

---

## 解决什么问题

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
scripts/run-fptoken-tests.ps1    编译 + 跑测（canonical）
lib/                               从 LXDB 工程同步的 JAR（补丁 Lucene 等）
export/                            补丁 Lucene 源码参考（FieldReader#fpBits 等）
```

---

## 核心概念

### 词项长什么样

```text
[4B 列名长度][UTF-8 列名][14B FP头][ngram 载荷]
```

- **列名**：例如 JSON 字段 `txt4_sfp` 对应的 key；查询时必须一致。
- **载荷**：仅指纹字节；**列名不参与** ngram 统计与 bucket 计算。
- **FP 头**：`index_id`、`group_id`、级别、hot 标记、组内 `termIndex` 等。

细节见 [`FpTokenTermLayout.java`](src/cn/lxdb/plugins/muqingyu/fptoken/dataset/common/FpTokenTermLayout.java)。

### 组（group）

同一 **列名 + index_id + group_id** 下的词项属于一组：

- 共享组级 doc 并集与一块 bucket 索引（`FpBlockInfo` 指向 termsbit 里的 hot/common tier）。
- 写段时换列或换组会 flush；小组走 **重建**，大组达标可 **透传** 已有 `fpBits`。

### v3 bucket 索引（当前格式）

`FpBlockInfo.FORMAT_VERSION = 3`。每组 termsbit 里两块 **tier**（hot / common），每块 6 个 **LenRow**（对应 ngram 长度 1~6）：

- 每个 LenRow：`bucket → order[]` 升序表 + skip 表（每 128 条一条，用于 selective 跳跃读）。
- **bucketIndex**：1~4 字节大端拼 `int`；5~6 字节用 murmurhash。

变更 bucket 规则或升级 jar 后，**旧段需重建**。

### 查询路径要点

- `FpSearch.loadBitIndex` → `terms.fpBits(indexId, groupId, keys, keys)`。
- `readfromBanksSelective` 在返回前用 skip 读好 order，**不持有** `IndexInput`。
- 不做全量 `fpBits(null, null)` 回退；无 bit 索引的极小组从 `maxGroupId+1` 做 sparse seek 补充。

配置常量：`Lucene80FPSearchConfig`（`NGRAM_MIN/MAX=1/6`，`HOT_TIER_TERM_COUNT_THRESHOLD=64`）。

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
