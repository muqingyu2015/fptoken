# FPToken（FP Token 模块）

面向 **LXDB / 补丁版 Lucene** 的二进制指纹（Fingerprint）索引：在 BlockTree 写段阶段按 **组（index_id + group_id）** 聚合词项，对 common 载荷做 **byte n-gram 热词挖掘** 与 **8×256 双套位图索引**，并支持高级别 term 的 **透传写出**（复用已有 `fpBits`）。

> 本仓库为 **2026-05 重写** 后的独立模块源码；须与完整 LXDB 工程（含补丁 Lucene）联编。设计说明见 [`docs/fp-token-design_20260517.html`](docs/fp-token-design_20260517.html)。

---

## 阅读导航

| 文档 | 说明 |
|------|------|
| [本 README § 写段架构与调用详解](#写段架构与调用详解入口fpblocktreetermswriter) | **从 `FPBlockTreeTermsWriter` 起的 Mermaid 流程图 / 调用树 / 类职责图** |
| [`docs/fp-token-design_20260517.html`](docs/fp-token-design_20260517.html) | 技术设计（类职责、数据流、落盘格式） |
| [`docs/fp-token-review-and-test-report_20260517.html`](docs/fp-token-review-and-test-report_20260517.html) | 代码审查 + 单元测试结果 + 潜在缺陷清单 |
| [`docs/README.md`](docs/README.md) | 历史/协作文档索引 |
| [`AGENTS.md`](AGENTS.md) | 贡献者与 Agent 速览 |

---

## 依赖

| 依赖 | 路径 / 说明 |
|------|-------------|
| **完整 `lib/`** | 与 Eclipse **`.classpath`** 对齐，约 **203** 个 JAR（`lxdb_common`、`lxdb_bigtable`、补丁 Lucene 8.9、slf4j/log4j、Tika/POI、JUnit 等）。从 LXDB 工程拷贝到 `lib/`；校验：`.\scripts\sync-lib-from-classpath.ps1` |
| **补丁 Lucene** | 由 `lib/` 提供：`Terms#iterator_fp()`、`Terms#fpBits`、`BlockTreeTermsWriter#writefp`、`TermsWriter` 等 |
| **JUnit** | `scripts/run-fptoken-tests.ps1` 可自动下载 JUnit Platform；另可自动拉取 `commons-logging-1.2.jar` |

在 **`lib/` 齐全** 时，脚本对 **全部** `src/cn` 执行 `javac` 并运行单测。若缺少补丁 JAR，脚本会回退到较小可编译子集（仅用于应急，见下文）。

---

## 包结构（`cn.lxdb.plugins.muqingyu.fptoken`）

```
token/          FPToken、FpTokenAnalyzer、BinarySlidingWindowApi（64B 窗 / 32B 步）
config/         FpTokenBlockLevelPolicy、Lucene80FPSearchConfig（字段后缀 _bfp / _sfp）
api/            FPBlockTreeTermsWriter、FpTokenBlockOrchestrator、FpFilteredTermsEnum
dataset/common/ FpTokenTermLayout、FpTermKey、FPDocList、FpBlockInfo、组 KV 容器
dataset/block/  FpGroupDataOriginal / Rebuild、FpGroupHotNgramRebuild、FpGroupHotNgramBitIndex
```

---

## 写段架构与调用详解（入口：`FPBlockTreeTermsWriter`）

本节以流程图说明写段全路径（入口 `api/FPBlockTreeTermsWriter`）。FP 字段：`Lucene80FPSearchConfig.isFpField`（`_bfp` / `_sfp`）。落盘细节见 [`docs/fp-token-design_20260517.html`](docs/fp-token-design_20260517.html)。

> 下图使用 Mermaid。请在支持 Mermaid 的预览中查看（VS Code、GitHub、GitLab 等）。

### 1. 与宿主 Lucene 的衔接

```mermaid
flowchart LR
  subgraph Host["补丁 Lucene（宿主）"]
    BTW[BlockTreeTermsWriter<br/>写 FP 字段]
    IT[Terms.iterator_fp]
    WF[TermsWriter.writefp]
    FB[Terms.fpBits]
    BIT[(termsbit / bitOut 文件)]
  end

  subgraph Module["fptoken 模块"]
    FPW[FPBlockTreeTermsWriter]
    FL[fpblock_list<br/>group_id → FpBlockInfo]
    OR[FpTokenBlockOrchestrator]
    FLU[FpGroupData*.flushto]
    IDX[FpGroupHotNgramBitIndex.flushto]
  end

  BTW -->|构造| FPW
  FPW -->|遍历| IT
  FPW --> OR
  OR --> FLU --> WF
  FLU -->|透传路径| FB
  FLU --> IDX --> BIT
  IDX --> FL
  FPW --- FL
  FPW --- BIT
```

---

### 2. 类职责与依赖（按包）

```mermaid
flowchart TB
  subgraph TOK["token/ · 索引上游（不写段）"]
    direction TB
    T1[FpTokenAnalyzer<br/>Lucene Analyzer 入口]
    T2[FPToken<br/>滑窗 → 带 FP 头的 term]
    T3[FpTokenBytesMode · BinarySlidingWindowApi<br/>字节模式 / 64×32 滑窗]
    T1 --> T2
    T2 -.-> T3
  end

  subgraph CFG["config/"]
    C1[Lucene80FPSearchConfig<br/>isFpField · NGRAM 1~6 · 阈值32 · BUCKETS256]
    C2[FpTokenBlockLevelPolicy<br/>resolveTargetBlockLevel<br/>shouldCompleteBlock]
  end

  subgraph API["api/ · 写段入口"]
    A1[FPBlockTreeTermsWriter<br/>writeTerms · fpblock_list · bitOut]
    A2[FpTokenBlockOrchestrator<br/>acceptTerm · finish · flush*]
    A3[FpFilteredTermsEnum<br/>合并索引注入 index_id 前缀]
    A1 --> A2
  end

  subgraph COM["dataset/common/"]
    direction TB
    M1[FpTokenTermLayout<br/>13 字节头 + payload]
    M2[FpTermKey<br/>Map 键 · ORDER_BY_LENGTH_THEN_BYTES]
    M3[FPDocList<br/>doc 列表 int[] / BitSet]
    M4[FpBlockInfo<br/>位图区元数据]
    M5[FpGroupKVOriginal / FpGroupKVRebuild<br/>6 字节组号 + 组数据]
    M6[FpStat · FpStatNgram<br/>段级 / ngram 统计]
    M5 --> M1
    M5 --> M3
  end

  subgraph BLK["dataset/block/"]
    B1[FpGroupDataOriginal<br/>高级别缓冲 hot+common]
    B2[FpGroupDataRebuild<br/>可合并缓冲 仅 common]
    B3[FpGroupHotNgramRebuild<br/>common→hot · maxDown]
    B4[FpGroupHotNgramBitIndex<br/>8×256 双套位图]
    B5[AnchorTierIndex<br/>热词锚点分档索引]
    B2 --> B3 --> B4
    B3 -.-> B5
  end

  TOK -.->|产出 term| API
  CFG --> A2
  A2 --> M5
  A2 --> B1
  A2 --> B2
  B1 --> M4
  B2 --> M4
  A3 -.->|检索侧| M1
```

```mermaid
flowchart TD
  subgraph Policy["FpTokenBlockLevelPolicy 判定逻辑"]
    P0{max maxDoc, termGuess<br/>≥ 100000?}
    P0 -->|是| P3[targetLevel = 3]
    P0 -->|否| P1[targetLevel = 1]
    P3 --> P4{distinctDoc ≥ 阈值<br/>或 distinctTerm ≥ 阈值?}
    P1 --> P4
    P4 -->|是| P5[闭块：应 flush 写出]
    P4 -->|否| P6[继续缓冲]
  end
```

---

### 3. FP 词项字节布局（`FpTokenTermLayout`）

```mermaid
flowchart LR
  subgraph Header["13 字节 FP 头"]
    direction LR
    H0["+0..1 index_id"]
    H1["+2..5 group_id"]
    H2["+6 level"]
    H3["+7 hot=1/common=0"]
    H4["+8..11 termIndex+isDel"]
    H5["+12 hotScanLevel maxDown"]
    H0 --> H1 --> H2 --> H3 --> H4 --> H5
  end
  Header --> Payload["+13.. payload<br/>纯指纹字节 n-gram"]
```

---

### 4. 主调用树流程图（`writeTerms` 全展开）

```mermaid
flowchart TD
  ROOT([BlockTreeTermsWriter 写 FP 字段])
  ROOT --> W1[FPBlockTreeTermsWriter.writeTerms]

  W1 --> W2[terms.iterator_fp]
  W1 --> W3[new FpTokenBlockOrchestrator]
  W3 --> W3a[FpTokenBlockLevelPolicy.resolveTargetBlockLevel]

  W1 --> LOOP{{每个 term}}
  LOOP --> AT[FpTokenBlockOrchestrator.acceptTerm]
  AT --> AT1[见 §5 acceptTerm 流程图]

  W1 --> FIN[orchestrator.finish]
  FIN --> FH[flushHighGroup · §6]
  FIN --> FC[flushCommonGroup]

  FC --> FT[FpGroupDataRebuild.flushto · §7]

  FT --> NG[FpGroupHotNgramRebuild.execute · §8]
  NG --> NG1[countNgramOccurrencesInCommon]
  NG --> NG2[buildHotTermsAndAnchorTierIndex]
  NG --> NG3[computeMaxDownLevels]
  NG --> NG4[mergeCommonDocsIntoHotTerms]
  NG4 --> NG4a[markParentPrefixesSkippedInCommonTerm]

  FT --> BI[FpGroupHotNgramBitIndex.execute · §9]
  BI --> BI1[rebuildHotTermOrderFromHotDocs]
  BI --> BI2[rebuildCommonTermToOrderFromHotDocs]
  BI --> BI3[markNgramsForPayload × hot/common]
  BI --> BI4[flushto → FpBlockInfo]

  FT --> WH[遍历 hotTermToDocs]
  WH --> MK[FpTokenTermLayout.make_fp_term]
  MK --> WP[TermsWriter.writefp]

  FT --> WC[遍历 commonTermToDocs]
  WC --> MK2[make_fp_term COMMON]
  MK2 --> WP2[writefp]

  FH --> FH1{体量达标?}
  FH1 -->|透传| FO[FpGroupDataOriginal.flushto]
  FO --> FB[terms.fpBits]
  FO --> WP3[writefp 读原头]
  FH1 -->|降级| MR[mergeIntoRebuild → Rebuild 路径]

  FT --> PL[fpblock_list.put group_id]
  BI4 --> PL
```

```mermaid
sequenceDiagram
  autonumber
  participant Host as BlockTreeTermsWriter
  participant FPW as FPBlockTreeTermsWriter
  participant Orch as FpTokenBlockOrchestrator
  participant Reb as FpGroupDataRebuild

  Host->>FPW: writeTerms
  FPW->>Orch: new + resolveTargetBlockLevel
  loop 每个 term
    FPW->>Orch: acceptTerm
  end
  FPW->>Orch: finish → flushHigh → flushCommon
  Orch->>Reb: flushto → ngram → bitindex → writefp
```

---

### 5. `acceptTerm` 流程图（组缓冲 + 分流）

`iterator_fp` 保证同 `(index_id, group_id)` 连续；`indexAndGroupEquals` 检测换组。

```mermaid
flowchart TD
  Start([acceptTerm]) --> ChgH{group_original 存在<br/>且组号变化?}
  ChgH -->|是| FH[flushHighGroup<br/>原理：先结算上级候选组]
  ChgH -->|否| ChgC
  FH --> ChgC{group_common 存在<br/>且组号变化?}
  ChgC -->|是| TC[tryFlushCommonIfComplete<br/>原理：仅体量达标才提前闭块]
  ChgC -->|否| Lev
  TC --> Lev{term.level<br/>≥ targetLevel?}

  Lev -->|是| EnsO[必要时 new FpGroupKVOriginal]
  EnsO --> IngO[FpGroupDataOriginal.ingest<br/>热词+common 入桶<br/>更新 distinctDocUnion]
  IngO --> End([返回])

  Lev -->|否| EnsC[必要时 new FpGroupKVRebuild]
  EnsC --> IngC[FpGroupDataRebuild.ingest<br/>isHotTerm→跳过<br/>仅 common 累积 posting]
  IngC --> End
```

---

### 6. `flushHighGroup`：透传 vs 降级

```mermaid
flowchart TD
  FH([flushHighGroup]) --> E0{group_original<br/>为空?}
  E0 -->|是| R0([return])
  E0 -->|否| E1[统计 distinctDocUnion<br/>与 termCount]
  E1 --> E2{shouldCompleteBlock<br/>体量达标?}

  E2 -->|是 · 透传| P1[Terms.fpBits<br/>复用已有位图]
  P1 --> P2[FpGroupDataOriginal.flushto<br/>writefp 热词+common<br/>level/index 读原头]
  P2 --> P3[bitOut + fpblock_list]

  E2 -->|否 · 降级| D1[mergeIntoRebuild<br/>并入 FpGroupDataRebuild]
  D1 --> D2[tryFlushCommonIfComplete]

  P3 --> CLR[group_original = null]
  D2 --> CLR
```

```mermaid
sequenceDiagram
  participant Orch as FpTokenBlockOrchestrator
  participant Policy as FpTokenBlockLevelPolicy
  participant Orig as FpGroupDataOriginal
  participant Reb as FpGroupDataRebuild
  participant Terms as Terms
  participant Bit as FpGroupHotNgramBitIndex
  participant TW as writefp

  Orch->>Orch: flushHighGroup()
  alt group_original == null
    Orch-->>Orch: return
  else 统计 distinctDocUnion / termCount
    Orch->>Policy: shouldCompleteBlock(1, targetLevel, docs, terms)
    alt 体量达标（透传）
      Orch->>Terms: fpBits(index_id, group_id, null, null)
      Terms-->>Bit: 已有位图（可能为 null）
      Orch->>Orig: flushto(orchestrator, bits)
      loop 热词 entrySet
        Orig->>TW: make_fp_term + writefp<br/>level/index 从原 term 头读取
      end
      loop common entrySet
        Orig->>TW: writefp common
      end
      Orig->>Bit: flushto(bitOut) 或复用
      Orig->>Orch: fpblock_list.put(group_id, FpBlockInfo)
    else 体量不足（降级）
      Orch->>Reb: mergeIntoRebuild(group_common.val)<br/>doc 并集 + hot/common 桶合并
      Orch->>Orch: tryFlushCommonIfComplete()
    end
    Orch->>Orch: group_original = null
  end
```

---

### 7. `flushCommonGroup` → `FpGroupDataRebuild.flushto`

```mermaid
sequenceDiagram
  participant Orch as FpTokenBlockOrchestrator
  participant Reb as FpGroupDataRebuild
  participant Ngram as FpGroupHotNgramRebuild
  participant Bit as FpGroupHotNgramBitIndex
  participant Layout as FpTokenTermLayout
  participant TW as writefp
  participant BitOut as bitOut

  Orch->>Reb: flushto(orchestrator, false)
  Reb->>Ngram: execute(group, orchestrator, threshold=32)
  Note over Ngram: 清空 hotTermToDocs/Level，<br/>从 common 重建热词，见 §8
  Ngram-->>Reb: hotTermToDocs 已填充

  Reb->>Bit: execute(group)
  Note over Bit: rebuildHotTermOrder +<br/>markNgramsForPayload，见 §9
  Bit-->>Reb: FpGroupHotNgramBitIndex

  Reb->>Reb: group_id = groupIndex++
  loop hotTermToDocs.entrySet<br/>ORDER_BY_LENGTH_THEN_BYTES
    alt docsize==0 占位热词
      Reb->>Reb: addDoc(del_term_docid)
    end
    Reb->>Layout: make_fp_term(HOT, index, maxDown, payload)
    Reb->>TW: writefp(reuse_term, FPDocList)
  end
  loop commonTermToDocs.entrySet
    Reb->>Layout: make_fp_term(COMMON, …)
    Reb->>TW: writefp（跳过空 doc）
  end
  Reb->>Bit: flushto(bitOut) → FpBlockInfo
  Reb->>Orch: fpblock_list.put(group_id, info)
  Reb->>Reb: resetAfterFlush()
  Orch->>Orch: group_common = null
```

---

### 8. `FpGroupHotNgramRebuild.execute` 流程图

```mermaid
flowchart TB
  START([execute 开始<br/>清空 hotTermToDocs / hotTermToLevel])

  subgraph S1["① countNgramOccurrencesInCommon"]
    direction TB
    A1[输入: commonTermToDocs]
    A2[每条 payload 滑窗 n=1..6]
    A3[单 common 内 ngram 去重]
    A4[输出: ngramOccurrenceCount<br/>跨 common 词项出现次数]
    A1 --> A2 --> A3 --> A4
  end

  subgraph S2["② buildHotTermsAndAnchorTierIndex"]
    direction TB
    B1[输入: 频次表]
    B2{count ≥ 32?}
    B3[输出: 热词键 + 空 FPDocList]
    B4[AnchorTierIndex 登记子串<br/>按 byteLen 分档 · 单档 cap 64]
    B1 --> B2
    B2 -->|是| B3 --> B4
    B2 -->|否| SKIP[跳过]
  end

  subgraph S3["③ computeMaxDownLevels"]
    direction TB
    C1[输入: anchorTierIndexByHotTerm]
    C2[从锚点长度向下累加各档 size]
    C3{累加 > 32?}
    C4[输出: hotTermToLevel maxDown<br/>控制查询向下遍历深度]
    C1 --> C2 --> C3
    C3 -->|否| C2
    C3 -->|是| C4
  end

  subgraph S4["④ mergeCommonDocsIntoHotTerms"]
    direction TB
    D1[输入: common doclist]
    D2[长→短扫描命中热词]
    D3[addAllDocsFrom]
    D4{extension ≤ maxDown?}
    D5[标记父前缀 skip<br/>本 common 内不再重复 merge]
    D6[输出: hotTermToDocs 含 posting<br/>深档不拼回时父词仍可有 doc]
    D1 --> D2 --> D3 --> D4
    D4 -->|是| D5
    D4 -->|否| D6
    D5 --> D6
  end

  START --> S1 --> S2 --> S3 --> S4
  S4 --> END([putAll → group.hotTermToDocs])
```

---

### 9. `FpGroupHotNgramBitIndex` 流程图

```mermaid
flowchart TB
  E([execute]) --> O1[rebuildHotTermOrderFromHotDocs<br/>热词编号 1..H]
  O1 --> O2[rebuildCommonTermToOrderFromHotDocs<br/>common 编号 1..C]
  O2 --> A1[allocBanks<br/>hot: 6×256 · common: 6×256]

  A1 --> H1[遍历每条热词 payload]
  H1 --> H2[滑窗 ngram len 1..6]
  H2 --> H3[bucketIndex<br/>1B→字节值 · 多B→hash%256]
  H3 --> H4[FixedBitSet.set order-1]

  A1 --> C1[遍历每条 common payload]
  C1 --> C2{切片已是热词整键?}
  C2 -->|是| SKIP[不写 common 侧]
  C2 -->|否| C3[bucket + set bit]

  H4 --> F([flushto bitOut])
  C3 --> F
  F --> F1[写 0,0 测序列化字节宽]
  F1 --> F2[填充 FpBlockInfo 偏移]
  F2 --> F3[交错写其余 li,bucket 对]
  F3 --> F4[fpblock_list.put]
```

---

### 10. 运行时对象关系图（单字段写段会话）

```mermaid
flowchart TB
  FPW[FPBlockTreeTermsWriter]
  FPW --> fpbl[(fpblock_list)]
  FPW --> bitOut[(bitOut IndexOutput)]
  FPW --> Orch[FpTokenBlockOrchestrator]

  Orch --> stat[FpStat 统计]
  Orch --> gi[groupIndex 递增分配 group_id]
  Orch --> tw[termsWriter.writefp]
  Orch --> gO[group_original]
  Orch --> gC[group_common]

  gO --> kO[6B 组号 key]
  gO --> DO[FpGroupDataOriginal]
  DO --> DOh[hotTermToDocs]
  DO --> DOc[commonTermToDocs]
  DO --> DOu[distinctDocUnion]

  gC --> kC[6B 组号 key]
  gC --> DR[FpGroupDataRebuild]
  DR --> DRc[commonTermToDocs · ingest 写入]
  DR --> DRu[distinctDocUnion]
  DR -->|flushto 后| DRh[hotTermToDocs]
  DR --> DRl[hotTermToLevel maxDown]
  DR --> DRo[hotTermToOrder]
  DRh --> WP[writefp]
  DRc --> WP
  DR --> BI[FpGroupHotNgramBitIndex]
  BI --> fpbl
  BI --> bitOut
```

---

### 11. 双路径决策流程图

```mermaid
flowchart TB
  T([每个 term]) --> Q1{level ≥ targetLevel?}

  Q1 -->|是| PATH_O[高级别路径 FpGroupDataOriginal]
  Q1 -->|否| PATH_R[可合并路径 FpGroupDataRebuild]

  subgraph ORIG["透传路径 · 目标：大组省 CPU"]
    direction TB
    O1[ingest: 热词 + common 保留 FP 头]
    O2[flushHighGroup]
    O3{体量达标?}
    O4[fpBits 复用位图]
    O5[flushto writefp<br/>maxDown 读原头]
    O1 --> O2 --> O3
    O3 -->|是| O4 --> O5
    O3 -->|否| O6[mergeIntoRebuild → 并入 Rebuild 路径]
  end

  subgraph REB["重建路径 · 目标：小组合并 + ngram 索引"]
    direction TB
    R1[ingest: 仅 common]
    R2[flushCommonGroup]
    R3[FpGroupHotNgramRebuild<br/>挖热词 · maxDown]
    R4[FpGroupHotNgramBitIndex<br/>新建 8×256 位图]
    R5[writefp 热词序: 长度→字典序]
    R1 --> R2 --> R3 --> R4 --> R5
  end

  PATH_O --> ORIG
  PATH_R --> REB
  O6 --> REB
```

---

### 12. 检索侧流程（写段产出 → 查询消费）

```mermaid
flowchart LR
  subgraph Write["写段产出"]
    W1[writefp terms]
    W2[bitOut + FpBlockInfo]
  end

  subgraph Read["检索（LXDB 查询模块 + 本仓库部分类）"]
    R1[FpFilteredTermsEnum<br/>合并索引注入 2B index_id]
    R2[读 term + hotScanLevel]
    R3[FpBlockInfo → 定位 bank]
    R4[按 maxDown 向下拼子档 posting]
    R1 --> R2 --> R3 --> R4
  end

  W1 --> R2
  W2 --> R3
```

---

### 13. 单页总览（层次图）

```mermaid
flowchart TB
  subgraph L0["L0 宿主"]
    H0[BlockTreeTermsWriter]
  end
  subgraph L1["L1 入口 api"]
    H1[FPBlockTreeTermsWriter.writeTerms]
  end
  subgraph L2["L2 编排 api"]
    H2[FpTokenBlockOrchestrator]
    H2a[acceptTerm / finish]
    H2b[flushHigh / flushCommon]
  end
  subgraph L3["L3 组缓冲 dataset/block"]
    H3a[FpGroupDataOriginal]
    H3b[FpGroupDataRebuild]
  end
  subgraph L4["L4 重建 dataset/block"]
    H4a[FpGroupHotNgramRebuild]
    H4b[FpGroupHotNgramBitIndex]
  end
  subgraph L5["L5 落盘"]
    H5a[TermsWriter.writefp]
    H5b[bitOut + FpBlockInfo]
  end

  H0 --> H1 --> H2
  H2 --> H2a --> H3a
  H2a --> H3b
  H2 --> H2b --> H3a
  H2b --> H3b --> H4a --> H4b --> H5a
  H4b --> H5b
  H3a --> H5a
  H3a --> H5b
```

---

## 构建与测试

**推荐脚本**（仓库根目录）：

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

| 场景 | 命令 |
|------|------|
| 默认单元测试（排除 `lxdb-runtime`、`performance`） | 上式或 `.\scripts\run-fptoken-tests.ps1` |
| 含依赖完整 LXDB 运行时的用例 | `.\scripts\run-fptoken-tests.ps1 -IncludeLxdbRuntimeTag`（须在完整 classpath 下） |
| 已用 IDE 与 LXDB 全量编译 | `.\scripts\run-fptoken-tests.ps1 -SkipCompile` |

**报告目录**（可删除）：`build/test-results/junit-html/index.html`

**编译说明**：

1. 运行 classpath：`bin` → `bin-test` → `lib/*.jar`。
2. 默认尝试编译全部 `src/cn`（需完整 `lib/`）。
3. 若失败，回退编译子集（`token/` 部分类 + `dataset/common` 等）；完整模块仍应在 LXDB IDE 或补齐 `lib/` 后编译。

**测试包**：`src/test/java/cn/lxdb/plugins/muqingyu/fptoken/tests/unit/`

---

## 与旧版 fptoken（互斥频繁项集 / Pre-merge hint）的关系

本仓库 **已不再包含** 旧版 `ExclusiveFpRowsProcessingApi`、采样挖掘、Pre-merge hint 等实现；相关文档若仍出现在 `docs/` 下，仅作历史参考。新模块解决的是 **Lucene 段内 FP 字段的写段编排与 n-gram 位图**，与「行级互斥项集三层输出」是不同层次的能力，可在 LXDB 产品内组合使用。

---

## 已知问题（摘要）

完整列表与测试证据见 [`docs/fp-token-review-and-test-report_20260517.html`](docs/fp-token-review-and-test-report_20260517.html)。摘要：

- **P0 / P1**：无开放项（审查中的逻辑/行为点均已按产品约定撤回，见报告 §4）。
- **P2（可选）**：块级别策略注释、Javadoc/类名一致性与集成测覆盖（BUG-201～204）。

---

## 许可与归属

模块作者见各源文件 `@author`；与 LXDB/Lucene 补丁的版权与分发策略以宿主工程为准。
