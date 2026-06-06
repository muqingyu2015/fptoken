# 热词重建诊断日志收集说明

部署带 `LOG_FP_NGRAM_DIAG` 的 fptoken 后，跑一轮 merge/写段，把下面几类日志发回即可。

## 1. 必收（grep 关键字）

```bash
grep -E 'fp_ngram_diag|rebuild_flush|writeTerms diff|original_flush|term write order|field write finished|KEY_SUSPECT|BUDGET_SUSPECT' your.log
```

| 关键字 | 含义 |
|--------|------|
| `fp_ngram_diag` | 每组 rebuild 热词四阶段耗时与表规模（一行） |
| `fp_ngram_diag_KEY_SUSPECT` | 热词 Map 键异常告警（应不出现） |
| `fp_ngram_diag_BUDGET_SUSPECT` | downTier 预算未参与标记告警 |
| `rebuild_flush diff` | 整组 flush 总耗时 + 原有 `ngramstat` |
| `writeTerms diff` | 整字段写段汇总 |
| `term write order` / `field write finished` | term 字典序检测 |

## 2. `fp_ngram_diag` 一行字段说明

示例：

```text
fp_ngram_diag common=117424 distinctDocUnion=307208 hotFreqThreshold=16 distinct_ngram=... hot_pending=... hot_final=... budget_entries=... phases_ms=count:...+build:...+budget:...+merge:... [ngramstat...]
```

| 字段 | 正常大致关系 |
|------|----------------|
| `hot_pending` ≈ `freqThreshold_keep`（在 ngramstat 里） | 建表热词数 |
| `hot_final` ≈ `hot_pending` | putAll 后热词数 |
| `distinct_ngram` | 滑窗去重后的 ngram 种类数 |
| `ngram_level_ok` + `ngram_level_skip` | 有 merge 时通常 > 0 |
| `phases_ms` | 哪一段最慢（常见 merge ≫ count） |
| `hot_doclist_sparse` / `hot_pending` | merge 后多少热词 posting 已升稀疏位图；新 jar 应明显 >0，且大组接近 `hot_pending` |

## 3. 跑前确认（配置）

- `Lucene80FPSearchConfig.LOG_FP_NGRAM_DIAG = true`（默认 true）
- `PRINT_DEBUG = false`（否则 bitset/term 逐条 INFO 爆炸）
- `CHECK_TERM_WRITE_ORDER = true`（乱序会有 WARN）
- 确认已部署**含 `HotMergeSlot` + `FPDocList` 早稀疏** 的 jar（对比上一版 merge 约减半）

## 4. 必留几行（对比用）

至少保存 **最大 common 组** 各一行（通常 `common=307200` 或接近满块）：

```bash
grep 'fp_ngram_diag common=307200' your.log
grep 'rebuild_flush.*columnLevel:3' your.log | tail -5
grep 'writeTerms diff' your.log | tail -20
```

记录：**开始/结束时间**、表名/字段（如 `fp_token_example_102009`）、是否 concurrent merge（日志里是否有 `SegmentMerger` 并行 flush）。

## 5. 可选（抓栈瓶颈时）

慢的时候打一次 jstack，重点看是否还在：

```bash
grep -E 'FPDocList\.(addAllDocsFrom|mergeSortedDocArrays)|markParentPrefixesSkippedInCommonTerm|hotMergeTable\.get|markCommonNgramsByUniqueSlices' a.txt | head -80
```

若 `mergeSortedDocArrays` 仍占满 CPU 且 `hot_doclist_sparse` 很低，说明早稀疏未生效或 doc 仍走数组路径。

## 6. 可选（内存）

日志里 HTML 行的 `fptokenbitset` / `fptoken` total（若有）截一条，便于和耗时对照。

## 7. 关闭诊断 INFO（保留 WARN）

```java
Lucene80FPSearchConfig.LOG_FP_NGRAM_DIAG = false;
```

`KEY_SUSPECT` / `BUDGET_SUSPECT` 仍会打 WARN。
