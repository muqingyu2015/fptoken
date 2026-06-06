# fptoken 文档导航

> Markdown 便于 Git/IDE 阅读；浏览器导航页见 [index.html](index.html)。

文档版本：**2026-06**（v3 bucket 索引、selective 查询、1~4 字节 bucket 直拼 int）

验证命令（仓库根）：

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

---

## Markdown 总索引

- [docs/README.md](README.md) — 全目录 HTML ↔ MD 对照表

---

## 当前模块（优先阅读）

| 顺序 | Markdown | HTML | 说明 |
|------|----------|------|------|
| 0 | [fp-token-design_20260517.md](fp-token-design_20260517.md) | [html](fp-token-design_20260517.html) | **v3 技术设计**（bucket 倒排、selective 读、落盘格式） |
| 1 | [fp-token-write-path-detailed.md](fp-token-write-path-detailed.md) | [html](fp-token-write-path-detailed.html) | 写段调用总流程（PNG/Mermaid 源） |
| 2 | [fp-token-write-search-alignment-report.md](fp-token-write-search-alignment-report.md) | [html](fp-token-write-search-alignment-report.html) | 写段与查询对齐审查 |
| 3 | [fp-token-review-and-test-report_20260517.md](fp-token-review-and-test-report_20260517.md) | [html](fp-token-review-and-test-report_20260517.html) | 代码审查 + 单测 |
| 4 | [fp-token-potential-bugs-review.md](fp-token-potential-bugs-review.md) | [html](fp-token-potential-bugs-review.html) | 遗留缺陷跟踪 |

---

## LXDB 接入

| Markdown | HTML |
|----------|------|
| [lxdb-integration-derived-data-and-selection-result.md](lxdb-integration-derived-data-and-selection-result.md) | [html](lxdb-integration-derived-data-and-selection-result.html) |
| [lxdb-marking-design-and-usage.md](lxdb-marking-design-and-usage.md) | [html](lxdb-marking-design-and-usage.html) |

---

## 使用与架构

| Markdown | HTML |
|----------|------|
| [fptoken-usage-manual.md](fptoken-usage-manual.md) | [html](fptoken-usage-manual.html) |
| [fptoken-project-documentation.md](fptoken-project-documentation.md) | [html](fptoken-project-documentation.html) |
| [fptoken-usage-and-architecture.md](fptoken-usage-and-architecture.md) | [html](fptoken-usage-and-architecture.html) |
| [intro.md](intro.md) | [html](intro.html) |

---

## 二进制检索 / 演示 PPT

| Markdown | HTML |
|----------|------|
| [binary-search-lucene-design.md](binary-search-lucene-design.md) | [html](binary-search-lucene-design.html) |
| [binary-search-lucene-references.md](binary-search-lucene-references.md) | [html](binary-search-lucene-references.html) |
| [fptoken-binary-search-ppt.md](fptoken-binary-search-ppt.md) | [html](fptoken-binary-search-ppt.html) |
| [binary-retrieval-solution-evaluation-ppt.md](binary-retrieval-solution-evaluation-ppt.md) | [html](binary-retrieval-solution-evaluation-ppt.html) |
| [fptoken-boss-customer-presentation.md](fptoken-boss-customer-presentation.md) | [html](fptoken-boss-customer-presentation.html) |

---

## 历史能力（互斥频繁项集 / Pre-merge hint）

本仓库 **2026-05 重写** 后生产代码为 BlockTree FP 模块；下列文档描述旧版或并行能力，**仅作参考**。

| Markdown | HTML |
|----------|------|
| [fptoken-design-and-implementation.md](fptoken-design-and-implementation.md) | [html](fptoken-design-and-implementation.html) |
| [posting-dedup-and-exclusive-fiset-design.md](posting-dedup-and-exclusive-fiset-design.md) | [html](posting-dedup-and-exclusive-fiset-design.html) |
| [frequent-itemset-miner-performance.md](frequent-itemset-miner-performance.md) | [html](frequent-itemset-miner-performance.html) |
| [hint-support-index-explainer.md](hint-support-index-explainer.md) | [html](hint-support-index-explainer.html) |
| [premerge-hint-performance-report.md](premerge-hint-performance-report.md) | [html](premerge-hint-performance-report.html) |
| [buildWithSupportBounds-performance-improvement-report.md](buildWithSupportBounds-performance-improvement-report.md) | [html](buildWithSupportBounds-performance-improvement-report.html) |
| [premerge-hint-contract.md](premerge-hint-contract.md) | （仅 MD） |

---

## 测试

| Markdown | HTML |
|----------|------|
| [test.md](test.md) | [html](test.html) |
| [test-factor-coverage-matrix.md](test-factor-coverage-matrix.md) | — |
| [method-coverage-checklist.md](method-coverage-checklist.md) | — |
