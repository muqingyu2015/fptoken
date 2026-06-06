# fptoken 文档索引

本目录为设计与协作文档的**唯一索引**；仓库根 [README.md](../README.md) 为项目主说明与命令入口。  
**每个 `.html` 均有同名 `.md`**，便于 Git/IDE 阅读；浏览器排版见 HTML 版。

自动化/贡献者速览：[AGENTS.md](../AGENTS.md)

---

## 快速导航

- [index.md](index.md) / [index.html](index.html) — 站内总导航
- [fp-token-design_20260517.md](fp-token-design_20260517.md) — **当前 v3 技术设计（推荐首读）**

---

## 写段调用（FPBlockTreeTermsWriter）

| Markdown | HTML | 说明 |
|----------|------|------|
| [fp-token-write-path-detailed.md](fp-token-write-path-detailed.md) | [html](fp-token-write-path-detailed.html) | 写段总流程 |
| — | [fp-token-write-path-detailed.png](fp-token-write-path-detailed.png) | 高分辨率 PNG |
| [fp-token-write-path-detailed.mmd](fp-token-write-path-detailed.mmd) | — | Mermaid 源 |

重新生成 PNG：`../scripts/render-fp-write-path-diagram.ps1`

---

## 当前模块（BlockTree FP · 2026-06）

| Markdown | HTML | 说明 |
|----------|------|------|
| [fp-token-design_20260517.md](fp-token-design_20260517.md) | [html](fp-token-design_20260517.html) | v3 bucket 倒排、selective 查询、落盘 |
| [fp-token-write-search-alignment-report.md](fp-token-write-search-alignment-report.md) | [html](fp-token-write-search-alignment-report.html) | 写查对齐审查 |
| [fp-token-review-and-test-report_20260517.md](fp-token-review-and-test-report_20260517.md) | [html](fp-token-review-and-test-report_20260517.html) | 审查 + 单测 |
| [fp-token-potential-bugs-review.md](fp-token-potential-bugs-review.md) | [html](fp-token-potential-bugs-review.html) | 遗留 BUG 跟踪 |
| [fptoken-usage-manual.md](fptoken-usage-manual.md) | [html](fptoken-usage-manual.html) | 使用手册 |
| [fptoken-project-documentation.md](fptoken-project-documentation.md) | [html](fptoken-project-documentation.html) | 项目总览 |
| [fptoken-usage-and-architecture.md](fptoken-usage-and-architecture.md) | [html](fptoken-usage-and-architecture.html) | 原理与接入 |
| [intro.md](intro.md) | [html](intro.html) | 项目介绍 |
| [test.md](test.md) | [html](test.html) | 测试报告页 |

---

## LXDB / 接入

| Markdown | HTML |
|----------|------|
| [lxdb-integration-derived-data-and-selection-result.md](lxdb-integration-derived-data-and-selection-result.md) | [html](lxdb-integration-derived-data-and-selection-result.html) |
| [lxdb-marking-design-and-usage.md](lxdb-marking-design-and-usage.md) | [html](lxdb-marking-design-and-usage.html) |

---

## 二进制检索 / 演示

| Markdown | HTML |
|----------|------|
| [binary-search-lucene-design.md](binary-search-lucene-design.md) | [html](binary-search-lucene-design.html) |
| [binary-search-lucene-references.md](binary-search-lucene-references.md) | [html](binary-search-lucene-references.html) |
| [fptoken-binary-search-ppt.md](fptoken-binary-search-ppt.md) | [html](fptoken-binary-search-ppt.html) |
| [binary-retrieval-solution-evaluation-ppt.md](binary-retrieval-solution-evaluation-ppt.md) | [html](binary-retrieval-solution-evaluation-ppt.html) |
| [fptoken-boss-customer-presentation.md](fptoken-boss-customer-presentation.md) | [html](fptoken-boss-customer-presentation.html) |

---

## 历史能力（互斥频繁项集 / Pre-merge hint）

源码不在本 repo；文档仅供对照。

| Markdown | HTML |
|----------|------|
| [fptoken-design-and-implementation.md](fptoken-design-and-implementation.md) | [html](fptoken-design-and-implementation.html) |
| [posting-dedup-and-exclusive-fiset-design.md](posting-dedup-and-exclusive-fiset-design.md) | [html](posting-dedup-and-exclusive-fiset-design.html) |
| [frequent-itemset-miner-performance.md](frequent-itemset-miner-performance.md) | [html](frequent-itemset-miner-performance.html) |
| [hint-support-index-explainer.md](hint-support-index-explainer.md) | [html](hint-support-index-explainer.html) |
| [premerge-hint-performance-report.md](premerge-hint-performance-report.md) | [html](premerge-hint-performance-report.html) |
| [buildWithSupportBounds-performance-improvement-report.md](buildWithSupportBounds-performance-improvement-report.md) | [html](buildWithSupportBounds-performance-improvement-report.html) |
| [premerge-hint-contract.md](premerge-hint-contract.md) | — |

---

## 测试与协作（仅 Markdown）

| 文件 | 说明 |
|------|------|
| [test-factor-coverage-matrix.md](test-factor-coverage-matrix.md) | 因子覆盖矩阵 |
| [method-coverage-checklist.md](method-coverage-checklist.md) | 方法覆盖清单 |
| [performance-benchmark-report.md](performance-benchmark-report.md) | 性能基准 |
| [service-level-objectives.md](service-level-objectives.md) | SLI/SLO |
| [jmh-benchmark-usage.md](jmh-benchmark-usage.md) | JMH 说明 |
| [technical-debt-register.md](technical-debt-register.md) | 技术债务 |
| [optimization-summary-collaborative.md](optimization-summary-collaborative.md) | 优化总结 |
| [worklog-collaborative-2026-04-22.md](worklog-collaborative-2026-04-22.md) | 工作日志 |
| [future-directions-collaborative.md](future-directions-collaborative.md) | 未来展望 |
| [testing-plan-collaborative.md](testing-plan-collaborative.md) | 测试规划 |
| [action-log-collaborative-2026-04-22.md](action-log-collaborative-2026-04-22.md) | 行动记录 |
| [call-chain-demo-ppt.md](call-chain-demo-ppt.md) | 调用链提纲 |
| [agent-memory-context-handoff-2026-04-24.md](agent-memory-context-handoff-2026-04-24.md) | Agent 上下文交接 |
| [fulltext-corpus-data-sources.md](fulltext-corpus-data-sources.md) | 语料数据源 |
| [web-csv-generation-spec.md](web-csv-generation-spec.md) | Web CSV 规范 |

---

## HTML ↔ MD 完整对照

| HTML | Markdown |
|------|----------|
| index.html | [index.md](index.md) |
| fp-token-design_20260517.html | [fp-token-design_20260517.md](fp-token-design_20260517.md) |
| fp-token-write-path-detailed.html | [fp-token-write-path-detailed.md](fp-token-write-path-detailed.md) |
| fp-token-write-search-alignment-report.html | [fp-token-write-search-alignment-report.md](fp-token-write-search-alignment-report.md) |
| fp-token-review-and-test-report_20260517.html | [fp-token-review-and-test-report_20260517.md](fp-token-review-and-test-report_20260517.md) |
| fp-token-potential-bugs-review.html | [fp-token-potential-bugs-review.md](fp-token-potential-bugs-review.md) |
| fptoken-usage-manual.html | [fptoken-usage-manual.md](fptoken-usage-manual.md) |
| fptoken-project-documentation.html | [fptoken-project-documentation.md](fptoken-project-documentation.md) |
| fptoken-usage-and-architecture.html | [fptoken-usage-and-architecture.md](fptoken-usage-and-architecture.html) |
| intro.html | [intro.md](intro.md) |
| test.html | [test.md](test.md) |
| lxdb-integration-derived-data-and-selection-result.html | [lxdb-integration-derived-data-and-selection-result.md](lxdb-integration-derived-data-and-selection-result.md) |
| lxdb-marking-design-and-usage.html | [lxdb-marking-design-and-usage.md](lxdb-marking-design-and-usage.md) |
| binary-search-lucene-design.html | [binary-search-lucene-design.md](binary-search-lucene-design.md) |
| binary-search-lucene-references.html | [binary-search-lucene-references.md](binary-search-lucene-references.md) |
| fptoken-binary-search-ppt.html | [fptoken-binary-search-ppt.md](fptoken-binary-search-ppt.md) |
| binary-retrieval-solution-evaluation-ppt.html | [binary-retrieval-solution-evaluation-ppt.md](binary-retrieval-solution-evaluation-ppt.md) |
| fptoken-boss-customer-presentation.html | [fptoken-boss-customer-presentation.md](fptoken-boss-customer-presentation.md) |
| fptoken-design-and-implementation.html | [fptoken-design-and-implementation.md](fptoken-design-and-implementation.md) |
| posting-dedup-and-exclusive-fiset-design.html | [posting-dedup-and-exclusive-fiset-design.md](posting-dedup-and-exclusive-fiset-design.md) |
| frequent-itemset-miner-performance.html | [frequent-itemset-miner-performance.md](frequent-itemset-miner-performance.md) |
| hint-support-index-explainer.html | [hint-support-index-explainer.md](hint-support-index-explainer.md) |
| premerge-hint-performance-report.html | [premerge-hint-performance-report.md](premerge-hint-performance-report.md) |
| buildWithSupportBounds-performance-improvement-report.html | [buildWithSupportBounds-performance-improvement-report.md](buildWithSupportBounds-performance-improvement-report.md) |

*无 HTML 对应：仅 `.md` 的协作/测试文档见上表「测试与协作」。*
