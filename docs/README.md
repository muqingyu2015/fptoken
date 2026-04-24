# fptoken 文档索引

本目录为设计与协作文档的**唯一索引**；仓库根 `README.md` 为项目主说明与命令入口。自动化/贡献者速览见仓库根 [`AGENTS.md`](../AGENTS.md)。

## 阅读顺序（建议）

| 顺序 | 文件 | 说明 |
|------|------|------|
| 1 | [fptoken-project-documentation.html](fptoken-project-documentation.html) | 项目总览、模块、运行方式 |
| 2 | [fptoken-usage-and-architecture.html](fptoken-usage-and-architecture.html) | 设计原理与接入示例 |
| 3 | [frequent-itemset-miner-performance.html](frequent-itemset-miner-performance.html) | Beam 挖掘与性能说明 |

## HTML 导航页

- [index.html](index.html) — 站内链接汇总（浏览器打开）

## LXDB / 接入

| 文件 | 说明 |
|------|------|
| [lxdb-integration-derived-data-and-selection-result.html](lxdb-integration-derived-data-and-selection-result.html) | `DerivedData` 与 `selectionResult` 双索引职责 |
| [intro.html](intro.html) | 项目介绍幻灯式页面 |

## 设计与方案

| 文件 | 说明 |
|------|------|
| [fptoken-design-and-implementation.html](fptoken-design-and-implementation.html) | 互斥频繁项集筛选设计 |
| [posting-dedup-and-exclusive-fiset-design.html](posting-dedup-and-exclusive-fiset-design.html) | 倒排与互斥项集 |
| [binary-search-lucene-design.html](binary-search-lucene-design.html) | 二进制检索（Lucene 向）草案 |
| [binary-search-lucene-references.html](binary-search-lucene-references.html) | 参考资料 |

## 测试与性能

| 文件 | 说明 |
|------|------|
| [test-factor-coverage-matrix.md](test-factor-coverage-matrix.md) | 因子覆盖矩阵 |
| [method-coverage-checklist.md](method-coverage-checklist.md) | 方法覆盖清单 |
| [premerge-hint-performance-report.html](premerge-hint-performance-report.html) | Pre-merge hint 性能说明 |
| [premerge-hint-contract.md](premerge-hint-contract.md) | Pre-merge hint 调用契约（何时传、字段要求、回退责任） |
| [performance-benchmark-report.md](performance-benchmark-report.md) | 性能基准记录 |
| [service-level-objectives.md](service-level-objectives.md) | 服务等级指标（SLI/SLO）与架构决策量化门槛 |
| [jmh-benchmark-usage.md](jmh-benchmark-usage.md) | JMH 使用说明（模板） |
| [technical-debt-register.md](technical-debt-register.md) | 技术债务清单（工时与影响范围） |

## 协作草稿（多角色汇总）

| 文件 | 说明 |
|------|------|
| [optimization-summary-collaborative.md](optimization-summary-collaborative.md) | 优化总结与改进意见（多角色汇总） |
| [worklog-collaborative-2026-04-22.md](worklog-collaborative-2026-04-22.md) | 工作日志（多角色汇总） |
| [future-directions-collaborative.md](future-directions-collaborative.md) | 未来展望（多角色汇总） |
| [testing-plan-collaborative.md](testing-plan-collaborative.md) | 测试规划（多角色汇总） |
| [action-log-collaborative-2026-04-22.md](action-log-collaborative-2026-04-22.md) | 行动记录（多角色汇总） |

## 其它

| 文件 | 说明 |
|------|------|
| [test.html](test.html) | 测试相关说明页 |
| [call-chain-demo-ppt.md](call-chain-demo-ppt.md) | 调用链演示提纲 |
