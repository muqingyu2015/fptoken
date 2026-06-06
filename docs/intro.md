# fptoken 项目介绍

> Markdown 摘要；幻灯式页面见 [HTML](intro.html)。

> **说明**：部分章节面向旧版采样/互斥项集模块；当前 BlockTree FP 见 [fp-token-design_20260517.md](fp-token-design_20260517.md)。

## 1. 解决什么问题

- 二进制/半结构化字段上的子串检索
- 海量稀疏列（JSON key）下的列级隔离

## 2. 总体架构

token 分析 → BlockTree 写段编排 → 热词重建 + v3 bucket 索引 → Lucene 查询

## 3. 抽样机制（重点）

历史文档：行级采样与频繁项挖掘。  
当前 FP 模块：段内组级 ngram 统计与热词阈值（`HOT_TIER_TERM_COUNT_THRESHOLD`）。

## 4. 当前质量与测试覆盖

48 项单测/功能测（`-ExcludePerfTag`）。

## 5. 如何使用（最简版）

```powershell
.\scripts\run-fptoken-tests.ps1
```

## 6. 后续演进建议

- 7+ 字节查询 slice 策略优化
- 段 rebuild 工具链
