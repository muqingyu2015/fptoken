# FPToken 设计原理与使用指南

> Markdown 摘要；流程图与接入示例见 [HTML](fptoken-usage-and-architecture.html)。

> **2026 全量更新说明**：本 HTML 部分章节仍描述旧版互斥项集 API；当前生产路径为 BlockTree FP，见 [fp-token-design_20260517.md](fp-token-design_20260517.md)。

## 1. 设计思路

- 二进制滑窗 ngram 作为词项，列名前缀隔离稀疏 JSON 列。
- 组内热词挖掘减少 posting 体积；bucket 倒排 O(1) 定位 order。

## 2. 核心原理

- **写**：闭块判定 → 透传或重建 → termsbit + 倒排。
- **查**：slice → bucketKey → selective 读 order → seek 倒排 → doc AND。

## 3. 调用流程图

见 [fp-token-write-path-detailed.md](fp-token-write-path-detailed.md)。

## 4. 使用方法

- 索引：`FpTokenAnalyzer` + 补丁 Lucene 写段
- 查询：`FpTokenQuery` 传入列名与查询字节

## 5. 运行测试

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```
