# fptoken 项目文档

> Markdown 摘要；完整架构图见 [HTML](fptoken-project-documentation.html)。  
> **2026-06**：请以 [fp-token-design_20260517.md](fp-token-design_20260517.md) 为准补充 v3 位图索引说明。

## 目录

1. 项目说明
2. 项目背景
3. 架构设计
4. 流程图
5. 实现原理
6. 重要实现与原理
7. 类调用关系与原理
8. 附录：配置参数与行为约束

## 要点

- **目标**：LXDB 上二进制指纹字段的 BlockTree 写段与 ngram 检索。
- **包**：`cn.lxdb.plugins.muqingyu.fptoken`
- **写段**：`FPBlockTreeTermsWriter` → Orchestrator → Original/Rebuild
- **索引**：`FpGroupHotNgramBitIndex` v3（hot/common tier + LenRow + skip）
- **查询**：`FpTokenQuery` + `FpSearch`（selective fpBits）
- **测试**：`scripts/run-fptoken-tests.ps1`

## 配置摘录

| 常量 | 值 | 含义 |
|------|-----|------|
| `NGRAM_MIN` / `NGRAM_MAX` | 1 / 6 | ngram 长度范围 |
| `HOT_TIER_TERM_COUNT_THRESHOLD` | 64 | 热词升格阈值 |

## 相关

- [仓库 README](../README.md)
- [写段流程图](fp-token-write-path-detailed.md)
