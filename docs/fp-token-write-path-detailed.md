# FPToken 写段调用总流程图（详细）

> Markdown 摘要；完整 PNG/HTML 见 [fp-token-write-path-detailed.html](fp-token-write-path-detailed.html)。

## 资源

| 文件 | 说明 |
|------|------|
| [fp-token-write-path-detailed.png](fp-token-write-path-detailed.png) | 高分辨率整图（推荐） |
| [fp-token-write-path-detailed.mmd](fp-token-write-path-detailed.mmd) | Mermaid 源 |
| [fp-token-write-path-detailed.html](fp-token-write-path-detailed.html) | 浏览器缩放查看 |

重新生成 PNG：

```powershell
.\scripts\render-fp-write-path-diagram.ps1
```

## 流程概要

1. **阶段 0~1**：`FpTokenAnalyzer` → 列名前缀 + FP 头 + ngram 载荷；`FPBlockTreeTermsWriter` 绑定 bitOut。
2. **阶段 2**：`writeTerms` → `FpTokenBlockOrchestrator` + `FpTokenBlockLevelPolicy`。
3. **阶段 3**：`acceptTerm` — 换列/换组 flush；高级别 → `FpGroupDataOriginal`，低级别 → `FpGroupDataRebuild`。
4. **阶段 4**：`flushHighGroup` — 达标透传 `fpBits`，不达标降级 merge。
5. **阶段 5**：`FpGroupHotNgramRebuild` → `FpGroupHotNgramBitIndex.execute` → writefp + bitOut。
6. **阶段 6~7**：倒排 + termsbit + `fpblock_list`；查询 `FpSearch` / `FpTokenQuery`。

简版 Mermaid 嵌入 [仓库 README](../README.md#写段总流程图入口fpblocktreetermswriter)。

## 相关

- [fp-token-design_20260517.md](fp-token-design_20260517.md)
