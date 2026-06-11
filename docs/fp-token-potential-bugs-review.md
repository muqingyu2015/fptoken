# FP Token：遗留缺陷（BUG）

> Markdown 摘要；状态表见 [HTML](fp-token-potential-bugs-review.html)。

## 1. 遗留 BUG 总表

跟踪写段、selective 读、查询 slice、列名前缀等已知问题及修复状态。  
**2026-06 已修复**：

- skip 表 `keysPtrRel` 段 1+ 读偏移错误（`selectiveIoBroken`）。
- `DiskLenRow` 延迟 IO 导致 `AlreadyClosedException` → selective 预取 + 释放 `IndexInput`。

## 2. 读写对齐（当前实现）

| 环节 | 契约 |
|------|------|
| 写 | `FpGroupHotNgramBitIndex.flushto` → v3 tier + skip |
| 读全量 | `readfrom(in, blkinfo)` |
| 读 selective | `readfromBanksSelective(in, blkinfo, keys, keys)` |
| 查 | `FpSearch.loadBitIndex` → `lookupHotOrders` / `lookupCommonOrders` |

## 相关

- [fp-token-write-search-alignment-report.md](fp-token-write-search-alignment-report.md)
