# FP Token 写段与查询对齐审查报告（复审）

> Markdown 摘要；完整复审表见 [HTML](fp-token-write-search-alignment-report.html)。

**复审对象**：`src/cn/lxdb/plugins/muqingyu/fptoken/`  
**写段入口**：`FPBlockTreeTermsWriter` / `FpTokenBlockOrchestrator`  
**查询入口**：`FpSearch` / `FpTokenQuery`  
**布局契约**：`FpTokenTermLayout`

## 结论（一句话）

报告曾列出的**代码级缺陷均已修复或按设计关闭**；**无未关闭的 P0/P1 实现 bug**。透传路径在真实 LXDB 索引上的集成风险需在宿主环境验证。

## 章节

1. **复审总表** — 逐项状态（布局、热词 merge、selective 读等）
2. **词项布局** — 列名前缀 + 14B 头字段与读写一致性
3. **写 / 查入口** — `writefp`、`fpBits`、`iterator_fp`
4. **透传** — 旧组读图、新组落盘（设计行为）
5. **源码核对明细** — 与 `FpGroupHotNgramBitIndex` v3、selective 预取对齐
6. **剩余关注** — 集成环境、段版本、重建策略（非单测 bug）

## v3 补充（2026-06）

- 查询：`loadBitIndex` → `readfromBanksSelective` 在 `fpBits` 返回前预取 bucket order，lookup 纯内存。
- bucket：`bucketIndex` 1~4 字节大端拼 int，5~6 murmurhash。
- 不做全量 `fpBits(null,null)` 查询回退。

## 相关

- [fp-token-review-and-test-report_20260517.md](fp-token-review-and-test-report_20260517.md)
