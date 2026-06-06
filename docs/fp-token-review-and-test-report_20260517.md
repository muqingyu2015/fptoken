# FP Token 代码审查与测试报告（2026-05-17）

> Markdown 摘要；完整表格见 [HTML](fp-token-review-and-test-report_20260517.html)。

## 1. 依赖与编译

- 完整 `lib/`（补丁 Lucene 8.9 + LXDB）联编。
- 脚本：`scripts/run-fptoken-tests.ps1`。

## 2. 测试执行摘要

- 单元 + 功能测试覆盖写段、位图 round-trip、selective 读、查询路径。
- 当前仓库：**48/48** 通过（`-ExcludePerfTag`）。

## 3. 潜在缺陷（按优先级）

- **P0/P1**：审查中列出的逻辑缺陷已关闭或按产品约定撤回。
- **P2（可选）**：Javadoc/命名一致、集成测覆盖扩展。

## 4. 已关闭 / 不再适用项

- v2 固定 8×256 `FixedBitSet` 位图相关项 → v3 sorted posting 已替代。
- selective 延迟 seek 持有 `IndexInput` → 已改为 `readSelective` 预取。

## 5. 与设计文档的一致性

见 [fp-token-design_20260517.md](fp-token-design_20260517.md)（2026-06 更新 v3 章节）。

## 6. 建议后续

- 生产段 rebuild 验证 bucket 规则变更。
- LXDB 灌数据日志级别（INFO flush / DEBUG query）。
