# 基于 Lucene 的二进制数据模糊检索技术方案

> Markdown 摘要；完整九章设计见 [HTML](binary-search-lucene-design.html)。

## 章节概要

1. **核心需求概述** — 字节级模糊/子串检索
2. **索引构建策略** — BlockTree + 侧车位图
3. **索引数据结构** — v3 sorted posting（本仓库已实现部分）
4. **查询执行流程** — slice AND、selective fpBits
5. **terms(field) 与读取侧** — `FieldReader#fpBits`
6. **思考与补充**
7. **改进建议**
8. **拆分与分块策略** — group / level
9. **整体架构** — 与 Lucene 解耦层
10. **Lucene 存储集成** — Writer/Reader 补丁点

## 与当前代码

本仓库 `cn.lxdb.plugins.muqingyu.fptoken` 为上述方案的 **BlockTree FP 落地**；详细实现见 [fp-token-design_20260517.md](fp-token-design_20260517.md)。

## 相关

- [binary-search-lucene-references.md](binary-search-lucene-references.md)
