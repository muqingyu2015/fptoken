# FPToken 项目介绍（老板/客户版）

> Markdown 摘要；商务 PPT HTML 见 [fptoken-boss-customer-presentation.html](fptoken-boss-customer-presentation.html)。

## 1. 我们在解决什么问题

海量稀疏字段上的快速子串检索，避免全表扫描。

## 2. FPToken 的核心思路

二进制 ngram + 组内热词 + 精确 bucket 倒排。

## 3. 给客户带来的直接价值

- 查询延迟可控
- 索引体积优于朴素 ngram
- 与现有 Lucene/LXDB 栈集成

## 4. 预期效果（区间）

视数据分布与查询模式；需 PoC 实测。

## 5. 风险与应对

格式升级 rebuild、监控 selective 命中率、回滚 jar 版本。

## 6. 交付方式与实施计划

插件 jar + 补丁 Lucene + 灌数据脚本 + 验收用例。

## 7. 结语

见 HTML。
