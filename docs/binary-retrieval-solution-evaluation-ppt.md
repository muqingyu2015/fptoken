# 二进制检索方案评估（专家评审版）

> Markdown 摘要；PPT HTML 见 [binary-retrieval-solution-evaluation-ppt.html](binary-retrieval-solution-evaluation-ppt.html)。

## 1. 方案简述

BlockTree + ngram bucket 索引 + selective 读盘；面向 LXDB 稀疏列场景。

## 2. 主要优势

- 列级隔离、可控内存
- 热词缩短 posting
- 与 Lucene 段结构兼容

## 3. 预期效果（区间）

延迟、索引体积、命中率需按业务数据集压测；见 HTML 表格。

## 4. 潜在问题与根因

- 段 rebuild 成本
- 查询 slice 策略（长 query 多 slice AND）
- 旧段格式迁移

## 5. 风险对冲

selective 读、闭块策略、透传路径、单测 + 生产日志。

## 6. 落地路线（90 天）

PoC → 灌数据 → 灰度 → 全量 rebuild

## 7~8. 结论与已采纳意见

见 HTML 正文。
