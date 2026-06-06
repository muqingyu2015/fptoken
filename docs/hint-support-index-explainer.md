# Hint 支持度回算：倒排索引优化说明

> Markdown 摘要；算法对比见 [HTML](hint-support-index-explainer.html)。  
> **状态：历史参考**（Pre-merge hint 支持度回算）。

## 章节

1. 原始做法（慢在哪里）
2. 优化做法（核心思想）
3. contains 是否暴力
4. 复杂度对比
5. 小例子
6. 为何保留 aggregated
7. 当前实现对应关系
8. 后续优化

## 相关

- [premerge-hint-contract.md](premerge-hint-contract.md)
- [premerge-hint-performance-report.md](premerge-hint-performance-report.md)
