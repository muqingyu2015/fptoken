# buildWithSupportBounds 性能优化复盘

> Markdown 摘要；完整复盘见 [HTML](buildWithSupportBounds-performance-improvement-report.html)。  
> **状态：历史参考**（互斥项集挖掘热点 `buildWithSupportBounds`）。

## 章节

1. 问题背景（~72% 热点）
2. 本次已落地改进
3. 关键代码思路
4. 验证与稳定性
5. 结果解读
6. 后续 P1/P2

## 与当前 repo

BlockTree FP 性能关注点：`FpGroupHotNgramRebuild`、`readSelective` skip 读、查询 seek 路径。
