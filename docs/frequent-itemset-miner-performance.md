# FrequentItemsetMiner：算法辨析与性能

> Markdown 摘要；完整分析见 [HTML](frequent-itemset-miner-performance.html)。  
> **状态：历史参考**（Beam 挖掘模块，非 BlockTree FP）。

## 章节

1. 源码在做什么（为何像排列）
2. 性能最坏与典型
3. 开源项目做法
4. 优化手段（投入从小到大）
5. 与本仓库设计文档关系
6. 小结

## 当前 FP 模块性能

见 `FpBitIndexPerfTest`、`FpSearchPerfTest`（`@Tag("performance")`）。
