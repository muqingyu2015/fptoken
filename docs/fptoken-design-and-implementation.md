# 第一步方案：频繁项集的互斥最优筛选

> Markdown 摘要；完整数学推导见 [HTML](fptoken-design-and-implementation.html)。  
> **状态：历史参考** — 本仓库 **不包含** `ExclusiveFpRowsProcessingApi` 实现。

## 2026 全量更新说明

当前生产代码为 BlockTree FP 模块；本文档描述行级互斥频繁项集筛选，可与 FP 在产品内组合，但源码不在本 repo。

## 章节

1. 你要解决的真实问题
2. 数学抽象：频繁项集 → 优化模型
3. 第一阶段：贪心近似
4. 第二阶段：整数规划
5. 数据结构与性能优化
6. 推荐落地路径
7. 价值边界

## 相关

- [posting-dedup-and-exclusive-fiset-design.md](posting-dedup-and-exclusive-fiset-design.md)
- [fp-token-design_20260517.md](fp-token-design_20260517.md)（当前模块）
