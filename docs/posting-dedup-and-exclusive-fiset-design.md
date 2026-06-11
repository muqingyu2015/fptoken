# 倒排共享存储与互斥频繁项集设计方案

> Markdown 摘要；完整方案见 [HTML](posting-dedup-and-exclusive-fiset-design.html)。  
> **状态：历史参考**

## 章节

1. 需求拆解
2. 无损去重：相同倒排只存一份
3. 频繁项集价值
4. 项集 → docid 列表
5. 互斥 + 最大长度候选模型
6. 两级求解策略
7. 存储格式建议
8. 落地里程碑
9. 最终答案（简版）

## 与当前 repo

BlockTree FP 使用 `FPDocList` + 组内 merge，**不是**本文档的项集输出层。
