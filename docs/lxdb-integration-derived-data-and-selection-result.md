# LXDB 集成说明（二）：DerivedData 与 SelectionResult

> Markdown 摘要；完整说明见 [HTML](lxdb-integration-derived-data-and-selection-result.html)。

## 1) 数据契约（已对齐）

- 高频压缩索引 vs skip index 输入边界
- FP 字段与 `selectionResult` 职责拆分

## 2) LXDB 侧索引职责拆分

| 索引 | 职责 |
|------|------|
| DerivedData / 主索引 | 原始行、常规检索 |
| FP termsbit + 倒排 | ngram bucket → order → posting |

## 3) 调用与落库流程（推荐）

1. 灌数据写 FP 字段（BlockTree + termsbit）
2. 查询 `FpTokenQuery` 走 selective `fpBits`
3. 结果与 DerivedData 合并由上层 SQL/FDW 负责

## 4) 三条实现约束

- 列名前缀必须与查询 field 一致
- group_id 与 `fpblock_list` 对齐
- 段升级后 rebuild bucket 索引

## 5) 为什么适合 LXDB

稀疏列 + 组级闭块 + selective IO，控制内存与 seek 次数。
