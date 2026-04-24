# FPToken

面向二进制检索与压缩场景的数据分层引擎。  
核心目标是：**把经常出现、经常共现的内容先抽出来做高价值索引，其余数据走低成本路径**。

---

## 1. 设计思路（先讲人话）

可以把系统理解成“仓库分拣”：

- 热门且常一起出现的货，放在最容易拿的位置（高频组合层）
- 热门但单独出现的货，也单独放好（高频单词层）
- 剩下零散货物放普通区（低频剩余层）

这样做的好处是：

- 检索时常见请求走快路
- 存储时减少重复，压缩率更好
- 低频内容也不丢，完整性保留

---

## 2. 核心实现思路

### 2.1 ByteRef 主链路（减少 GC）

项目主链路已经切到 `ByteRef` 思路：`source + offset + length`。  
目的是减少“同一段原始字节被反复切片复制”的内存开销。

关键点：

- `DocTerms` 内部主存储是 `List<ByteRef>`
- 分词和索引构建优先走 `ByteRef`
- 词典哈希/比较支持按 `ByteRef` 区间直接计算

### 2.2 固定采样流程（先快后准）

处理流程是固定“采样 + 回算”：

1. 全量建索引
2. 抽样做候选挖掘（快）
3. 用全量数据回算支持度（准）
4. 做互斥挑选（减少重复）

### 2.3 三层结果结构

最终产出三块主数据（`FinalIndexData`）：

- `highFreqMutexGroupPostings`：高频互斥组合倒排
- `highFreqSingleTermPostings`：高频单词倒排
- `lowHitForwardRows`：低命中残差正排

并补充统一的 `terms -> postingIndex` 引用与 skip/bitset 索引结构。

### 2.4 滑动窗口策略（_bin 场景）

`_bin` 场景支持“存储与检索窗口分离”：

- 存储 TermVector：`32B` 无重叠分段
- BitSet 逻辑窗口：`64B` 窗口 + `32B` 步长交叉

对应 API：`cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi`

### 2.5 Pre-merge Hint 设计理念（Lucene merge 场景）

索引合并会反复触发同一套计算。前一轮段里的高频信息，通常对下一轮仍有参考价值。  
因此这里采用“**先提示、再核验**”的设计，不直接信任历史结果。

核心原则：

- **可复用**：允许把历史段里“可能高频”的 term 先喂进来，减少盲目搜索空间
- **可校验**：提示候选和普通候选一样，最终都要在当前全量数据上回算支持度
- **可降级**：提示不准也不会污染结果，最多是没有提速；正确性不受影响
- **可调权重**：通过 `hintBoostWeight` 控制“优先尝试”的力度，而不是强制入选

这样做的本质是：把历史信息当“导航”，不是当“真相”。

---

## 3. 处理流程（代码视角）

1. 输入 `List<DocTerms>`
2. `TermTidsetIndex` 建立 `term -> doc bitset`
3. 在采样集合挖掘候选频繁项集
4. 在全量集合回算候选支持度
5. 互斥挑选（Two-phase picker）
6. 构建三层结果 + skip/bitset 辅助索引

---

## 4. 关键模型

### `DocTerms`

- `docId`
- `termRefs`（主存储，`List<ByteRef>`）

### `SelectedGroup`

- `termRefs`（组合词）
- `docIds`
- `support`
- `estimatedSaving`

### `LineFileProcessingResult.FinalIndexData`

- 高频组合层：`getHighFreqMutexGroupPostings()`
- 高频单词层：`getHighFreqSingleTermPostings()`
- 低频残差层：`getLowHitForwardRows()`
- 统一倒排引用：`getHighFreqMutexGroupTermsToIndex()` / `getHighFreqSingleTermToIndex()` / `getLowHitTermToIndexes()`
- 辅助检索结构：skip bitset + `getOneByteDocidBitsetIndex()`

---

## 5. 推荐入口 API

### A. 行处理主入口

类：`cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi`

常用方法：

- `processRows(List<DocTerms> rows)`
- `processRows(List<DocTerms> rows, int minSupport, int minItemsetSize, int hotTermThresholdExclusive)`
- `processRowsWithNgram(...)`
- `processRows(List<DocTerms> rows, ProcessingOptions options)`

merge 加速扩展（Lucene merge 场景）：

- `ProcessingOptions.withPremergeMutexGroupHints(...)`：对应历史段 `highFreqMutexGroupPostings` 的 pre-merge 提示（仅 term 序列）
- `ProcessingOptions.withPremergeSingleTermHints(...)`：对应历史段 `highFreqSingleTermPostings` 的 pre-merge 提示（支持度仍由当前段回算）
- `ProcessingOptions.withHintBoostWeight(...)`：提示候选优先级加权
- `ProcessingOptions.withHintValidationMode(...)`：`STRICT` / `FILTER_ONLY`

设计说明（与实现策略对应）：

- 你提供的 hint 可以有重复、不完整、甚至跨段冲突信息，系统会先做去重与映射
- hint 只影响“候选优先级”，不会绕过频率与互斥选择规则
- 最终入选仍依赖当前批次真实数据，不满足阈值会被淘汰
- `STRICT` 适合强约束场景（输入不合法直接失败），`FILTER_ONLY` 适合线上鲁棒场景（跳过坏 hint 继续跑）

### B. 底层选择器入口

类：`cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector`

- `selectExclusiveBestItemsets(...)`
- `selectExclusiveBestItemsetsWithStats(...)`
- 采样参数代码配置：`setSampleRatio` / `setMinSampleCount` / `setSamplingSupportScale`

### C. 二进制滑窗入口

类：`cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi`

- `termVectors32(source, offset, length)`：32/32 存储分段
- `bitsetWindows64Step32(source, offset, length)`：64/32 逻辑窗口
- `slidingWindows(...)`：通用窗口

---

## 6. LXDB 接入示例

```java
List<DocTerms> rows = ...;

LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(
        rows,
        80,  // minSupport
        2,   // minItemsetSize
        16   // hotTermThresholdExclusive
);

LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

List<SelectedGroup> groups = finalData.getHighFreqMutexGroupPostings();
List<LineFileProcessingResult.HotTermDocList> hotTerms = finalData.getHighFreqSingleTermPostings();
List<DocTerms> lowHitRows = finalData.getLowHitForwardRows();
```

---

## 7. 参数建议（起步）

- `ngramStart=2, ngramEnd=4`
- `minItemsetSize=2`
- `minSupport`：先从行数的 `0.05%~1%` 试
- `hotTermThresholdExclusive`：想让单词层更“精选”就调大

---

## 8. 测试与运行

项目是 Eclipse/Java 结构，不依赖 Maven。

- 功能测试：`scripts/run-fptoken-tests.ps1`
- 性能测试：`scripts/run-fptoken-tests.ps1 -Perf`
- 大规模测试：`scripts/run-fptoken-tests.ps1 -Scale`

---

## 9. 目录说明

- `src/cn/lxdb/plugins/muqingyu/fptoken/api`：对外 API
- `src/cn/lxdb/plugins/muqingyu/fptoken/exclusivefp`：核心算法与模型
- `src/cn/lxdb/plugins/muqingyu/fptoken/runner`：文件加载与运行入口
- `src/test/java/cn/lxdb/plugins/muqingyu/fptoken/tests`：单元/功能/性能测试

