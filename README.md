# FPToken

面向二进制检索/压缩场景的数据整理引擎。  
你可以把它理解成：**先找出“经常一起出现”的关键词组合，再把数据分层存起来，方便后续查得快、压得小**。

项目把输入行数据（`DocTerms`）转换为可直接落库使用的三层结果：

- 高频组合层（`highFreqMutexGroupPostings`）：常一起出现的一组词
- 高频单词层（`highFreqSingleTermPostings`）：单个高频词
- 剩余低频层（`lowHitForwardRows`）：上面两层没覆盖到的词

---

## 1. 项目定位

`fptoken` 的核心目标是：从大量行记录里，把“最值得单独建索引”的内容先挑出来。

一个生活化例子（商品篮子）：

- 1000 个订单里，经常一起出现“可乐+薯片+炸鸡”  
- 这组就适合单独做成一个“常见组合”
- 其它零散商品留在剩余层按需查

在本项目里，“高频互斥频繁项集”可以简单理解为：

- **高频**：经常出现
- **组合**：几个词经常一起出现
- **互斥**：为了不重复统计，会尽量让每个词只归到一个最合适的组合里

最终分层后，便于后续索引系统（如 LXDB）构建：

- 高频层：倒排结构，命中快、压缩比高
- 低频层：保留正排，结合 skip index 控制成本

当前实现为固定采样流程：先抽一部分做初筛，再回到全量数据做确认。

---

## 2. 处理流程

1. 输入 `List<DocTerms>`（每条记录：`docId + terms`）
2. 先建立“这个词出现在哪些行”的索引
3. 在采样数据里先找“可能是热门”的词组合
4. 回到全量数据里复核这些组合是不是站得住
5. 做一次去重取舍，避免同一个词在多个组合里反复出现
6. 产出三层结果（热门组合层 / 热门单词层 / 剩余低频层）

---

## 3. 实现手段

这一节不讲代码细节，只讲“系统是怎么做事的”。

### 3.1 先抽样，再复核（先快后准）

可以把它理解成：

- 先从全班作业里抽一部分看，快速找到“可能常见的问题”
- 再回到全班作业里核对一遍，确认这些问题确实普遍存在

在本项目里就是：

- 先在样本数据上找候选组合（省时间）
- 再在全量数据上重新计算支持度（保准确）

### 3.2 先建“反查表”（词 -> 行）

如果每次都从头扫全部行会很慢，所以先建一张“反查表”：

- 给你一个词，马上知道它在哪些行出现过
- 这样算组合的共同出现行会更快

### 3.3 做一次“去重取舍”（互斥选择）

很多组合会互相重叠（抢同一个词）。  
系统会做一轮“取舍”：

- 选更有价值的一组
- 尽量不让同一个词在多个组合里重复占位

可以理解成整理衣柜：

- 一件衣服尽量放在一个最合适的分类里，不要在多个盒子里重复记账

### 3.4 结果分三层存（热门走快路，冷门走普通路）

最终会把结果拆成三层：

- 热门组合层：最常见、最值钱的“词组合”
- 热门单词层：单个词也很常见
- 剩余低频层：其余词保留在普通层

好处是：

- 热门查询走前两层，快
- 冷门查询走剩余层，完整性不丢

### 3.5 为什么还有 skip index 和 bitset

可把它理解为“快速跳页 + 打勾表”：

- bitset：像一张打勾表，某行命中就打勾
- skip index：先快速跳过明显不可能命中的块，少做无用扫描

两者结合能减少 CPU 和内存压力，尤其在大批量数据时更明显。

---

## 4. 快速开始

### 3.1 运行文件入口程序

入口类：

- `cn.lxdb.plugins.muqingyu.fptoken.FptokenLineFileEntryMain`

参数顺序：

`inputFile ngramStart ngramEnd minSupport minItemsetSize hotTermThresholdExclusive`

示例（IDE 里配置 main 参数）：

```text
sample-data/line-records/records_001_small.txt 2 4 80 2 16
```

### 3.2 运行测试

项目为 Eclipse/Java 项目（不依赖 Maven 构建流程），使用脚本：

- `scripts/run-fptoken-tests.ps1`：默认功能测试集
- `scripts/run-fptoken-tests.ps1 -Perf`：开启性能测试标记
- `scripts/run-fptoken-tests.ps1 -Scale`：开启大规模场景

---

## 5. 关键数据模型

### `DocTerms`

单条输入记录：

- `docId`：文档/行标识（建议非负）
- `terms`：词项集合（`Collection<byte[]>`）

### `LineFileProcessingResult`

处理结果总对象，重点字段：

- `getFinalIndexData()`：推荐主出口
- `getSelectionResult()`：挖掘统计与兼容访问
- `getProcessingStats()`：统一统计视图

### `LineFileProcessingResult.FinalIndexData`

最终三层核心结构（建议优先看这个）：

- `getHighFreqMutexGroupPostings()`：热门“词组合”层
- `getHighFreqSingleTermPostings()`：热门“单个词”层
- `getLowHitForwardRows()`：剩余词层（不热门但保留）

以及统一倒排引用形式：

- `getHighFreqMutexGroupTermsToIndex()`
- `getHighFreqSingleTermToIndex()`
- `getLowHitTermToIndexes()`

补充检索结构：

- `getHighFreqMutexGroupSkipBitsetIndex()`
- `getHighFreqSingleTermSkipBitsetIndex()`
- `getLowHitTermSkipBitsetIndex()`
- `getOneByteDocidBitsetIndex()`（1-byte -> docId bitset）

---

## 6. 关键 API 详细使用说明

## A. `ExclusiveFpRowsProcessingApi`（推荐业务入口）

类路径：

`cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi`

### A1. `processRows(List<DocTerms> rows)`

使用默认参数执行完整流程，适合先跑通链路。

**输入约束**

- `rows` 不能为空引用（`null` 会抛异常）
- 空列表允许，返回空结果结构

**返回值**

- `LineFileProcessingResult`

### A2. `processRows(List<DocTerms> rows, int minSupport, int minItemsetSize, int hotTermThresholdExclusive)`

最常用显式参数入口。

**参数解释**

- `minSupport`：最小支持度（越大越保守）
- `minItemsetSize`：最小项集长度（通常建议 `>=2`）
- `hotTermThresholdExclusive`：高频单词项阈值（严格 `count > threshold`）

可以把这三个参数理解为：

- `minSupport`：至少出现多少次，才算“值得关注”
- `minItemsetSize`：一个组合至少要有几个词
- `hotTermThresholdExclusive`：单个词出现多少次才算“热门”

### A3. `processRowsWithNgram(...)`

当 `rows` 每条是“原始字节行”（通常 `terms.size()==1`）时推荐使用。  
内部会按 `ngramStart~ngramEnd` 切词后再挖掘。

### A4. `processRows(..., skipHashMinGram, skipHashMaxGram)`

控制 skip-bitset 哈希层 n-gram 范围，常用于调优低命中检索性能。

### A5. `processRows(List<DocTerms>, ProcessingOptions)`

统一参数对象入口，适合可配置平台集成。

---

## B. `ExclusiveFrequentItemsetSelector`（底层核心入口）

类路径：

`cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector`

### B1. 仅要分组

`selectExclusiveBestItemsets(...)`  
返回 `List<SelectedGroup>`。

### B2. 需要统计

`selectExclusiveBestItemsetsWithStats(...)`  
返回 `ExclusiveSelectionResult`，包含：

- `groups`
- `frequentTermCount`
- `candidateCount`
- `intersectionCount`
- `truncatedByCandidateLimit`

### B3. 采样参数（代码内配置）

- `setSampleRatio(double)`
- `setMinSampleCount(int)`
- `setSamplingSupportScale(double)`

说明：`setSamplingEnabled(boolean)` 仅为兼容保留，当前固定采样流程，不作为执行分支开关。

---

## 7. 典型接入示例（LXDB 场景）

```java
List<DocTerms> rows = ...; // 上游提供（可为原始行或已分词行）

LineFileProcessingResult result =
        ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rows,
                2,   // ngramStart
                4,   // ngramEnd
                80,  // minSupport
                2,   // minItemsetSize
                16   // hotTermThresholdExclusive
        );

LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

// 高频倒排层
List<SelectedGroup> groups = finalData.getHighFreqMutexGroupPostings();
List<LineFileProcessingResult.HotTermDocList> hotTerms = finalData.getHighFreqSingleTermPostings();

// 低频正排层
List<DocTerms> lowHitRows = finalData.getLowHitForwardRows();

// 统一倒排引用（terms -> postingIndex）
List<LineFileProcessingResult.TermsPostingIndexRef> groupRefs = finalData.getHighFreqMutexGroupTermsToIndex();
List<LineFileProcessingResult.TermsPostingIndexRef> hotRefs = finalData.getHighFreqSingleTermToIndex();
List<LineFileProcessingResult.TermsPostingIndexRef> lowHitRefs = finalData.getLowHitTermToIndexes();
```

---

## 8. 参数建议（起步值）

如果你处理的是“中等规模批次（几千到一万行）”，可从以下值开始：

- `ngramStart=2, ngramEnd=4`
- `minItemsetSize=2`
- `minSupport`：按数据规模与噪声水平调（可先从总行数的 0.05%~1% 试）
- `hotTermThresholdExclusive`：想让“热门单词层”更严格就调大，想多收一些就调小

---

## 9. 注意事项

- `docId` 建议非负且可控范围
- `rows` 为空时返回空结构，不抛异常
- API 内部会做防御性拷贝，不修改调用方输入
- 若传入的是原始字节行，优先使用带 n-gram 的入口

---

## 10. 目录概览（核心）

- `src/cn/lxdb/plugins/muqingyu/fptoken/api`：对外处理 API
- `src/cn/lxdb/plugins/muqingyu/fptoken/exclusivefp`：算法核心（index/miner/picker/model/config）
- `src/cn/lxdb/plugins/muqingyu/fptoken/runner`：文件加载、样例数据、命令行运行器
- `src/test/java/cn/lxdb/plugins/muqingyu/fptoken/tests`：功能/集成/性能/边界/回归测试

---

## 11. 术语速查（尽量人话）

- 高频互斥频繁项集：经常一起出现、并且尽量不和其他组合“抢词”的词组合
- 倒排：从“词”反查“哪些行有它”
- 正排：按“行”存“这行有哪些词”
- 支持度（support）：一个词或组合出现了多少行

