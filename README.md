# FPToken

面向二进制检索与压缩场景的数据分层引擎。  
核心目标：**把经常出现、经常共现的内容先抽成高价值索引，其余走低成本路径**。

## 目录（阅读导航）

自上而下按 `##` / `###` 标题阅读即可。常用跳转：

- **术语**：下节「术语与对外名」  
- **怎么跑测试 / HTML 报告**：「官方命令入口」  
- **目录树说明**：「仓库地图」  
- **全部设计类文档**：[`docs/README.md`](docs/README.md) · 浏览器导航页 [`docs/index.html`](docs/index.html)  
- **服务等级指标（SLI/SLO）**：[`docs/service-level-objectives.md`](docs/service-level-objectives.md)
- **技术债务清单**：[`docs/technical-debt-register.md`](docs/technical-debt-register.md)
- **Agent / 贡献者速览**：[`AGENTS.md`](AGENTS.md)

---

## 术语与对外名

| 对内概念 | 对外文档推荐写法 |
|----------|------------------|
| 历史段高频提示 | **Pre-merge hint**（API：`withPremergeMutexGroupHints` / `withPremergeSingleTermHints`） |
| 互斥高频组合层 | `highFreqMutexGroupPostings` |
| 高频单词层 | `highFreqSingleTermPostings` |
| 低频残差层 | `lowHitForwardRows` |

---

## 官方命令入口

**唯一推荐脚本**：`scripts/run-fptoken-tests.ps1`（下载缺失 JUnit JAR、编译、`bin` + `bin-test` 跑全包测试）。

采样参数支持外部化配置（无需重新编译）：

- JVM 参数：`-Dfptoken.selector.sampleRatio=0.35`
- JVM 参数：`-Dfptoken.selector.minSampleCount=96`
- JVM 参数：`-Dfptoken.selector.samplingSupportScale=0.0`
- 环境变量（备选）：`FPTOKEN_SELECTOR_SAMPLE_RATIO` / `FPTOKEN_SELECTOR_MIN_SAMPLE_COUNT` / `FPTOKEN_SELECTOR_SAMPLING_SUPPORT_SCALE`

运行中若要重新读取外部值，可调用：`ExclusiveFrequentItemsetSelector.reloadSamplingConfig()`

`samplingSupportScale <= 0` 的默认自动策略具备统计依据：  
默认使用二项比例估计的中心值（MLE，`z=0`），即按采样比例直接缩放支持度阈值；  
代码保留了置信边际计算钩子，后续可按场景把 `z` 提高到如 `1.645` 以偏向更保守阈值。  
若需完全手工控制，可显式传入 `>0` 缩放值覆盖自动策略。

服务等级目标与架构量化门槛见：[`docs/service-level-objectives.md`](docs/service-level-objectives.md)。
其中包含统一延迟口径（含 `P99`）、规模目标（百万文档能力）以及 ADR 决策 gate。

互斥筛选评分公式支持运行时注入（长度/支持度/业务价值/成本）：

- JVM 参数：`-Dfptoken.picker.score.lengthWeight=1`
- JVM 参数：`-Dfptoken.picker.score.supportWeight=1000`
- JVM 参数：`-Dfptoken.picker.score.businessValueWeight=1000000`
- JVM 参数：`-Dfptoken.picker.score.costWeight=1000000`
- JVM 参数：`-Dfptoken.picker.score.priorityBoostWeight=1000`
- JVM 参数：`-Dfptoken.picker.score.coverageWeight=1000`

运行中可调用：`ExclusiveFrequentItemsetSelector.reloadPickerScoringWeights()`，也可直接通过
`ExclusiveFrequentItemsetSelector.setPickerScoringWeights(...)` 注入自定义权重对象。

| 场景 | 命令 |
|------|------|
| 默认测试（含 `@EnabledIfSystemProperty` 未开启时跳过的性能用例） | `.\scripts\run-fptoken-tests.ps1` |
| 打开性能类 JVM 开关 | `.\scripts\run-fptoken-tests.ps1 -Perf` |
| 大规模 / 预算 / 压力 / 浸泡 | 同上，加 `-Scale`、`-Budget`、`-Stress`、`-Soak`（可组合） |
| **HTML 报告**（JUnit XML + 单页汇总） | `.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag` |
| 仅生成报告（已用 IDE 编译好） | `.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag -SkipCompile` |

报告输出目录（**可整目录删除**）：

- XML：`build/test-results/junit-xml/`
- HTML：`build/test-results/junit-html/index.html`

**兼容别名**：`scripts/run-tests-html-report.ps1` 等价于带 `-HtmlReport -ExcludePerfTag` 的薄封装；`scripts/run-tests-html-report.cmd` 双击可从资源管理器调用。

---

## 仓库地图

| 路径 | 说明 |
|------|------|
| `src/cn/.../fptoken/` | 生产源码（API、算法、runner） |
| `src/test/java/.../tests/` | JUnit 测试（unit / functional / performance 等子包） |
| `sample-data/` | 行样例与 real-docs，供功能/性能用例加载 |
| `docs/` | 设计与协作文档，**索引**：`docs/README.md` |
| `scripts/` | `run-fptoken-tests.ps1`、HTML 报告别名 |
| `lib/` | JUnit 等 JAR（可由脚本自动下载） |
| `archive/` | **不参与构建与发布** 的历史草稿，见 `archive/README.md` |

---

## 1. 设计思路（先讲人话）

可以把系统理解成“仓库分拣”：

- 热门且常一起出现的货，放在最容易拿的位置（高频组合层）
- 热门但单独出现的货，也单独放好（高频单词层）
- 剩下零散货物放普通区（低频剩余层）

好处：检索走快路、压缩更好、低频不丢。

---

## 2. 核心实现思路

### 2.1 ByteRef 主链路（减少 GC）

主链路使用 `ByteRef`：`source + offset + length`，减少同一段字节的反复切片复制。

- `DocTerms` 主存 `List<ByteRef>`
- 分词与索引优先 `ByteRef`
- 哈希/比较支持按区间直接算

### 2.2 固定采样流程（先快后准）

1. 全量建索引  
2. 抽样挖掘候选  
3. 全量回算支持度  
4. 互斥挑选  

### 2.3 三层结果结构（`FinalIndexData`）

- `highFreqMutexGroupPostings`：高频互斥组合倒排  
- `highFreqSingleTermPostings`：高频单词倒排  
- `lowHitForwardRows`：低命中残差正排  

以及 `terms -> postingIndex`、skip/bitset 等辅助结构。

### 2.4 滑动窗口（`_bin` 场景）

- 存储 TermVector：`32B` 无重叠分段  
- BitSet 逻辑窗口：`64B` 窗口 + `32B` 步长交叉  

API：`cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi`

### 2.5 Pre-merge hint（Lucene merge 场景）

索引合并会反复算同一类问题。历史段里**可能高频**的 term 可作为 **hint** 喂入当前段：**先提示、再核验**，不盲信历史。

- **可复用**：缩小搜索空间  
- **可校验**：与普通候选一样全量回算支持度  
- **可降级**：不准则最多不提速，不影响正确性  
- **可调**：`hintBoostWeight`、`HintValidationMode`（`STRICT` / `FILTER_ONLY`）

LXDB 侧如何把 `selectionResult` / `derivedData` 拆成压缩索引与 skip 索引，见  
[`docs/lxdb-integration-derived-data-and-selection-result.html`](docs/lxdb-integration-derived-data-and-selection-result.html)。

### 2.6 无状态并行扩展（分片友好）

- `SelectionRequest` 支持请求级 `executionTuning`，采样/挑选参数在单次调用内隔离，不依赖可变全局状态。  
- `ExclusiveFpRowsProcessingApi.processRows(rows, options)` 现在把 `options` 映射为请求级调参传递给 selector，不再写入全局静态配置。  
- 这意味着多个实例/线程可以并行处理不同数据分片并使用不同参数，为未来横向扩展（按 shard 拆分）保留了兼容路径。
- 架构约束上，核心挖掘流程只消费不可变配置对象（`SelectionRequest` + `ExecutionTuning`）；策略参数不再通过处理链路中的全局 setter 透传。

---

## 3. 处理流程（代码视角）

1. 输入 `List<DocTerms>`  
2. `TermTidsetIndex`：`term -> doc bitset`  
3. 采样挖掘候选频繁项集  
4. 全量回算支持度  
5. 互斥挑选（Two-phase picker）  
6. 三层结果 + skip/bitset  

---

## 4. 关键模型

### `DocTerms`

- `docId`  
- `termRefs`（`List<ByteRef>`）

### `SelectedGroup`

- 组合词、`docIds`、`support`、`estimatedSaving`

### `LineFileProcessingResult.FinalIndexData`

- `getHighFreqMutexGroupPostings()` / `getHighFreqSingleTermPostings()` / `getLowHitForwardRows()`  
- 倒排引用与 `getOneByteDocidBitsetIndex()` 等  

---

## 5. 推荐入口 API

### A. 行处理主入口

`cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi`

- `processRows(List<DocTerms> rows)`  
- `processRows(..., int minSupport, int minItemsetSize, int hotTermThresholdExclusive)`  
- `processRowsWithNgram(...)`  
- `processRows(rows, ProcessingOptions options)`  

Pre-merge：`withPremergeMutexGroupHints`、`withPremergeSingleTermHints`、`withHintBoostWeight`、`withHintValidationMode`。

Pre-merge hint 契约规范（调用时机、字段要求、回退责任、回滚流程）见：
[`docs/premerge-hint-contract.md`](docs/premerge-hint-contract.md)。

离线日志评估（生产 merge 日志可回放分析）：

- `cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer`
- 输入包含 `improvementPercent=` 或 `improveMedianPercent=` 的日志行，输出
  有效/中性/负优化占比与启用建议（默认阈值：`>=+5%` 记有效，`<=-5%` 记负优化）。

### B. 底层选择器

`cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector`

### C. 二进制滑窗

`cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi`

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
- `minSupport`：从行数的约 `0.05% ~ 1%` 试起  
- `hotTermThresholdExclusive`：单词层要更精选则调大  

---

## 8. 源码与测试目录

- `src/cn/lxdb/plugins/muqingyu/fptoken/api`：对外 API  
- `src/cn/lxdb/plugins/muqingyu/fptoken/exclusivefp`：核心算法与模型  
- `src/cn/lxdb/plugins/muqingyu/fptoken/runner`：文件加载与入口  
- `src/test/java/cn/lxdb/plugins/muqingyu/fptoken/tests`：单元 / 功能 / 性能等  

项目为 **Eclipse 风格**（无 Maven/Gradle 为默认构建）；也可用 IDE 编译后再 `-SkipCompile` 跑报告。
