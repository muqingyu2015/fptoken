# FPToken 服务等级指标（SLI/SLO）与架构决策基线

本文定义 FPToken 在“索引构建/merge 处理”链路的统一服务等级口径，用作性能回归、容量规划与架构选型（采样策略、候选上限、hint 策略）的量化依据。

## 1. 适用范围与环境边界

- 适用对象：`ExclusiveFpRowsProcessingApi.processRows(...)` 与 `ExclusiveFrequentItemsetSelector.select...(...)` 主链路。
- 数据形态：`sample-data/line-records`、`sample-data/real-docs` 以及合成 merge-like 数据。
- 测试入口：`scripts/run-fptoken-tests.ps1 -Perf`（必要时叠加 `-Scale`、`-Budget`）。
- 环境说明：以下 SLO 默认以“单 JVM、本地基准机、无外部 IO 干扰”为基线。跨机器对比必须记录 CPU/内存与 JVM 参数。

## 2. 核心 SLI 定义

### SLI-1：端到端延迟（Latency）

- 指标：单次 `processRows(...)` 或 selector 主流程耗时（ms）。
- 统计口径：`P50 / P95 / P99`，按同一数据集重复运行 N 次（建议 N >= 15）统计。
- 目标关注：上线准入以 `P99` 为主，调参效果以 `P50/P95` 辅助解释。

### SLI-2：规模处理能力（Scale Capacity）

- 指标：给定支持度阈值下，可在预算内处理的文档规模（docs）。
- 统计口径：固定 `minSupport`、`minItemsetSize`、`maxItemsetSize` 与 `maxCandidateCount`，记录最大稳定通过规模与总耗时。

### SLI-3：吞吐效率（Throughput）

- 指标：`docs/s`（文档数 / 总耗时）。
- 统计口径：与 SLI-1 同批次数据联合记录，避免单独样本失真。

### SLI-4：稳定性与抖动（Stability）

- 指标：抖动代理值 `P99 / P50`。
- 统计口径：同配置重复运行，关注长尾是否异常放大。

### SLI-5：结果正确性守恒（Correctness Guard）

- 指标：hint 开启/关闭下关键结果守恒（覆盖率、支持度合法性、全量回算约束）。
- 统计口径：功能/单元测试断言 + 性能测试中的结果一致性检查。

## 3. 当前 SLO 目标（默认基线）

> 说明：目标值以当前仓库已有性能用例预算与报告量级为基准，后续可按硬件分层维护 profile（dev/ci/prod-like）。

- **SLO-LAT-001（在线延迟目标）**：典型 merge-like 场景 `P99 < 200ms`。
- **SLO-SCALE-001（规模目标）**：在 `minSupport >= 3` 的标准配置下，可处理 `1,000,000` 文档（分批/分片执行，总流程不失败）。
- **SLO-THR-001（吞吐目标）**：典型 real-docs 负载保持 `>= 5,000 docs/s`（单机基线）。
- **SLO-STAB-001（稳定性目标）**：`P99/P50 <= 3.0`，避免严重长尾抖动。
- **SLO-COR-001（正确性目标）**：pre-merge hints 仅允许影响排序与搜索空间，不得破坏“全量回算后再参与挑选”的正确性约束。

## 4. 架构决策量化规则（ADR Gate）

任何涉及以下主题的改动，必须引用至少 2 个 SLI 结果并给出结论：

- 采样策略（`sampleRatio`、`minSampleCount`、`samplingSupportScale`）
- 候选上限/束宽（`maxCandidateCount`、beam 相关参数）
- pre-merge hint 策略（hint 来源、去重方式、boost 权重）
- 索引结构调整（skip/bitset/倒排布局）

建议采用以下决策门槛：

- **接受（Accept）**：`P99` 不回退超过 10%，且吞吐不下降超过 5%，正确性守恒全通过。
- **有条件接受（Conditional）**：单项指标回退但换来明显规模收益（例如可处理文档规模提升 >= 2x），需记录 trade-off。
- **拒绝（Reject）**：违反 `SLO-COR-001` 或 `P99` 回退超过 20% 且无规模收益补偿。

## 5. 现有测试与 SLI 映射

- 延迟/抖动：`PerformanceExecutionReliabilityTest`、`PerformanceAdvancedInfrastructureTest`、`PerformanceRegressionGateTest`
- 规模能力：`MaxLengthFilePerformanceTest`、`PerformanceCaseCatalogPart1Test`、`LxdbDualIndexBuildPerformanceTest`
- pre-merge hint 量化：`MergePremergeHintAccelerationPerformanceTest`、`SampleDataLineRecordsPremergeHintPerformanceTest`
- 正确性守恒：`PremergeHintIntegrationUnitTest`、`PremergeHintPostingsFunctionalTest`

## 6. 执行与记录规范

- 跑批建议：
  - `.\scripts\run-fptoken-tests.ps1 -Perf`
  - `.\scripts\run-fptoken-tests.ps1 -Perf -Scale`
  - `.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag`（生成报告索引）
- 每次架构参数调整需记录：
  - 变更前后参数
  - 至少一组 `P50/P95/P99` 与 docs/s
  - 是否满足本文件 SLO 与 ADR Gate 结论

## 7. 后续演进建议

- 将 SLO profile 外部化（dev/ci/prod-like）并通过系统属性注入阈值。
- 在 CI 中增加“预算门禁 job”，对 `P99` 与规模能力做自动 fail-fast。
- 将本文件目标同步到 `docs/performance-benchmark-report.md` 的固定章节，形成持续更新的基线历史。
