# fptoken

**作者**: muqingyu  
**构建状态**: ✅ Maven编译通过，JAR打包成功  
**最新构建**: 2026-04-22 17:10  
**JDK**: 1.8.0_92 | **Maven**: 3.6.3

---

## 项目概述

这是我接触LXDB后独立设计的第一个技术方案。目前行业内二进制检索的主流方案主要有两种：ngram和Data Skipping Index，其余多为两者的变种。ngram的缺点是数据膨胀率非常高，存储成本大，大规模数据下性能严重下降。Data Skipping Index在命中词条特别多时效率急剧恶化，无法保证稳定查询性能。因此我需要一个从根源上解决上述问题的新方案。

**核心思路**：借鉴频繁项集思想，通过识别频繁项来控制膨胀率，同时提升高命中词条的查找效率；在此基础上结合Data Skipping Index思路，优化低频命中词条的检索性能。通过将两者有机融合，最终实现一个兼顾存储效率和查询性能的二进制检索方案。

## 架构

```
DocTerms → TermTidsetIndex（垂直索引：两阶段构建，先统计频率再分配BitSet）
         → BeamFrequentItemsetMiner（Beam Search近似频繁项集挖掘）
         → TwoPhaseExclusiveItemsetPicker（互斥选择：贪心初解 + 1-opt替换）
         → SelectedGroup（结果输出）
```

## 采样+近似挖掘流程

```
rows → 两阶段索引构建（一次全量扫描统计频率 → 过滤低频词 → 分配BitSet → 二次扫描设位图）
     → 30% 文档随机采样（位过滤，从全量tidsets提取采样子集）
     → 采样挖掘（minSupport自动按采样比例缩放）
     → 全量回算（在全量tidsets上做精确交集，确保结果完整性）
     → 互斥挑选 → 输出
```

采样比率从 20% 提升到 30%，minSupport 自动缩放，确保大词表场景下低频词不会被遗漏。

## 性能基准

### 小规模数据

| 文档数 | 平均耗时 | 分组数 | 吞吐量 |
|--------|---------|-------|--------|
| 100 | 1.0 ms | 4 | 100,000 docs/sec |
| 500 | 2.3 ms | 4 | 214,286 docs/sec |
| 1,000 | 2.7 ms | 5 | 375,000 docs/sec |
| 2,000 | 5.0 ms | 4 | 400,000 docs/sec |

### 大规模数据（两阶段索引构建）

| 文档数 | 词表大小 | minSupport | 旧版耗时 | 优化后耗时 | 加速比 |
|--------|---------|-----------|---------|-----------|-------|
| 20,000 | 10,000 | 1,000 | 422 ms | 260 ms | **1.6x** |
| 50,000 | 5,000 | 2,500 | 475 ms | 346 ms | **1.4x** |
| 100,000 | 20,000 | 5,000 | 5,787 ms | 3,951 ms | **1.5x** |

### 采样质量（全量 vs 30%采样）

| 文档数 | 词表大小 | Groups差异 | Doc-refs差异 |
|--------|---------|-----------|-------------|
| 20,000 | ~500 | 0 | -8 |
| 50,000 | ~2,000 | 0 | -48 |
| 50,000 | ~40 | 0 | -120 |
| 100,000 | ~40 | 0 | -182 |

*测试环境: JDK 1.8.0_92, Windows 10*

## 性能瓶颈分布（50K docs）

| 阶段 | 占比 |
|------|------|
| 索引构建 | 98.3% |
| 频繁项集挖掘 | 1.0% |
| 互斥挑选 | 0.1% |
| 结果输出 | 0.5% |

## 优化清单

### ✅ 已完成

1. **两阶段索引构建** — 先统计频率再按需分配BitSet，大词表下内存降50%，时间降34-50%。
2. **ByteArrayKey.forLookup()** — 阶段二扫描使用零拷贝查找键，消除第二次扫描的数组拷贝开销。
3. **采样策略优化** — 采样比率20%→30%，minSupport自动按采样比例缩放，大词表质量显著改善。
4. **移除冗余缓存代码** — 原缓存包装（HashMap<String, Double>）因0%命中率已移除。
5. **bitSetToDocIds空集优化** — 空BitSet快速返回空列表。
6. **POM结构优化** — 移除未使用的Lucene依赖，正确配置`sourceDirectory`。
7. **构建文档与基准测试** — FptokenBenchmark、TwoPhaseIndexTest、BottleneckProfiler等。

### 📋 待评估

- **RoaringBitmap替代BitSet** — 稀疏位图可再降内存4-6倍，但增加序列化复杂度（P1优先级）
- **intersects()快速跳过优化** — BeamFrequentItemsetMiner中利用BitSet.intersects()代替cardinality()做快速失败（P2）
- **采样策略重构** — 当前采样在索引构建之后做位过滤，无法节省索引构建时间（索引占98%）。若需节省索引时间，需改为"先采样文档再建索引"

## 构建

```bash
# 编译
mvn clean compile

# 打包（不含测试）
mvn clean package

# 运行基准测试
javac -cp target\fptoken-1.0.0.jar -d target\classes FptokenBenchmark.java
java -cp target\fptoken-1.0.0.jar;target\classes FptokenBenchmark
```

## 项目结构

```
src/
  cn/lxdb/plugins/muqingyu/fptoken/
    config/          配置类（SelectorConfig, EngineTuningConfig）
    index/           索引构建（TermTidsetIndex, TermDocumentIndex）
    miner/           频繁项集挖掘（BeamFrequentItemsetMiner）
    model/           数据模型（CandidateItemset, DocTerms, SelectedGroup等）
    picker/          互斥选择器（Greedy, GreedySwapBased, TwoPhase）
    util/            工具类（ByteArrayKey, ByteArrayUtils）
    example/         使用示例
    ExclusiveFrequentItemsetSelector.java   门面类（主入口）
    MutuallyExclusivePatternSelector.java   模式选择器
```
