# fptoken 项目今日行动记录（2026-04-22）

> 本文档记录今日各角色的具体行动：做了什么、如何做的、具体思路。
> 每个角色独立撰写，汇总在同一文件中。

---

## 1. Developer（开发工程师） — 今日行动记录

### 总体时间线

```
14:00 收到任务：对 fptoken 进行架构优化和性能优化
14:10 拉取各角色分析
14:20 编写 BottleneckProfiler 定位瓶颈
14:30 发现索引构建占 98%，确认优化方向
14:40 实现采样策略重构（20%→30%，位过滤采样）
15:00 实现两阶段索引构建 buildWithSupportBounds
15:10 发现采样质量下降（doc-refs 差异 -19254）
15:20 修复采样质量：提升采样比例 + 自动缩放 minSupport
15:30 实现 ByteArrayKey.forLookup() 零拷贝
15:40 验证：采样质量恢复（Groups diff=0）
15:50 发现采样无法绕过索引构建瓶颈
16:00 实现 new BitSet() 按需扩容
16:10 两阶段索引构建效果验证：2x 加速，146x 内存节省
16:20 采样参数配置化（EngineTuningConfig）
16:30 实现 OpenHashTable（尝试替代 HashMap）
16:40 发现 OpenHashTable 在大词表场景下无优势
16:50 确定最终方案：buildWithSupportBounds 过滤低频词
17:00 采样质量最终验证通过（全规模一致）
17:10 收到老板指令：各角色写文档
17:20-17:50 创建协作文档（现名见 docs/README.md「协作草稿」）
```

### 具体行动

#### 行动 1：瓶颈定位（BottleneckProfiler.java）

**思路：** 不能靠猜测，要用数据说话。写一个精确计时的 profiler，把 pipeline 分成 4 个阶段分别计时。

**做法：**
```java
Phase 1 - Index build: 224.8 ms (98.0%)
Phase 2 - Mining:        3.1 ms ( 1.4%)
Phase 3 - Picking:       0.2 ms ( 0.1%)
Phase 4 - To output:     1.2 ms ( 0.5%)
```

**结论：** 索引构建是唯一瓶颈，采样只能优化 1.4% 的挖掘阶段。

#### 行动 2：采样策略重构

**思路：** 原始采样先建采样集索引再回算全量索引，建两次索引成本翻倍。改为先建一次全量索引，用位过滤产生采样 tidsets。

**做法：**
1. 修改 `selectExclusiveBestItemsets` 方法
2. 先调用 `buildWithSupportBounds` 建全量索引
3. 如果启用了采样且采样集 < 全量：随机选文档 → 创建 BitSet 掩码 → 对每个 tidset 做 clone().and(mask)
4. 在采样 tidsets 上挖掘（minSupport 按采样比例缩放）
5. 对挖掘结果在全量 tidsets 上回算（做精确交集）
6. 回算后过滤掉不满足全量 minSupport 的组合

**关键代码：**
```java
// 采样掩码
BitSet sampleMask = new BitSet(docCount);
for (int sid : sampledDocIds) {
    sampleMask.set(sid);
}
// 位过滤
List<BitSet> sampledTidsets = new ArrayList<>(fullTidsets.size());
for (BitSet full : fullTidsets) {
    BitSet s = (BitSet) full.clone();
    s.and(sampleMask);
    sampledTidsets.add(s);
}
// minSupport 自动缩放
double effectiveScale = sampleRatio;
int sampledMinSupport = Math.max(1, (int) Math.round(minSupport * effectiveScale));
```

#### 行动 3：两阶段索引构建（buildWithSupportBounds）

**思路：** 低频词占唯一词 75-85%，每个低频词都分配完整的 BitSet(docCount) 是巨大的浪费。先统计频率再按需分配。

**做法：**
1. 第一次扫描：统计每个词的出现频率（HashMap<ByteArrayKey, int[]>）
2. 过滤：只保留 support ∈ [minSupport, maxSupport] 的词
3. 为通过过滤的词分配 BitSet（new BitSet(docCount) 或 new BitSet()）
4. 第二次扫描：对通过过滤的词设位图（ByteArrayKey.forLookup 零拷贝）

#### 行动 4：ByteArrayKey.forLookup()

**思路：** 第二次扫描中每个词都要创建 ByteArrayKey 做 HashMap 查找，但 ByteArrayKey 构造时会拷贝数组。对于只做查找的场景，不需要拷贝。

**做法：** 新增静态方法，直接引用原始数组，不拷贝。
```java
public static ByteArrayKey forLookup(byte[] value, int hash) {
    ByteArrayKey k = new ByteArrayKey();
    k.value = value;  // 直接引用，不拷贝
    k.hash = hash;
    return k;
}
```

#### 行动 5：new BitSet() 按需扩容

**思路：** 既然低频词浪费空间，为什么不用 `new BitSet()` 空构造器？BitSet 内部会自动扩容，低频词只占 64 bits，高频词自动增长。

**做法：** 把 `build()` 中的 `new BitSet(docCount)` 改为 `new BitSet()`。

**效果验证：**
```
旧版：100K docs/20K vocab，bits=357786KB，时间=5150ms
新版：100K docs/20K vocab，bits=2442KB，时间=2572ms
加速比：2x，内存节省：146x
```

#### 行动 6：OpenHashTable 尝试

**思路：** Java Perf Guru 建议用自定义哈希表减少装箱开销。用三个并行数组（keys[], hashes[], values[]）替代 HashMap。

**做法：** 实现开放寻址线性探测哈希表。
```java
public final class OpenHashTable {
    private byte[][] keys;
    private int[] hashes;
    private int[] values;
    private int size;
    private int mask;
    // ...
}
```

**结果：** 在 41 万唯一词场景下不比 HashMap 快。因为 HashMap 桶链已很短，开放寻址在冲突时遍历更多槽。

#### 行动 7：采样参数配置化

**思路：** 老板要求采样比例不要写死。

**做法：** 从 `EngineTuningConfig` 读取默认值，通过 setter 方法暴露。
```java
private static double sampleRatio = EngineTuningConfig.DEFAULT_SAMPLE_RATIO;
private static int minSampleCount = EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT;
private static double samplingSupportScale = EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE;

public static void setSampleRatio(double ratio) { ... }
public static void setMinSampleCount(int count) { ... }
public static void setSamplingSupportScale(double scale) { ... }
```

#### 行动 8：文档编写

按老板要求创建 5 个文档：
- `docs/optimization-summary-collaborative.md` — 各角色改进意见
- `docs/worklog-collaborative-2026-04-22.md` — 今日工作日志（思考过程）
- `docs/future-directions-collaborative.md` — 未来展望（重点膨胀率和检索）
- `docs/testing-plan-collaborative.md` — 测试规划
- `docs/action-log-collaborative-2026-04-22.md` — 今日行动记录（本文档）

---

## 2. Architect（架构师） — 今日行动记录

### 思路

从架构层面审视 fptoken 的整体设计，评估采样策略和索引构建的合理性，给出优先级建议。

### 具体行动

#### 行动 1：架构审查

**做了什么：** 审查了 `ExclusiveFrequentItemsetSelector`、`TermTidsetIndex`、`BeamFrequentItemsetMiner` 的架构关系。

**发现：** 
- 索引构建和挖掘耦合在 `selectExclusiveBestItemsets` 方法中
- 采样在索引构建之后做位过滤，对 98% 的瓶颈无效
- 但这是正确的——采样本意是优化挖掘阶段，不是索引构建

**结论：** 采样无法绕过索引构建瓶颈。必须从索引构建本身入手。

#### 行动 2：两阶段索引构建评估

**做了什么：** 评估 Developer 提出的两阶段索引构建方案。

**判断：** 先统计频率再按需分配 BitSet，架构上正确且有效。在大词表场景下内存降低 4-6 倍。

**建议：** P0 优先级，立即实施。

#### 行动 3：RoaringBitmap 评估

**做了什么：** 评估是否需要用 RoaringBitmap 替代 BitSet。

**结论：** 两阶段索引构建 + new BitSet() 按需扩容已解决核心问题。RoaringBitmap 可以进一步减少内存但收益递减。**老板确认不需要，已移除待办。**

#### 行动 4：文档输出

将架构分析结论写入上述协作 Markdown 文件。

---

## 3. Java Perf Guru（Java 性能专家） — 今日行动记录

### 思路

从 JVM 和性能角度分析瓶颈来源，给出具体的代码级和 JVM 级调优建议。

### 具体行动

#### 行动 1：性能瓶颈分析

**做了什么：** 分析 BottleneckProfiler 数据 + 代码审查。

**发现：**
1. 索引构建占 98.2%——这是唯一的性能瓶颈
2. `build()` 中 `new BitSet(docCount)` 为低频词浪费大量内存
3. `buildWithSupportBounds` 两次扫描中 HashMap 操作密集

**建议：**
1. 用 `new BitSet()` 替代 `new BitSet(docCount)`（P0，已实施）
2. 用 `ByteArrayKey.forLookup()` 减少第二次扫描数组拷贝（P0，已实施）
3. 自定义 int[] 哈希表替代 HashMap（P1，尝试后无优势）

#### 行动 2：GC 分析

**做了什么：** 分析两阶段扫描的对象创建模式。

**发现：** 100K docs 下 Young GC 频繁。第一次扫描创建 41 万 ByteArrayKey + HashMap Entry，第二次扫描创建 1230 万 ByteArrayKey.forLookup。

**建议：** 预分配 HashMap 容量、固定堆大小、使用 G1GC。

#### 行动 3：OpenHashTable 实现评估

**做了什么：** 审查 Developer 的 OpenHashTable 实现。

**发现问题：** 开放寻址的线性探测在冲突时性能下降，且 byte[] 相等比较开销与 HashMap 相同。

**结论：** HashMap 在当前场景下足够好。不推荐替换。

#### 行动 4：文档输出

将性能分析结论写入各文档。

---

## 4. QA Lead（质量保证负责人） — 今日行动记录

### 思路

确保优化不会引入质量回归，验证采样结果的正确性。

### 具体行动

#### 行动 1：采样质量验证

**做了什么：** 设计并运行对比测试。

**做法：**
1. `SamplingCompareTest`：500~10000 文档，随机数据
2. `LargeScaleTest`：20000~100000 文档，随机数据
3. `LargeVocabTest`：50000 文档，大词表确定性数据

**发现的问题：**
- 20% 采样在大词表下 doc-refs 差异达 -19254
- 根因：低频词在采样集中消失
- 修复后：30% 采样 + minSupport 缩放 + 全量回算 → Groups diff=0

#### 行动 2：0 groups 问题排查

**做了什么：** 发现采样路径挖不到组合（0 groups）。

**排查过程：**
1. 检查 minSupport 缩放逻辑
2. 发现 `samplingSupportScale=1.0` 时 minSupport 不缩放
3. 30% 采样集上高频词被稀释，达不到全量 minSupport
4. 回退到自动缩放（默认按采样比例缩放）

#### 行动 3：文档输出

将质量验证结论写入各文档。

---

## 5. Search Guru（搜索专家） — 今日行动记录

### 思路

从检索场景角度评估优化方向，确保频繁项集在二进制检索中有实际价值。

### 具体行动

#### 行动 1：检索场景分析

**做了什么：** 分析频繁项集在二进制检索中的作用。

**核心发现：** 只有高频组合对压缩率有贡献。低频组合即使丢失也不影响检索性能。

**结论：** 当前采样策略（30% + minSupport 缩放）在检索场景下安全。

#### 行动 2：膨胀率分析

**做了什么：** 分析膨胀率控制的重要性。

**建议：** 应该优先监控膨胀率，而不是只关注挖掘时间。一个挖掘快但膨胀率高的方案是无用的。

#### 行动 3：文档输出

将检索场景分析写入各文档。

---

## 6. Big Data Guru（大数据专家） — 今日行动记录

### 思路

评估当前方案在大规模场景下的可扩展性，给出分片并行建议。

### 具体行动

#### 行动 1：可扩展性分析

**做了什么：** 分析当前 O(n) 方案在 100K~10M docs 下的表现。

**发现：** 100K docs ~2.5s，1M docs 预估 ~25s，10M docs 预估 ~250s。

**结论：** 百万级可接受，千万级需要并行化。

#### 行动 2：并行方案设计

**做了什么：** 设计分片并行索引构建方案。

**思路：** ForkJoinPool 分片处理 → 各片独立统计频率 → 合并节点汇总。

#### 行动 3：文档输出

将大规模分析写入各文档。

---

## 7. OS Tuning Guru（操作系统调优专家） — 今日行动记录

### 思路

从系统层面分析性能瓶颈，给出 JVM 参数和系统级调优建议。

### 具体行动

#### 行动 1：系统资源分析

**做了什么：** 分析当前方案的 CPU/内存/I/O 消耗模式。

**发现：** CPU 计算密集，内存有 GC 压力，无 I/O 瓶颈。

**建议：** 固定堆大小、G1GC、预分配 HashMap。

#### 行动 2：JVM 参数建议

**做了什么：** 提供 4 组推荐的 JVM 参数组合用于不同场景测试。

#### 行动 3：文档输出

将系统级分析写入各文档。

---

*文档版本：1.0 | 最后更新：2026-04-22*
*各角色独立撰写，汇总于此。*
