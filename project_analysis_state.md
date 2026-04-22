# fptoken Project Analysis State

## Current Status (2026-04-22 17:10)

### ✅ Completed

1. **两阶段索引构建** (`buildWithSupportBounds`)
   - Phase 1: 统计频率（HashMap<ByteArrayKey, int[]>），不分配 BitSet
   - Phase 2: 只对满足 [minSupport, maxSupport] 的 term 分配 BitSet
   - 效果: 100K docs/20K vocab: 内存从 2.2GB→1.0GB，时间 5.8s→3.9s（-32%）
   
2. **ByteArrayKey.forLookup() 零拷贝查找**
   - 阶段二扫描使用 `forLookup(rawTerm, hash)` 替代 `new ByteArrayKey(rawTerm)`
   - 消除第二次扫描中的数组拷贝（Arrays.copyOf），降低 GC 压力
   - 效果: 100K docs/20K vocab: 4.8s→3.9s（额外 -18%）

3. **采样策略优化**
   - 采样比率: 20% → 30%
   - minSupport 自动缩放: 默认按 SAMPLE_RATIO (0.3) 缩放
   - `setSamplingSupportScale(0.0)` = auto-scale, `setSamplingSupportScale(1.0)` = no scale
   - 采样质量: 大词表下 doc-refs 差异从 -19254 降到 -48（50K docs）

4. **采样路径重构**
   - 保持"先建全量索引 → 位过滤提取采样集 → 挖掘 → 全量回算"路径
   - 只建一次全量索引（两阶段），采样阶段只做 BitSet.and()
   - 不需要额外建索引，不需要 termId 映射

### 🔄 Current Bottleneck

- 索引构建占 98.3%，采样无法优化索引阶段
- 小数据下采样速度持平（1.0x），大数据下采样结果质量好但速度无优势
- 核心: 两阶段索引构建的两次扫描 + HashMap 查找仍是瓶颈

### 📋 Pending Optimizations

**P0: 两阶段索引构建合并为单次扫描**
- 当前两次扫描都做 HashMap lookup + ByteArrayKey 创建（虽然后者已优化为 forLookup）
- 思路: 阶段一统计频率时同时记录 `(termId, docId)` 对，阶段二直接设置 BitSet
- 预计加速: 再降 15-25%

**P1: RoaringBitmap**
- 稀疏位图节省 4-6x 内存
- 需评估序列化兼容性和性能影响
- 与二进制检索（最终目的）的格式对接

**P1: intersects() 快速跳过**
- BeamFrequentItemsetMiner 中 `if (tidset.cardinality() < minSupport)` 可改为 `if (!tidset.intersects(...))`
- 仅在极低 minSupport 下有效

**P2: 先采样再建索引**
- 如果采样文档集远小于全量（< 10%），可以先采样文档再只在采样集上建索引
- 当前 30% 采样 + 两次索引构建（采样+全量）反而更慢
- 仅当采样比率 < 10% 或全量数据极大时才有价值

### Files Modified

- `src/.../ExclusiveFrequentItemsetSelector.java` - 采样路径重构，minSupport 缩放
- `src/.../index/TermTidsetIndex.java` - buildWithSupportBounds, forLookup 优化
- `src/.../util/ByteArrayKey.java` - 新增 forLookup() 静态方法
- `README.md` - 更新性能数据和优化清单

### Test Files

- `TwoPhaseIndexTest.java` - 两阶段索引构建验证
- `BottleneckProfiler.java` - 瓶颈分析
- `SamplingCompareTest.java` - 采样 vs 全量对比
- `LargeVocabTest.java` - 大词表测试
- `LargeScaleTest.java` - 大规模测试
- `FptokenBenchmark.java` - 基准测试
