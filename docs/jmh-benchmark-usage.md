# JMH Benchmark Usage

当前项目是 Eclipse 风格工程（没有 Maven/Gradle wrapper），所以先提供一个可复制的
JMH 基准测试模板，等你接入依赖后直接落地到测试目录即可。

## 依赖

- `org.openjdk.jmh:jmh-core`
- `org.openjdk.jmh:jmh-generator-annprocess`

## 基准代码模板

```java
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class BeamFrequentItemsetMinerJmhBenchmark {

    @Param({"10000"}) private int docCount;
    @Param({"2000"}) private int termCount;
    @Param({"0.02"}) private double hitRate;
    @Param({"3"}) private int minSupport;
    @Param({"2"}) private int minItemsetSize;
    @Param({"5"}) private int maxItemsetSize;
    @Param({"20000"}) private int maxCandidateCount;
    @Param({"1000"}) private int maxFrequentTermCount;
    @Param({"16"}) private int maxBranchingFactor;
    @Param({"64"}) private int beamWidth;

    private BeamFrequentItemsetMiner miner;
    private SelectorConfig config;
    private List<BitSet> tidsetsByTermId;

    @Setup(Level.Trial)
    public void setup() {
        this.miner = new BeamFrequentItemsetMiner();
        this.config = new SelectorConfig(minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        this.tidsetsByTermId = syntheticTidsets(termCount, docCount, hitRate, 42L);
    }

    @Benchmark
    public FrequentItemsetMiningResult benchmarkBeamSearch() {
        return miner.mineWithStats(
                tidsetsByTermId,
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth
        );
    }

    private static List<BitSet> syntheticTidsets(int terms, int docs, double p, long seed) {
        Random random = new Random(seed);
        List<BitSet> out = new ArrayList<>(terms);
        for (int termId = 0; termId < terms; termId++) {
            BitSet bits = new BitSet(docs);
            for (int docId = 0; docId < docs; docId++) {
                if (random.nextDouble() < p) {
                    bits.set(docId);
                }
            }
            out.add(bits);
        }
        return out;
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(BeamFrequentItemsetMinerJmhBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .build();
        new Runner(options).run();
    }
}
```

## 建议先比较的指标

- 平均耗时（`ms/op`）
- 各版本的 `intersectionCount` 趋势
- 各版本候选截断比例（是否更早触发 `maxCandidateCount`）
