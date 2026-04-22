package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class ComparisonWithBaselineTest {

    @Test
    void compareWithSimpleNgram() {
        List<DocTerms> rows = createLongPatternDataset(9000);
        CompressibilityPipelineSupport.PipelineResult selector = CompressibilityPipelineSupport.run(
                rows, 50, 3, 10, 260000, 5000, 32, 96, 120);

        TermTidsetIndex index = selector.getIndex();
        Set<Integer> top3GramTermIds = selectTop3GramTermIds(
                index,
                Math.max(32, selector.getMetrics().getSelectedItemsetCount() * 2));
        CompressionMetricsUtil.CompressionMetrics ngramMetrics =
                CompressionMetricsUtil.calculateCoverageAndPotentialSavingsFromTermIds(top3GramTermIds, index, rows);

        System.out.println("selector ratio=" + String.format("%.4f", selector.getMetrics().getCompressionRatio())
                + ", coverage=" + String.format("%.4f", selector.getMetrics().getCoverageRatio())
                + ", dictBytes=" + selector.getMetrics().getDictionaryBytes()
                + ", elapsedMs=" + selector.getElapsedMs());
        System.out.println("ngram ratio=" + String.format("%.4f", ngramMetrics.getCompressionRatio())
                + ", coverage=" + String.format("%.4f", ngramMetrics.getCoverageRatio())
                + ", dictBytes=" + ngramMetrics.getDictionaryBytes());

        // 3-gram 覆盖率可能更高，但长模式编码更紧凑；这里重点比较整体压缩收益。
        assertTrue(selector.getMetrics().getAverageItemsetLength() >= 2d);
        assertTrue(selector.getMetrics().getCompressionRatio() >= ngramMetrics.getCompressionRatio() - 0.15d);
    }

    @Test
    void compareWithDifferentSelectorStrategies() {
        List<DocTerms> rows = createLongPatternDataset(9000);

        CompressibilityPipelineSupport.PipelineResult strategyA = CompressibilityPipelineSupport.run(
                rows, 20, 3, 10, 280000, 6000, 48, 128, 180);
        CompressibilityPipelineSupport.PipelineResult strategyB = CompressibilityPipelineSupport.run(
                rows, 100, 3, 6, 220000, 4096, 24, 64, 100);
        CompressibilityPipelineSupport.PipelineResult strategyC = CompressibilityPipelineSupport.run(
                rows, 200, 3, 4, 150000, 3000, 16, 32, 50);

        System.out.println("A ratio=" + String.format("%.4f", strategyA.getMetrics().getCompressionRatio())
                + ", coverage=" + String.format("%.4f", strategyA.getMetrics().getCoverageRatio())
                + ", elapsedMs=" + strategyA.getElapsedMs());
        System.out.println("B ratio=" + String.format("%.4f", strategyB.getMetrics().getCompressionRatio())
                + ", coverage=" + String.format("%.4f", strategyB.getMetrics().getCoverageRatio())
                + ", elapsedMs=" + strategyB.getElapsedMs());
        System.out.println("C ratio=" + String.format("%.4f", strategyC.getMetrics().getCompressionRatio())
                + ", coverage=" + String.format("%.4f", strategyC.getMetrics().getCoverageRatio())
                + ", elapsedMs=" + strategyC.getElapsedMs());

        assertTrue(strategyA.getMetrics().getCoverageRatio() + 0.03d >= strategyC.getMetrics().getCoverageRatio());
        assertTrue(strategyC.getElapsedMs() <= strategyA.getElapsedMs() * 2L + 1L);
    }

    private static Set<Integer> selectTop3GramTermIds(TermTidsetIndex index, int k) {
        List<byte[]> terms = index.getIdToTermUnsafe();
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < terms.size(); i++) {
            if (terms.get(i).length == 3) {
                ids.add(Integer.valueOf(i));
            }
        }
        ids.sort(Comparator.<Integer>comparingInt(id -> index.getTidsetsByTermIdUnsafe().get(id.intValue()).cardinality())
                .reversed());
        Set<Integer> out = new HashSet<>();
        for (int i = 0; i < ids.size() && i < k; i++) {
            out.add(ids.get(i));
        }
        return out;
    }

    private static List<DocTerms> createLongPatternDataset(int docs) {
        return CompressibilityAssessmentFixtures.longPatternDataset(docs, 20260424L);
    }
}
