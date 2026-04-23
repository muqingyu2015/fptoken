package cn.lxdb.plugins.muqingyu.fptoken.tests.demo;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 调用链演示测试（默认不执行）：
 * 从输入到输出拆成 10 步，逐步打印每一步的输入和输出，便于做方案讲解。
 *
 * <p>运行方式：
 * <pre>
 *   scripts/run-fptoken-tests.ps1
 *   + JVM 参数: -Dfptoken.runCallChainDemo=true
 * </pre>
 */
@Tag("demo")
@EnabledIfSystemProperty(named = "fptoken.runCallChainDemo", matches = "true")
class CallChainTenStepsDemoTest {

    @Test
    void demo_callChain_shouldPrintTenStepsInputAndOutput() {
        int ngramStart = 2;
        int ngramEnd = 4;
        int minSupport = 2;
        int minItemsetSize = 2;
        int maxItemsetSize = 4;
        int maxCandidateCount = 20_000;
        int hotTermThresholdExclusive = 2;

        // Step 1: 准备原始输入行（模拟业务输入）
        List<String> rawLines = buildDemoLines();
        printStep(1, "准备输入文本行",
                "输入: " + rawLines.size() + " 行文本",
                "输出: 每行将被转成 byte[]，并赋予 docId");
        for (int i = 0; i < rawLines.size(); i++) {
            System.out.println("  line[" + i + "] = \"" + rawLines.get(i) + "\"");
        }

        // Step 2: 文本 -> byte[]（演示 byte 的可读展示）
        List<byte[]> lineBytes = new ArrayList<byte[]>(rawLines.size());
        for (String line : rawLines) {
            lineBytes.add(line.getBytes(StandardCharsets.UTF_8));
        }
        printStep(2, "文本转字节",
                "输入: 文本行",
                "输出: byte[]（展示 text + hex）");
        for (int i = 0; i < lineBytes.size(); i++) {
            System.out.println("  bytes[" + i + "] = " + readableBytes(lineBytes.get(i)));
        }

        // Step 3: 构建原始 DocTerms（每行先只放 1 个原始 byte[]）
        List<DocTerms> rawRows = new ArrayList<DocTerms>(lineBytes.size());
        for (int i = 0; i < lineBytes.size(); i++) {
            rawRows.add(new DocTerms(i, Collections.singletonList(lineBytes.get(i))));
        }
        printStep(3, "构建原始 rows",
                "输入: byte[] + docId",
                "输出: List<DocTerms>（每条先保留原始整行）");
        printRowsPreview(rawRows, 4);

        // Step 4: n-gram 切词（2~4）
        List<DocTerms> tokenizedRows = new ArrayList<DocTerms>(rawRows.size());
        for (DocTerms row : rawRows) {
            List<byte[]> terms = row.getTermsUnsafe();
            List<byte[]> tokenized = ByteNgramTokenizer.tokenize(terms.get(0), ngramStart, ngramEnd);
            tokenizedRows.add(new DocTerms(row.getDocId(), tokenized));
        }
        printStep(4, "n-gram 切词",
                "输入: 原始 rows（每条一个 byte[]）",
                "输出: 分词后 rows（每条多个 term）");
        printTokenizedPreview(tokenizedRows, 2, 8);

        // Step 5: 建立 term -> docId 位图索引
        TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
                tokenizedRows, minSupport, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
        List<byte[]> vocabulary = fullIndex.getIdToTermUnsafe();
        List<BitSet> fullTidsets = fullIndex.getTidsetsByTermIdUnsafe();
        printStep(5, "建立反查索引（term -> doc 位图）",
                "输入: 分词后 rows",
                "输出: 词表 + 每个词对应的命中行位图");
        printTermTidsetPreview(vocabulary, fullTidsets, 6);

        // Step 6: 采样（演示版：取前 70% docId）
        int sampleSize = Math.max(1, (int) Math.ceil(rawRows.size() * 0.7d));
        int[] sampledDocIds = firstNDocIds(rawRows.size(), sampleSize);
        BitSet sampleMask = new BitSet(rawRows.size());
        for (int sid : sampledDocIds) {
            sampleMask.set(sid);
        }
        List<BitSet> sampledTidsets = new ArrayList<BitSet>(fullTidsets.size());
        for (BitSet full : fullTidsets) {
            BitSet sampled = (BitSet) full.clone();
            sampled.and(sampleMask);
            sampledTidsets.add(sampled);
        }
        int sampledMinSupport = Math.max(1, (int) Math.round(minSupport * ((double) sampleSize / rawRows.size())));
        printStep(6, "采样并生成样本索引",
                "输入: 全量 tidsets + 采样 docId",
                "输出: sampledTidsets + sampledMinSupport=" + sampledMinSupport);
        System.out.println("  sampledDocIds = " + toIntList(sampledDocIds));

        // Step 7: 在采样数据上挖掘候选组合
        SelectorConfig sampledConfig = new SelectorConfig(
                sampledMinSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult mining = miner.mineWithStats(
                sampledTidsets,
                sampledConfig,
                EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT,
                EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR,
                EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH,
                2,
                5_000L
        );
        printStep(7, "样本挖掘候选",
                "输入: sampledTidsets + 配置",
                "输出: candidates=" + mining.getCandidates().size());
        printCandidatePreview(mining.getCandidates(), vocabulary, 6);

        // Step 8: 在全量数据上回算支持度
        List<CandidateItemset> recomputed = recomputeOnFullData(
                mining.getCandidates(), fullTidsets, minSupport);
        printStep(8, "全量回算支持度",
                "输入: 样本候选 + 全量 tidsets",
                "输出: 回算后候选=" + recomputed.size());
        printCandidatePreview(recomputed, vocabulary, 6);

        // Step 9: 做互斥选择，转成可读 group
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selectedCandidates = picker.pick(
                recomputed,
                Math.max(1, vocabulary.size()),
                EngineTuningConfig.DEFAULT_MAX_SWAP_TRIALS,
                EngineTuningConfig.DEFAULT_MIN_NET_GAIN
        );
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, vocabulary);
        printStep(9, "互斥挑选（去重取舍）",
                "输入: 回算后候选",
                "输出: selectedGroups=" + selectedGroups.size());
        printGroupPreview(selectedGroups, 6);

        // Step 10: 生成最终业务输出（三层结果）
        ExclusiveSelectionResult selectionResult = new ExclusiveSelectionResult(
                selectedGroups,
                mining.getFrequentTermCount(),
                mining.getGeneratedCandidateCount(),
                mining.getIntersectionCount(),
                maxCandidateCount,
                mining.isTruncatedByCandidateLimit()
        );
        LineFileProcessingResult.DerivedData derivedData =
                ExclusiveFpRowsProcessingApi.buildDerivedData(
                        tokenizedRows, selectionResult, hotTermThresholdExclusive);
        LineFileProcessingResult finalResult = new LineFileProcessingResult(
                rawRows,
                selectionResult,
                derivedData,
                EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM
        );
        printStep(10, "生成最终三层输出",
                "输入: selectionResult + derivedData",
                "输出: FinalIndexData（三层结构 + 统一统计）");
        printFinalResultSummary(finalResult);

        assertTrue(!finalResult.getFinalIndexData().getHighFreqMutexGroupPostings().isEmpty(),
                "demo data should produce at least one high-frequency group");
    }

    private static List<String> buildDemoLines() {
        List<String> lines = new ArrayList<String>();
        lines.add("GET /api/order?id=1001 HTTP/1.1");
        lines.add("GET /api/order?id=1002 HTTP/1.1");
        lines.add("POST /api/pay?id=1001 HTTP/1.1");
        lines.add("GET /api/order?id=1001 HTTP/1.1");
        lines.add("POST /api/pay?id=1002 HTTP/1.1");
        lines.add("GET /api/order?id=1003 HTTP/1.1");
        return lines;
    }

    private static void printStep(int stepNo, String title, String input, String output) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("Step " + stepNo + ": " + title);
        System.out.println("Input : " + input);
        System.out.println("Output: " + output);
        System.out.println("------------------------------------------------------------");
    }

    private static String readableBytes(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8)
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
        return "text=\"" + text + "\", hex=" + ByteArrayUtils.toHex(bytes);
    }

    private static void printRowsPreview(List<DocTerms> rows, int limit) {
        int bound = Math.min(limit, rows.size());
        for (int i = 0; i < bound; i++) {
            DocTerms row = rows.get(i);
            byte[] raw = row.getTermsUnsafe().get(0);
            System.out.println("  docId=" + row.getDocId() + ", raw=" + readableBytes(raw));
        }
    }

    private static void printTokenizedPreview(List<DocTerms> rows, int rowLimit, int termLimit) {
        int bound = Math.min(rowLimit, rows.size());
        for (int i = 0; i < bound; i++) {
            DocTerms row = rows.get(i);
            List<byte[]> terms = row.getTermsUnsafe();
            System.out.println("  docId=" + row.getDocId() + ", termCount=" + terms.size());
            int termBound = Math.min(termLimit, terms.size());
            for (int j = 0; j < termBound; j++) {
                System.out.println("    term[" + j + "]=" + readableBytes(terms.get(j)));
            }
        }
    }

    private static void printTermTidsetPreview(List<byte[]> vocabulary, List<BitSet> tidsets, int limit) {
        int bound = Math.min(limit, vocabulary.size());
        for (int i = 0; i < bound; i++) {
            System.out.println("  termId=" + i
                    + ", term=" + readableBytes(vocabulary.get(i))
                    + ", docIds=" + bitsetToDocIds(tidsets.get(i)));
        }
    }

    private static void printCandidatePreview(List<CandidateItemset> candidates, List<byte[]> vocabulary, int limit) {
        int bound = Math.min(limit, candidates.size());
        for (int i = 0; i < bound; i++) {
            CandidateItemset candidate = candidates.get(i);
            System.out.println("  candidate[" + i + "] support=" + candidate.getSupport()
                    + ", saving=" + candidate.getEstimatedSaving()
                    + ", terms=" + candidateTermsReadable(candidate, vocabulary));
        }
    }

    private static void printGroupPreview(List<SelectedGroup> groups, int limit) {
        int bound = Math.min(limit, groups.size());
        for (int i = 0; i < bound; i++) {
            SelectedGroup group = groups.get(i);
            List<byte[]> terms = group.getTerms();
            List<String> readableTerms = new ArrayList<String>(terms.size());
            for (byte[] term : terms) {
                readableTerms.add(readableBytes(term));
            }
            System.out.println("  group[" + i + "] support=" + group.getSupport()
                    + ", terms=" + readableTerms
                    + ", docIds=" + group.getDocIds());
        }
    }

    private static void printFinalResultSummary(LineFileProcessingResult result) {
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();
        LineFileProcessingResult.ProcessingStats stats = result.getProcessingStats();
        System.out.println("  highFreqMutexGroupPostings=" + finalData.getHighFreqMutexGroupPostings().size());
        System.out.println("  highFreqSingleTermPostings=" + finalData.getHighFreqSingleTermPostings().size());
        System.out.println("  lowHitForwardRows=" + finalData.getLowHitForwardRows().size());
        System.out.println("  candidateCount=" + stats.getCandidateCount()
                + ", frequentTermCount=" + stats.getFrequentTermCount()
                + ", intersectionCount=" + stats.getIntersectionCount());
    }

    private static List<CandidateItemset> recomputeOnFullData(
            List<CandidateItemset> candidates,
            List<BitSet> fullTidsets,
            int minSupport
    ) {
        List<CandidateItemset> out = new ArrayList<CandidateItemset>(candidates.size());
        BitSet scratch = new BitSet(64);
        for (CandidateItemset candidate : candidates) {
            int[] termIds = candidate.getTermIdsUnsafe();
            if (termIds.length == 0) {
                continue;
            }
            scratch.clear();
            scratch.or(fullTidsets.get(termIds[0]));
            for (int i = 1; i < termIds.length && scratch.cardinality() >= minSupport; i++) {
                scratch.and(fullTidsets.get(termIds[i]));
            }
            int support = scratch.cardinality();
            if (support >= minSupport) {
                out.add(CandidateItemset.trusted(termIds, (BitSet) scratch.clone(), support));
            }
        }
        return out;
    }

    private static List<SelectedGroup> toSelectedGroups(List<CandidateItemset> candidates, List<byte[]> vocabulary) {
        List<SelectedGroup> out = new ArrayList<SelectedGroup>(candidates.size());
        for (CandidateItemset candidate : candidates) {
            int[] termIds = candidate.getTermIdsUnsafe();
            List<byte[]> terms = new ArrayList<byte[]>(termIds.length);
            for (int termId : termIds) {
                terms.add(ByteArrayUtils.copy(vocabulary.get(termId)));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitsetToDocIds(candidate.getDocBitsUnsafe()),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    private static List<Integer> bitsetToDocIds(BitSet bitSet) {
        List<Integer> out = new ArrayList<Integer>();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            out.add(i);
        }
        return out;
    }

    private static int[] firstNDocIds(int total, int n) {
        int[] out = new int[Math.max(0, Math.min(total, n))];
        for (int i = 0; i < out.length; i++) {
            out[i] = i;
        }
        return out;
    }

    private static List<Integer> toIntList(int[] values) {
        List<Integer> out = new ArrayList<Integer>(values.length);
        for (int value : values) {
            out.add(value);
        }
        return out;
    }

    private static List<String> candidateTermsReadable(CandidateItemset candidate, List<byte[]> vocabulary) {
        int[] termIds = candidate.getTermIdsUnsafe();
        List<String> out = new ArrayList<String>(termIds.length);
        for (int termId : termIds) {
            out.add(readableBytes(vocabulary.get(termId)));
        }
        return out;
    }
}

