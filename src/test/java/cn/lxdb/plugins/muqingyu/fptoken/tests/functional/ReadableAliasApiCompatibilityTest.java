package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.api.MutuallyExclusivePatternSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermDocumentIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.PatternMinerWithBeamSearch;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.GreedySwapBasedPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReadableAliasApiCompatibilityTest {

    @Test
    void selectorAliasClass_matchesLegacyFacadeBehavior() {
        List<DocTerms> rows = sampleRows();

        List<SelectedGroup> legacy = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 12, 2, 6, 10_000);
        List<SelectedGroup> alias = MutuallyExclusivePatternSelector.select(rows, 12, 2, 6, 10_000);
        assertEquals(ByteArrayTestSupport.groupsFingerprint(legacy), ByteArrayTestSupport.groupsFingerprint(alias));

        ExclusiveSelectionResult legacyWithStats =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 12, 2, 6, 10_000);
        ExclusiveSelectionResult aliasWithStats =
                MutuallyExclusivePatternSelector.selectWithStats(rows, 12, 2, 6, 10_000);
        assertEquals(ByteArrayTestSupport.groupsFingerprint(legacyWithStats.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(aliasWithStats.getGroups()));
        assertEquals(legacyWithStats.getIntersectionCount(), aliasWithStats.getIntersectionCount());
    }

    @Test
    void indexAliasApis_matchLegacyBuildOutput() {
        List<DocTerms> rows = sampleRows();
        TermTidsetIndex legacyIndex = TermTidsetIndex.build(rows);
        TermTidsetIndex aliasMethodIndex = TermTidsetIndex.createFromDocuments(rows);
        TermDocumentIndex aliasClassIndex = TermDocumentIndex.createFromDocuments(rows);

        assertEquals(legacyIndex.getIdToTerm().size(), aliasMethodIndex.getIdToTerm().size());
        assertEquals(legacyIndex.getIdToTerm().size(), aliasClassIndex.getIdToTerm().size());
        assertEquals(legacyIndex.getTidsetsByTermId().size(), aliasClassIndex.getDocumentBitsetsByTermId().size());
    }

    @Test
    void minerAliasClass_matchesLegacyMinerOutput() {
        List<DocTerms> rows = sampleRows();
        SelectorConfig config = new SelectorConfig(12, 2, 6, 10_000);
        List<BitSet> tidsets = TermTidsetIndex.build(rows).getTidsetsByTermIdUnsafe();

        BeamFrequentItemsetMiner legacyMiner = new BeamFrequentItemsetMiner();
        PatternMinerWithBeamSearch aliasMiner = new PatternMinerWithBeamSearch();

        FrequentItemsetMiningResult legacy =
                legacyMiner.mineWithStats(tidsets, config, 1000, 16, 24, 3, 2000L);
        FrequentItemsetMiningResult alias =
                aliasMiner.findFrequentPatternsWithStatistics(tidsets, config, 1000, 16, 24, 3, 2000L);
        assertEquals(legacy.getGeneratedCandidateCount(), alias.getGeneratedCandidateCount());
        assertEquals(legacy.getIntersectionCount(), alias.getIntersectionCount());
    }

    @Test
    void pickerAliasClass_matchesTwoPhasePickerOutput() {
        List<CandidateItemset> candidates = sampleCandidates();
        TwoPhaseExclusiveItemsetPicker legacyPicker = new TwoPhaseExclusiveItemsetPicker();
        GreedySwapBasedPicker aliasPicker = new GreedySwapBasedPicker();

        List<CandidateItemset> legacy = legacyPicker.pick(candidates, 16, 20);
        List<CandidateItemset> alias = aliasPicker.selectOptimalMutuallyExclusiveSets(candidates, 16, 20);
        assertEquals(fingerprint(legacy), fingerprint(alias));
    }

    private static List<DocTerms> sampleRows() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] common = ByteArrayTestSupport.hex("A1B2");
        byte[] t1 = ByteArrayTestSupport.hex("11");
        byte[] t2 = ByteArrayTestSupport.hex("22");
        byte[] t3 = ByteArrayTestSupport.hex("33");
        for (int i = 0; i < 20; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, t1));
        }
        for (int i = 20; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, t2));
        }
        for (int i = 40; i < 60; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, t3));
        }
        return rows;
    }

    private static List<CandidateItemset> sampleCandidates() {
        List<CandidateItemset> out = new ArrayList<>();
        out.add(candidate(new int[] {0, 1}, 0, 1, 2, 3, 4));
        out.add(candidate(new int[] {2, 3}, 0, 1, 2, 3));
        out.add(candidate(new int[] {4, 5}, 4, 5, 6, 7, 8));
        out.add(candidate(new int[] {6, 7}, 6, 7, 8, 9));
        return out;
    }

    private static CandidateItemset candidate(int[] termIds, int... docIds) {
        BitSet bits = new BitSet();
        for (int docId : docIds) {
            bits.set(docId);
        }
        return new CandidateItemset(termIds, bits);
    }

    private static String fingerprint(List<CandidateItemset> candidates) {
        StringBuilder sb = new StringBuilder();
        for (CandidateItemset candidate : candidates) {
            int[] ids = candidate.getTermIdsUnsafe();
            sb.append(ids.length > 0 ? ids[0] : -1)
                    .append(':')
                    .append(candidate.getSupport())
                    .append('|');
        }
        return sb.toString();
    }
}
