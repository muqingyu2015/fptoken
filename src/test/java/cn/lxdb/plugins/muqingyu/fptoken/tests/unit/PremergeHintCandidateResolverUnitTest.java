package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.hints.PremergeHintCandidateResolver;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class PremergeHintCandidateResolverUnitTest {

    @Test
    void mapHintCandidatesToTermIds_shouldMapAndDedupByVocabulary() {
        List<ByteRef> vocabulary = new ArrayList<>();
        vocabulary.add(ref("a"));
        vocabulary.add(ref("b"));
        vocabulary.add(ref("c"));

        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> hints = new ArrayList<>();
        hints.add(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(list(ref("c"), ref("a"))));
        hints.add(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(list(ref("a"), ref("c"))));
        hints.add(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(list(ref("x"))));

        List<CandidateItemset> mapped = PremergeHintCandidateResolver.mapHintCandidatesToTermIds(hints, vocabulary);

        assertEquals(1, mapped.size());
        int[] termIds = mapped.get(0).getTermIdsUnsafe();
        assertEquals(2, termIds.length);
        assertEquals(0, termIds[0]);
        assertEquals(2, termIds[1]);
    }

    @Test
    void applyHintBoost_shouldOnlyBoostHintMatchedCandidates() {
        CandidateItemset hinted = CandidateItemset.trusted(new int[] {0, 1}, new BitSet(), 3);
        CandidateItemset normal = CandidateItemset.trusted(new int[] {1, 2}, new BitSet(), 3);
        List<CandidateItemset> recomputed = list(hinted, normal);
        List<CandidateItemset> hintCandidates = Collections.singletonList(
                CandidateItemset.trusted(new int[] {0, 1}, new BitSet(), 0)
        );

        List<CandidateItemset> boosted = PremergeHintCandidateResolver.applyHintBoost(recomputed, hintCandidates, 5);

        assertEquals(2, boosted.size());
        assertTrue(boosted.get(0).getPriorityBoost() > 0);
        assertEquals(0, boosted.get(1).getPriorityBoost());
    }

    @Test
    void applyHintBoost_shouldScaleBoostByHintQualityScore() {
        List<ByteRef> vocabulary = list(ref("a"), ref("b"), ref("c"));
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> hints = list(
                new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(list(ref("a"), ref("b")), 1),
                new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(list(ref("b"), ref("c")), 4)
        );
        List<CandidateItemset> hintCandidates = PremergeHintCandidateResolver.mapHintCandidatesToTermIds(hints, vocabulary);

        CandidateItemset ab = CandidateItemset.trusted(new int[] {0, 1}, new BitSet(), 3);
        CandidateItemset bc = CandidateItemset.trusted(new int[] {1, 2}, new BitSet(), 3);
        List<CandidateItemset> boosted = PremergeHintCandidateResolver.applyHintBoost(
                list(ab, bc), hintCandidates, 2);

        assertTrue(boosted.get(1).getPriorityBoost() > boosted.get(0).getPriorityBoost());
        assertEquals(4, boosted.get(0).getPriorityBoost());
        assertEquals(16, boosted.get(1).getPriorityBoost());
    }

    @SafeVarargs
    private static <T> List<T> list(T... items) {
        List<T> out = new ArrayList<>(items.length);
        Collections.addAll(out, items);
        return out;
    }

    private static ByteRef ref(String v) {
        byte[] bytes = v.getBytes();
        return new ByteRef(bytes, 0, bytes.length);
    }
}
