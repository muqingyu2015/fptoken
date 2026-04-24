package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.hints;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-merge hints 相关逻辑的独立组件：
 * 负责 hint 映射、候选去重合并与 hint 权重增强。
 *
 * <p>核心挖掘链（index/miner/picker）无需感知 hint 数据来源，只接收标准候选列表。</p>
 */
public final class PremergeHintCandidateResolver {
    private PremergeHintCandidateResolver() {
    }

    public static List<CandidateItemset> mapHintCandidatesToTermIds(
            List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> hints,
            List<ByteRef> termVocabulary
    ) {
        if (hints == null || hints.isEmpty()) {
            return Collections.emptyList();
        }
        Map<IntArrayKey, CandidateItemset> out = new HashMap<>();
        for (int i = 0; i < hints.size(); i++) {
            ExclusiveFrequentItemsetSelector.PremergeHintCandidate hint = hints.get(i);
            if (hint == null || hint.getTermRefs().isEmpty()) {
                continue;
            }
            int[] termIds = mapHintTermsToTermIds(hint.getTermRefs(), termVocabulary);
            if (termIds.length == 0) {
                continue;
            }
            IntArrayKey key = new IntArrayKey(termIds);
            CandidateItemset old = out.get(key);
            int qualityBoost = Math.max(0, hint.getQualityScore() - 1);
            if (old == null) {
                CandidateItemset base = CandidateItemset.trusted(termIds, new BitSet(), 0);
                out.put(key, qualityBoost > 0 ? base.withPriorityBoost(qualityBoost) : base);
            } else if (qualityBoost > old.getPriorityBoost()) {
                out.put(key, old.withPriorityBoost(qualityBoost - old.getPriorityBoost()));
            }
        }
        return new ArrayList<>(out.values());
    }

    public static List<CandidateItemset> mergeAndDedupCandidates(
            List<CandidateItemset> mined,
            List<CandidateItemset> hinted
    ) {
        if ((mined == null || mined.isEmpty()) && (hinted == null || hinted.isEmpty())) {
            return Collections.emptyList();
        }
        Map<IntArrayKey, CandidateItemset> merged = new HashMap<>();
        if (mined != null) {
            for (int i = 0; i < mined.size(); i++) {
                CandidateItemset c = mined.get(i);
                merged.put(new IntArrayKey(c.getTermIdsUnsafe()), c);
            }
        }
        if (hinted != null) {
            for (int i = 0; i < hinted.size(); i++) {
                CandidateItemset c = hinted.get(i);
                IntArrayKey key = new IntArrayKey(c.getTermIdsUnsafe());
                if (!merged.containsKey(key)) {
                    merged.put(key, c);
                }
            }
        }
        return new ArrayList<>(merged.values());
    }

    public static List<CandidateItemset> applyHintBoost(
            List<CandidateItemset> recomputed,
            List<CandidateItemset> hintCandidates,
            int hintBoostWeight
    ) {
        if (recomputed == null || recomputed.isEmpty() || hintCandidates == null || hintCandidates.isEmpty()
                || hintBoostWeight <= 0) {
            return recomputed == null ? Collections.<CandidateItemset>emptyList() : recomputed;
        }
        Map<IntArrayKey, Integer> hintQualityBoostByKey = new HashMap<>();
        for (int i = 0; i < hintCandidates.size(); i++) {
            CandidateItemset c = hintCandidates.get(i);
            IntArrayKey key = new IntArrayKey(c.getTermIdsUnsafe());
            Integer old = hintQualityBoostByKey.get(key);
            int qualityBoost = Math.max(0, c.getPriorityBoost());
            if (old == null || qualityBoost > old.intValue()) {
                hintQualityBoostByKey.put(key, Integer.valueOf(qualityBoost));
            }
        }
        List<CandidateItemset> out = new ArrayList<>(recomputed.size());
        for (int i = 0; i < recomputed.size(); i++) {
            CandidateItemset c = recomputed.get(i);
            IntArrayKey key = new IntArrayKey(c.getTermIdsUnsafe());
            Integer qualityBoost = hintQualityBoostByKey.get(key);
            if (qualityBoost != null) {
                int qualityMultiplier = 1 + Math.max(0, qualityBoost.intValue());
                int boost = hintBoostWeight * Math.max(1, c.length()) * qualityMultiplier;
                out.add(c.withPriorityBoost(boost));
            } else {
                out.add(c);
            }
        }
        return out;
    }

    private static int[] mapHintTermsToTermIds(List<ByteRef> hintTerms, List<ByteRef> termVocabulary) {
        int[] mapped = new int[hintTerms.size()];
        int used = 0;
        for (int i = 0; i < hintTerms.size(); i++) {
            ByteRef hint = hintTerms.get(i);
            int termId = findTermIdInVocabulary(hint, termVocabulary);
            if (termId >= 0) {
                mapped[used++] = termId;
            }
        }
        if (used == 0) {
            return new int[0];
        }
        int[] termIds = Arrays.copyOf(mapped, used);
        Arrays.sort(termIds);
        int uniqueCount = 1;
        for (int i = 1; i < termIds.length; i++) {
            if (termIds[i] != termIds[i - 1]) {
                termIds[uniqueCount++] = termIds[i];
            }
        }
        return Arrays.copyOf(termIds, uniqueCount);
    }

    private static int findTermIdInVocabulary(ByteRef hint, List<ByteRef> termVocabulary) {
        for (int i = 0; i < termVocabulary.size(); i++) {
            if (equalsRef(hint, termVocabulary.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static boolean equalsRef(ByteRef a, ByteRef b) {
        if (a.getLength() != b.getLength()) {
            return false;
        }
        byte[] as = a.getSourceUnsafe();
        byte[] bs = b.getSourceUnsafe();
        int ao = a.getOffset();
        int bo = b.getOffset();
        for (int i = 0; i < a.getLength(); i++) {
            if (as[ao + i] != bs[bo + i]) {
                return false;
            }
        }
        return true;
    }

    private static final class IntArrayKey {
        private final int[] values;
        private final int hash;

        private IntArrayKey(int[] values) {
            this.values = values;
            this.hash = Arrays.hashCode(values);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IntArrayKey)) {
                return false;
            }
            IntArrayKey that = (IntArrayKey) o;
            return Arrays.equals(values, that.values);
        }
    }
}
