package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CompressionMetricsUtil {
    private static final int DEFAULT_CODE_INDEX_BYTES = 4;

    private CompressionMetricsUtil() {
    }

    static CompressionMetrics calculateCoverageAndPotentialSavings(
            List<CandidateItemset> selectedItemsets,
            TermTidsetIndex index,
            List<DocTerms> originalDocs
    ) {
        if (selectedItemsets == null) {
            throw new IllegalArgumentException("selectedItemsets must not be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("index must not be null");
        }
        if (originalDocs == null) {
            throw new IllegalArgumentException("originalDocs must not be null");
        }

        List<byte[]> idToTerm = index.getIdToTermUnsafe();
        int[] termLengths = new int[idToTerm.size()];
        Map<ByteArrayKey, Integer> termIdByBytes = new HashMap<>(idToTerm.size() * 2 + 1);
        for (int termId = 0; termId < idToTerm.size(); termId++) {
            byte[] term = idToTerm.get(termId);
            termLengths[termId] = term.length;
            termIdByBytes.put(new ByteArrayKey(term), Integer.valueOf(termId));
        }

        List<CandidateItemset> sortedItemsets = new ArrayList<>(selectedItemsets);
        sortedItemsets.sort(Comparator.comparingInt(CandidateItemset::length).reversed()
                .thenComparing(Comparator.comparingInt(CandidateItemset::getSupport).reversed())
                .thenComparing(Comparator.comparingInt(CandidateItemset::getEstimatedSaving).reversed()));

        long dictionaryBytes = 0L;
        int maxItemsetLength = 0;
        int totalItemsetLength = 0;
        for (CandidateItemset itemset : sortedItemsets) {
            if (itemset == null) {
                continue;
            }
            int[] termIds = itemset.getTermIdsUnsafe();
            maxItemsetLength = Math.max(maxItemsetLength, termIds.length);
            totalItemsetLength += termIds.length;
            for (int termId : termIds) {
                if (termId >= 0 && termId < termLengths.length) {
                    dictionaryBytes += termLengths[termId];
                }
            }
        }

        long originalBytes = 0L;
        long coveredBytes = 0L;
        long encodedBytes = 0L;
        long totalTermUnits = 0L;
        long coveredTermUnits = 0L;
        long matchedItemsetOccurrences = 0L;

        for (DocTerms doc : originalDocs) {
            if (doc == null) {
                continue;
            }
            List<byte[]> terms = doc.getTermsUnsafe();
            totalTermUnits += terms.size();
            for (byte[] term : terms) {
                if (term != null) {
                    originalBytes += term.length;
                }
            }

            BitSet termIdsInDoc = new BitSet(termLengths.length);
            for (byte[] term : terms) {
                if (term == null || term.length == 0) {
                    continue;
                }
                Integer termId = termIdByBytes.get(new ByteArrayKey(term));
                if (termId != null) {
                    termIdsInDoc.set(termId.intValue());
                }
            }

            // 同一文档中避免项集之间重复覆盖同一 termId，近似模拟“互斥编码”。
            BitSet usedInDoc = new BitSet(termLengths.length);
            for (CandidateItemset itemset : sortedItemsets) {
                if (itemset == null) {
                    continue;
                }
                int[] termIds = itemset.getTermIdsUnsafe();
                if (termIds.length == 0) {
                    continue;
                }
                boolean matched = true;
                for (int termId : termIds) {
                    if (termId < 0 || termId >= termLengths.length || !termIdsInDoc.get(termId) || usedInDoc.get(termId)) {
                        matched = false;
                        break;
                    }
                }
                if (!matched) {
                    continue;
                }
                matchedItemsetOccurrences++;
                encodedBytes += DEFAULT_CODE_INDEX_BYTES;
                for (int termId : termIds) {
                    usedInDoc.set(termId);
                    coveredBytes += termLengths[termId];
                    coveredTermUnits++;
                }
            }
        }

        long compressedBytes = Math.max(0L, (originalBytes - coveredBytes) + encodedBytes + dictionaryBytes);
        double coverageRatio = originalBytes <= 0L ? 0d : (double) coveredBytes / (double) originalBytes;
        double compressionRatio = originalBytes <= 0L
                ? 0d
                : (double) (originalBytes - compressedBytes) / (double) originalBytes;
        double avgItemsetLength = sortedItemsets.isEmpty() ? 0d : (double) totalItemsetLength / (double) sortedItemsets.size();
        double coveredTermRatio = totalTermUnits <= 0L ? 0d : (double) coveredTermUnits / (double) totalTermUnits;

        return new CompressionMetrics(
                originalBytes,
                dictionaryBytes,
                encodedBytes,
                compressedBytes,
                coverageRatio,
                compressionRatio,
                coveredTermRatio,
                sortedItemsets.size(),
                avgItemsetLength,
                maxItemsetLength,
                matchedItemsetOccurrences
        );
    }

    static CompressionMetrics calculateCoverageAndPotentialSavingsFromTermIds(
            Set<Integer> selectedTermIds,
            TermTidsetIndex index,
            List<DocTerms> originalDocs
    ) {
        List<CandidateItemset> singletonCandidates = new ArrayList<>();
        for (Integer termId : selectedTermIds) {
            if (termId != null) {
                singletonCandidates.add(new CandidateItemset(new int[] {termId.intValue()}, new BitSet(), 0));
            }
        }
        return calculateCoverageAndPotentialSavings(singletonCandidates, index, originalDocs);
    }

    static final class CompressionMetrics {
        private final long originalBytes;
        private final long dictionaryBytes;
        private final long encodedBytes;
        private final long compressedBytes;
        private final double coverageRatio;
        private final double compressionRatio;
        private final double coveredTermRatio;
        private final int selectedItemsetCount;
        private final double averageItemsetLength;
        private final int maxItemsetLength;
        private final long matchedItemsetOccurrences;

        private CompressionMetrics(
                long originalBytes,
                long dictionaryBytes,
                long encodedBytes,
                long compressedBytes,
                double coverageRatio,
                double compressionRatio,
                double coveredTermRatio,
                int selectedItemsetCount,
                double averageItemsetLength,
                int maxItemsetLength,
                long matchedItemsetOccurrences
        ) {
            this.originalBytes = originalBytes;
            this.dictionaryBytes = dictionaryBytes;
            this.encodedBytes = encodedBytes;
            this.compressedBytes = compressedBytes;
            this.coverageRatio = coverageRatio;
            this.compressionRatio = compressionRatio;
            this.coveredTermRatio = coveredTermRatio;
            this.selectedItemsetCount = selectedItemsetCount;
            this.averageItemsetLength = averageItemsetLength;
            this.maxItemsetLength = maxItemsetLength;
            this.matchedItemsetOccurrences = matchedItemsetOccurrences;
        }

        long getOriginalBytes() {
            return originalBytes;
        }

        long getDictionaryBytes() {
            return dictionaryBytes;
        }

        long getEncodedBytes() {
            return encodedBytes;
        }

        long getCompressedBytes() {
            return compressedBytes;
        }

        double getCoverageRatio() {
            return coverageRatio;
        }

        double getCompressionRatio() {
            return compressionRatio;
        }

        double getCoveredTermRatio() {
            return coveredTermRatio;
        }

        int getSelectedItemsetCount() {
            return selectedItemsetCount;
        }

        double getAverageItemsetLength() {
            return averageItemsetLength;
        }

        int getMaxItemsetLength() {
            return maxItemsetLength;
        }

        long getMatchedItemsetOccurrences() {
            return matchedItemsetOccurrences;
        }
    }
}
