package cn.lxdb.plugins.muqingyu.fptoken.runner.result;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * FinalIndexData 的构建步骤集合。
 *
 * <p>将“索引派生算法”从数据模型中拆出，减少类嵌套层级与引用跳转；
 * {@link LineFileProcessingResult.FinalIndexData} 仅负责持有数据，不承担构建细节。</p>
 */
final class LineFileIndexBuilders {

    private static final ThreadLocal<BitsetBuildScratch> BITSET_BUILD_SCRATCH =
            new ThreadLocal<BitsetBuildScratch>() {
                @Override
                protected BitsetBuildScratch initialValue() {
                    return new BitsetBuildScratch();
                }
            };

    private LineFileIndexBuilders() {
    }

    static List<LineFileProcessingResult.TermsPostingIndexRef> buildHighFreqMutexGroupTermsToIndex(
            List<SelectedGroup> groups
    ) {
        List<LineFileProcessingResult.TermsPostingIndexRef> out =
                new ArrayList<LineFileProcessingResult.TermsPostingIndexRef>(groups.size());
        for (int i = 0; i < groups.size(); i++) {
            out.add(new LineFileProcessingResult.TermsPostingIndexRef(groups.get(i).getTerms(), i));
        }
        return out;
    }

    static List<LineFileProcessingResult.TermsPostingIndexRef> buildHighFreqSingleTermToIndex(
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        List<LineFileProcessingResult.TermsPostingIndexRef> out =
                new ArrayList<LineFileProcessingResult.TermsPostingIndexRef>(hotTerms.size());
        for (int i = 0; i < hotTerms.size(); i++) {
            out.add(new LineFileProcessingResult.TermsPostingIndexRef(hotTerms.get(i).getTerm(), i));
        }
        return out;
    }

    static List<LineFileProcessingResult.TermsPostingIndexRef> buildLowHitTermToIndexes(
            List<DocTerms> lowHitForwardRows
    ) {
        int estimatedSize = 0;
        for (int i = 0; i < lowHitForwardRows.size(); i++) {
            estimatedSize += lowHitForwardRows.get(i).getTermRefsUnsafe().size();
        }
        List<LineFileProcessingResult.TermsPostingIndexRef> out =
                new ArrayList<LineFileProcessingResult.TermsPostingIndexRef>(estimatedSize);
        for (int rowIndex = 0; rowIndex < lowHitForwardRows.size(); rowIndex++) {
            DocTerms row = lowHitForwardRows.get(rowIndex);
            int docId = row.getDocId();
            for (ByteRef term : row.getTermRefsUnsafe()) {
                out.add(new LineFileProcessingResult.TermsPostingIndexRef(term, docId));
            }
        }
        return out;
    }

    static LineFileProcessingResult.TermBlockSkipBitsetIndex buildTermBlockSkipBitsetIndex(
            List<LineFileProcessingResult.TermsPostingIndexRef> refs,
            int skipHashMinGram,
            int skipHashMaxGram
    ) {
        BitsetBuildScratch scratch = BITSET_BUILD_SCRATCH.get();
        List<ByteRef> logicalTerms = scratch.borrowLogicalTerms();
        List<Integer> termPostingIndexes = scratch.borrowTermPostingIndexes();
        int maxPostingIndex = -1;
        int maxTermLen = 0;

        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            int postingIndex = ref.getPostingIndex();
            if (postingIndex > maxPostingIndex) {
                maxPostingIndex = postingIndex;
            }
            for (ByteRef term : ref.getTermsUnsafe()) {
                logicalTerms.add(term);
                termPostingIndexes.add(postingIndex);
                if (term.getLength() > maxTermLen) {
                    maxTermLen = term.getLength();
                }
            }
        }

        List<LineFileProcessingResult.HashLevelBitsets> hashLevels = scratch.borrowHashLevels();
        for (int gramLength = skipHashMinGram; gramLength <= skipHashMaxGram; gramLength++) {
            if (maxTermLen >= gramLength) {
                hashLevels.add(buildHashLevelBitsets(
                        gramLength, logicalTerms, termPostingIndexes, scratch));
            }
        }
        return new LineFileProcessingResult.TermBlockSkipBitsetIndex(maxPostingIndex, hashLevels);
    }

    static LineFileProcessingResult.OneByteDocidBitsetIndex buildOneByteDocidBitsetIndex(List<DocTerms> loadedRows) {
        BitsetBuildScratch scratch = BITSET_BUILD_SCRATCH.get();
        int maxDocId = -1;
        for (DocTerms row : loadedRows) {
            if (row.getDocId() > maxDocId) {
                maxDocId = row.getDocId();
            }
        }

        List<BitSet> buckets = scratch.borrowBuckets();
        for (DocTerms row : loadedRows) {
            int docId = row.getDocId();
            for (ByteRef bytes : row.getTermRefsUnsafe()) {
                byte[] src = bytes.getSourceUnsafe();
                int start = bytes.getOffset();
                int end = start + bytes.getLength();
                for (int i = start; i < end; i++) {
                    buckets.get(src[i] & 0xFF).set(docId);
                }
            }
        }
        return new LineFileProcessingResult.OneByteDocidBitsetIndex(maxDocId, buckets);
    }

    static void validateSkipHashGramRange(int skipHashMinGram, int skipHashMaxGram) {
        if (skipHashMinGram < 2) {
            throw new IllegalArgumentException("skipHashMinGram must be >= 2");
        }
        if (skipHashMaxGram < skipHashMinGram) {
            throw new IllegalArgumentException("skipHashMaxGram must be >= skipHashMinGram");
        }
    }

    private static LineFileProcessingResult.HashLevelBitsets buildHashLevelBitsets(
            int gramLength,
            List<ByteRef> logicalTerms,
            List<Integer> termPostingIndexes,
            BitsetBuildScratch scratch
    ) {
        List<BitSet> buckets = scratch.borrowBuckets();
        for (int termId = 0; termId < logicalTerms.size(); termId++) {
            ByteRef term = logicalTerms.get(termId);
            if (term.getLength() < gramLength) {
                continue;
            }
            int postingIndex = termPostingIndexes.get(termId);
            for (int start = 0; start <= term.getLength() - gramLength; start++) {
                int bucket = hashWindowToBucket(term, start, gramLength);
                buckets.get(bucket).set(postingIndex);
            }
        }
        return new LineFileProcessingResult.HashLevelBitsets(gramLength, buckets);
    }

    private static int hashWindowToBucket(ByteRef arr, int start, int len) {
        int h = 1;
        byte[] source = arr.getSourceUnsafe();
        int base = arr.getOffset() + start;
        for (int i = 0; i < len; i++) {
            h = 31 * h + (source[base + i] & 0xFF);
        }
        return h & 0xFF;
    }

    private static final class BitsetBuildScratch {
        private final List<BitSet> buckets = new ArrayList<BitSet>(256);
        private final List<ByteRef> logicalTerms = new ArrayList<ByteRef>();
        private final List<Integer> termPostingIndexes = new ArrayList<Integer>();
        private final List<LineFileProcessingResult.HashLevelBitsets> hashLevels =
                new ArrayList<LineFileProcessingResult.HashLevelBitsets>();

        private List<BitSet> borrowBuckets() {
            while (buckets.size() < 256) {
                buckets.add(new BitSet());
            }
            for (int i = 0; i < 256; i++) {
                buckets.get(i).clear();
            }
            return buckets;
        }

        private List<ByteRef> borrowLogicalTerms() {
            logicalTerms.clear();
            return logicalTerms;
        }

        private List<Integer> borrowTermPostingIndexes() {
            termPostingIndexes.clear();
            return termPostingIndexes;
        }

        private List<LineFileProcessingResult.HashLevelBitsets> borrowHashLevels() {
            hashLevels.clear();
            return hashLevels;
        }
    }
}
