package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.protocol;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 模块间通信协议（Data Contracts）。
 *
 * <p>目标：显式定义 index / miner / picker 之间允许交换的数据形态，避免模块通过隐式约定耦合。
 * 当前协议固定为：
 * <ul>
 *   <li>index -> miner: 仅传 {@link TidsetMiningInput}（termId 对齐的 tidset 列表）</li>
 *   <li>miner -> picker: 仅传 {@link PickerInput}（候选项集 + 词典大小）</li>
 * </ul>
 */
public final class ModuleDataContracts {
    private ModuleDataContracts() {
    }

    /**
     * index 输出给 miner 的标准输入：
     * termId 维度的文档位图列表，列表下标即 termId。
     */
    public static final class TidsetMiningInput {
        private final List<BitSet> tidsetsByTermId;
        private final int vocabularySize;

        public TidsetMiningInput(List<BitSet> tidsetsByTermId) {
            List<BitSet> source = Objects.requireNonNull(tidsetsByTermId, "tidsetsByTermId");
            this.vocabularySize = source.size();
            for (int i = 0; i < source.size(); i++) {
                if (source.get(i) == null) {
                    throw new IllegalArgumentException("tidsetsByTermId[" + i + "] must not be null");
                }
            }
            this.tidsetsByTermId = Collections.unmodifiableList(source);
        }

        /** 只读 tidset 视图（termId 下标对齐）。 */
        public List<BitSet> getTidsetsByTermId() {
            return tidsetsByTermId;
        }

        /** 读取某个 termId 对应 tidset。 */
        public BitSet tidsetAt(int termId) {
            return tidsetsByTermId.get(termId);
        }

        public int getVocabularySize() {
            return vocabularySize;
        }
    }

    /**
     * miner 输出给 picker 的标准输入：
     * 候选项集与词典大小（termId 上界语义）。
     */
    public static final class PickerInput {
        private final List<CandidateItemset> candidates;
        private final int dictionarySize;

        public PickerInput(List<CandidateItemset> candidates, int dictionarySize) {
            this.candidates = Objects.requireNonNull(candidates, "candidates");
            if (dictionarySize <= 0) {
                throw new IllegalArgumentException("dictionarySize must be > 0");
            }
            this.dictionarySize = dictionarySize;
            for (int i = 0; i < candidates.size(); i++) {
                if (candidates.get(i) == null) {
                    throw new IllegalArgumentException("candidates[" + i + "] must not be null");
                }
            }
        }

        public List<CandidateItemset> getCandidates() {
            return candidates;
        }

        public int getDictionarySize() {
            return dictionarySize;
        }
    }
}
