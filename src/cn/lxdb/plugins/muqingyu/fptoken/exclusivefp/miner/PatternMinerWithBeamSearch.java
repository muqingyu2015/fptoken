package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import java.util.BitSet;
import java.util.List;

/**
 * 对 {@link BeamFrequentItemsetMiner} 的可读性命名包装。
 *
 * <p>适用于希望在业务代码中直接表达“使用 Beam Search 进行模式挖掘”的场景。
 */
public final class PatternMinerWithBeamSearch {
    private final BeamFrequentItemsetMiner delegate;

    public PatternMinerWithBeamSearch() {
        this.delegate = new BeamFrequentItemsetMiner();
    }

    public PatternMinerWithBeamSearch(BeamFrequentItemsetMiner.ScoreFunction scoreFunction) {
        this.delegate = new BeamFrequentItemsetMiner(scoreFunction);
    }

    /** 可读性入口：查找频繁模式并返回统计。 */
    public FrequentItemsetMiningResult findFrequentPatternsWithStatistics(
            List<BitSet> termDocumentBitsets,
            SelectorConfig config,
            int maxVocabularySize,
            int maxChildrenPerNode,
            int maxCandidatesToRetainPerLevel
    ) {
        return delegate.mineWithStats(
                termDocumentBitsets,
                config,
                maxVocabularySize,
                maxChildrenPerNode,
                maxCandidatesToRetainPerLevel
        );
    }

    /** 带早停参数的可读性入口。 */
    public FrequentItemsetMiningResult findFrequentPatternsWithStatistics(
            List<BitSet> termDocumentBitsets,
            SelectorConfig config,
            int maxVocabularySize,
            int maxChildrenPerNode,
            int maxCandidatesToRetainPerLevel,
            int maxIdleLevels,
            long maxRuntimeMillis
    ) {
        return delegate.mineWithStats(
                termDocumentBitsets,
                config,
                maxVocabularySize,
                maxChildrenPerNode,
                maxCandidatesToRetainPerLevel,
                maxIdleLevels,
                maxRuntimeMillis
        );
    }
}
