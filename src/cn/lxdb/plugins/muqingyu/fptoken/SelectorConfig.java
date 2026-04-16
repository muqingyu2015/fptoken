package cn.lxdb.plugins.muqingyu.fptoken;

/**
 * 作者：muqingyu
 *
 * 选择器参数配置。
 *
 * 参数含义：
 * - minSupport: 最小支持度，控制“多少文档共同出现”才算频繁。
 * - minItemsetSize: 最小项集长度，通常设为 2（过滤掉单词项）。
 * - maxItemsetSize: 最大项集长度，防止组合爆炸。
 * - maxCandidateCount: 候选上限，硬性控制时间和内存风险。
 *
 * 建议：
 * - 线上优先把 maxCandidateCount 设为可控值（先稳时延，再追求更优结果）。
 * - maxItemsetSize 通常 3~6 较常见，过大时候选增长会非常快。
 */
public final class SelectorConfig {
    private final int minSupport;
    private final int minItemsetSize;
    private final int maxItemsetSize;
    private final int maxCandidateCount;

    public SelectorConfig(int minSupport, int minItemsetSize, int maxItemsetSize, int maxCandidateCount) {
        if (minSupport <= 0) {
            throw new IllegalArgumentException("minSupport must be > 0");
        }
        if (minItemsetSize <= 0) {
            throw new IllegalArgumentException("minItemsetSize must be > 0");
        }
        if (maxItemsetSize < minItemsetSize) {
            throw new IllegalArgumentException("maxItemsetSize must be >= minItemsetSize");
        }
        if (maxCandidateCount <= 0) {
            throw new IllegalArgumentException("maxCandidateCount must be > 0");
        }
        this.minSupport = minSupport;
        this.minItemsetSize = minItemsetSize;
        this.maxItemsetSize = maxItemsetSize;
        this.maxCandidateCount = maxCandidateCount;
    }

    public int getMinSupport() {
        return minSupport;
    }

    public int getMinItemsetSize() {
        return minItemsetSize;
    }

    public int getMaxItemsetSize() {
        return maxItemsetSize;
    }

    public int getMaxCandidateCount() {
        return maxCandidateCount;
    }
}
