package cn.lxdb.plugins.muqingyu.fptoken.config;

/**
 * 挖掘与后续阶段共用的数值边界：在构造时一次性校验，避免各层重复判参。
 *
 * <p><b>字段含义</b>：
 * <ul>
 *   <li>{@code minSupport}：项集至少出现在多少个文档中（支持度下限）。</li>
 *   <li>{@code minItemsetSize} / {@code maxItemsetSize}：输出项集长度闭区间 [{@code min}, {@code max}]（词个数）。</li>
 *   <li>{@code maxCandidateCount}：挖掘阶段最多保留多少条 {@link cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset}；达到后停止并标记截断。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class SelectorConfig {
    private final int minSupport;
    private final int minItemsetSize;
    private final int maxItemsetSize;
    private final int maxCandidateCount;

    /**
     * @param minSupport 必须 {@code > 0}
     * @param minItemsetSize 必须 {@code > 0}
     * @param maxItemsetSize 必须 {@code >= minItemsetSize}
     * @param maxCandidateCount 必须 {@code > 0}
     * @throws IllegalArgumentException 参数不合法时
     */
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
