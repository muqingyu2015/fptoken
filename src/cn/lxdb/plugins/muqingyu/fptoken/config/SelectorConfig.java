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

    /** 可读性别名：语义同 {@link #getMinSupport()}。 */
    public int getMinimumRequiredSupport() {
        return minSupport;
    }

    public int getMinItemsetSize() {
        return minItemsetSize;
    }

    /** 可读性别名：语义同 {@link #getMinItemsetSize()}。 */
    public int getMinimumPatternLength() {
        return minItemsetSize;
    }

    public int getMaxItemsetSize() {
        return maxItemsetSize;
    }

    /** 可读性别名：语义同 {@link #getMaxItemsetSize()}。 */
    public int getMaximumPatternLength() {
        return maxItemsetSize;
    }

    public int getMaxCandidateCount() {
        return maxCandidateCount;
    }

    /** 可读性别名：语义同 {@link #getMaxCandidateCount()}。 */
    public int getMaximumIntermediateResults() {
        return maxCandidateCount;
    }

    /** Builder 入口：适合按名称设置配置字段，提高调用可读性。 */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 兼容旧构造器的命名式构建器。
     * <p>默认值与构造校验一致：所有字段需被赋予合法值后才能 {@link #build()}。
     */
    public static final class Builder {
        private int minimumRequiredSupport;
        private int minimumPatternLength;
        private int maximumPatternLength;
        private int maximumIntermediateResults;

        private Builder() {
        }

        public Builder minimumRequiredSupport(int value) {
            this.minimumRequiredSupport = value;
            return this;
        }

        public Builder minimumPatternLength(int value) {
            this.minimumPatternLength = value;
            return this;
        }

        public Builder maximumPatternLength(int value) {
            this.maximumPatternLength = value;
            return this;
        }

        public Builder maximumIntermediateResults(int value) {
            this.maximumIntermediateResults = value;
            return this;
        }

        public SelectorConfig build() {
            return new SelectorConfig(
                    minimumRequiredSupport,
                    minimumPatternLength,
                    maximumPatternLength,
                    maximumIntermediateResults
            );
        }
    }
}
