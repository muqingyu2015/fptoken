package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 对外行记录处理 API：仅基于 rows 进行互斥频繁项集处理与派生数据生成。
 */
public final class ExclusiveFpRowsProcessingApi {

    private ExclusiveFpRowsProcessingApi() {
    }

    /**
     * 行级处理总入口（对外 API）。
     *
     * <p>该方法接收上游（如 LXDB）准备好的 {@code List<DocTerms>}，完成以下流程：</p>
     * <ol>
     *   <li>对输入 rows 做防御性复制，避免修改调用方对象。</li>
     *   <li>执行互斥高频项挖掘，得到 {@link ExclusiveSelectionResult}。</li>
     *   <li>基于同一批 rows 构建最终三层索引输入：
     *     <ul>
     *       <li>{@code highFreqMutexGroupPostings}：高频互斥组合倒排（一个 group 内多个 term 共享 docId 列表）</li>
     *       <li>{@code highFreqSingleTermPostings}：高频单 term 倒排</li>
     *       <li>{@code lowHitForwardRows}：排除高频层后剩余的低命中正排行（供 skip index 使用）</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p><b>建议使用方式</b>：</p>
     * <pre>{@code
     * LineFileProcessingResult result =
     *         ExclusiveFpRowsProcessingApi.processRows(rows, 80, 2, 16);
     *
     * LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();
     * List<SelectedGroup> groupPostings = finalData.getHighFreqMutexGroupPostings();
     * List<LineFileProcessingResult.HotTermDocList> termPostings = finalData.getHighFreqSingleTermPostings();
     * List<DocTerms> lowHitRows = finalData.getLowHitForwardRows();
     * }</pre>
     *
     * <p><b>参数说明</b>：</p>
     * <ul>
     *   <li>{@code rows}：输入文档行；每个 {@link DocTerms} 代表一条记录（docId + terms）。</li>
     *   <li>{@code minSupport}：最小支持度；越大结果越“保守”，候选规模更小。</li>
     *   <li>{@code minItemsetSize}：最小项集长度；通常建议 {@code >= 2}。</li>
     *   <li>{@code hotTermThresholdExclusive}：高频单 term 阈值（严格 {@code count > threshold} 才进入 highFreqSingleTermPostings）。</li>
     * </ul>
     *
     * <p><b>返回值说明</b>：</p>
     * <ul>
     *   <li>{@link LineFileProcessingResult#getFinalIndexData()} 是业务主出口（推荐优先使用）。</li>
     *   <li>{@link LineFileProcessingResult#getSelectionResult()} 提供挖掘统计与兼容访问。</li>
     *   <li>{@link LineFileProcessingResult#getLoadedRows()} 为处理时使用的输入快照。</li>
     * </ul>
     *
     * <p><b>注意事项</b>：</p>
     * <ul>
     *   <li>方法内部会设置采样参数（代码配置），无需依赖外部 properties 文件。</li>
     *   <li>若 rows 为空，会返回空结果结构；调用方可直接按空集合处理。</li>
     *   <li>若希望完全禁用采样，请在外层先调用 {@link ExclusiveFrequentItemsetSelector#setSamplingEnabled(boolean)} 统一配置。</li>
     * </ul>
     */
    public static LineFileProcessingResult processRows(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive
    ) {
        // 代码内配置抽样参数（不依赖 properties 文件）
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
        ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);

        // 处理层不要修改调用方传入的 rows，这里先做一份防御性拷贝。
        List<DocTerms> loadedRows = copyRows(rows);
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                loadedRows, minSupport, minItemsetSize, 6, 200000);
        LineFileProcessingResult.DerivedData derived =
                buildDerivedData(loadedRows, result, hotTermThresholdExclusive);
        // 返回结构中会聚合出最终三元结果（新命名）：
        // highFreqMutexGroupPostings + highFreqSingleTermPostings + lowHitForwardRows。
        return new LineFileProcessingResult(loadedRows, result, derived);
    }

    public static LineFileProcessingResult.DerivedData buildDerivedData(
            List<DocTerms> loadedRows,
            ExclusiveSelectionResult result,
            int hotTermThresholdExclusive
    ) {
        // cutRes 基于输入行做浅结构复制，后续只移除词项，不改变 docId。
        List<DocTerms> cutRes = copyRows(loadedRows);
        // hotTerms 在“原始 cutRes”上统计，语义是“先统计，再剔除 selected 词项”。
        List<LineFileProcessingResult.HotTermDocList> hotTerms = buildHotTerms(cutRes, hotTermThresholdExclusive);
        // selectedTerms 代表本轮已经被最终挑中的互斥项，必须从两个派生视图同步去掉。
        Set<ByteArrayKey> selectedTerms = collectSelectedTerms(result);
        List<DocTerms> filteredCutRes = removeSelectedTermsFromCutRes(cutRes, selectedTerms);
        List<LineFileProcessingResult.HotTermDocList> filteredHotTerms =
                removeSelectedTermsFromHotTerms(hotTerms, selectedTerms);
        return new LineFileProcessingResult.DerivedData(
                hotTermThresholdExclusive, filteredCutRes, filteredHotTerms);
    }

    private static List<DocTerms> copyRows(List<DocTerms> rows) {
        List<DocTerms> out = new ArrayList<DocTerms>(rows.size());
        for (DocTerms row : rows) {
            out.add(new DocTerms(row.getDocId(), row.getTermsUnsafe()));
        }
        return out;
    }

    private static List<LineFileProcessingResult.HotTermDocList> buildHotTerms(
            List<DocTerms> rows,
            int hotTermThresholdExclusive
    ) {
        // LinkedHashSet 保证 docId 去重且保持首次出现顺序，便于稳定回放与测试断言。
        Map<ByteArrayKey, LinkedHashSet<Integer>> termToDocs = new LinkedHashMap<ByteArrayKey, LinkedHashSet<Integer>>();
        for (DocTerms row : rows) {
            int docId = row.getDocId();
            for (byte[] term : row.getTermsUnsafe()) {
                ByteArrayKey key = new ByteArrayKey(term);
                LinkedHashSet<Integer> docs = termToDocs.get(key);
                if (docs == null) {
                    docs = new LinkedHashSet<Integer>();
                    termToDocs.put(key, docs);
                }
                docs.add(docId);
            }
        }

        List<LineFileProcessingResult.HotTermDocList> out =
                new ArrayList<LineFileProcessingResult.HotTermDocList>();
        for (Map.Entry<ByteArrayKey, LinkedHashSet<Integer>> entry : termToDocs.entrySet()) {
            List<Integer> docIds = new ArrayList<Integer>(entry.getValue());
            // 阈值语义是严格大于（count > threshold）。
            if (docIds.size() > hotTermThresholdExclusive) {
                out.add(new LineFileProcessingResult.HotTermDocList(entry.getKey().bytes(), docIds));
            }
        }
        // 展示层排序：先按出现文档数降序，再按字节序升序保证同 count 稳定顺序。
        Collections.sort(out, new Comparator<LineFileProcessingResult.HotTermDocList>() {
            @Override
            public int compare(
                    LineFileProcessingResult.HotTermDocList a,
                    LineFileProcessingResult.HotTermDocList b
            ) {
                int c = b.getCount() - a.getCount();
                if (c != 0) {
                    return c;
                }
                return ByteArrayUtils.compareUnsigned(a.getTerm(), b.getTerm());
            }
        });
        return out;
    }

    private static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
        Set<ByteArrayKey> out = new LinkedHashSet<ByteArrayKey>();
        for (SelectedGroup g : result.getGroups()) {
            for (byte[] term : g.getTerms()) {
                out.add(new ByteArrayKey(term));
            }
        }
        return out;
    }

    private static List<DocTerms> removeSelectedTermsFromCutRes(
            List<DocTerms> cutRes,
            Set<ByteArrayKey> selectedTerms
    ) {
        // 空集走直返路径，避免不必要分配。
        if (selectedTerms.isEmpty()) {
            return cutRes;
        }
        List<DocTerms> out = new ArrayList<DocTerms>(cutRes.size());
        for (DocTerms row : cutRes) {
            List<byte[]> filtered = new ArrayList<byte[]>();
            for (byte[] term : row.getTermsUnsafe()) {
                if (!selectedTerms.contains(new ByteArrayKey(term))) {
                    filtered.add(term);
                }
            }
            out.add(new DocTerms(row.getDocId(), filtered));
        }
        return out;
    }

    private static List<LineFileProcessingResult.HotTermDocList> removeSelectedTermsFromHotTerms(
            List<LineFileProcessingResult.HotTermDocList> hotTerms,
            Set<ByteArrayKey> selectedTerms
    ) {
        // 空集走直返路径，避免不必要分配。
        if (selectedTerms.isEmpty()) {
            return hotTerms;
        }
        List<LineFileProcessingResult.HotTermDocList> out =
                new ArrayList<LineFileProcessingResult.HotTermDocList>(hotTerms.size());
        for (LineFileProcessingResult.HotTermDocList item : hotTerms) {
            if (!selectedTerms.contains(new ByteArrayKey(item.getTerm()))) {
                out.add(item);
            }
        }
        return out;
    }
}
