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
