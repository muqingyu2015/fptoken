package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FptokenLineFileRunnerMainDerivedDataTest {

    @Test
    void buildDerivedData_shouldKeepOnlyHotTermsAboveThreshold_andRemoveSelectedTerms() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("A"), bytes("B"), bytes("C"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("A"), bytes("B"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("A"), bytes("D"))));

        SelectedGroup selected = new SelectedGroup(
                Collections.singletonList(bytes("B")),
                Arrays.asList(0, 1),
                2,
                10
        );
        ExclusiveSelectionResult result = new ExclusiveSelectionResult(
                Collections.singletonList(selected), 0, 0, 0, 0, false);

        LineFileProcessingResult.DerivedData derived =
                ExclusiveFpRowsProcessingApi.buildDerivedData(rows, result, 1);

        // 原始 rows 必须保留（不应被 cut_res 副作用修改）
        assertTrue(containsTerm(rows.get(0), "B"));
        assertTrue(containsTerm(rows.get(1), "B"));

        List<DocTerms> cutRes = derived.getCutRes();
        assertEquals(3, cutRes.size());
        assertFalse(containsTerm(cutRes.get(0), "B"));
        assertFalse(containsTerm(cutRes.get(1), "B"));
        assertTrue(containsTerm(cutRes.get(0), "A"));
        assertTrue(containsTerm(cutRes.get(0), "C"));
        assertTrue(containsTerm(cutRes.get(2), "D"));

        Map<String, LineFileProcessingResult.HotTermDocList> hotTerms = toHexMap(derived.getHotTerms());
        assertEquals(1, hotTerms.size());
        LineFileProcessingResult.HotTermDocList hotA = hotTerms.get(ByteArrayUtils.toHex(bytes("A")));
        assertTrue(hotA != null);
        assertEquals(Arrays.asList(0, 1, 2), hotA.getDocIds());
    }

    private static Map<String, LineFileProcessingResult.HotTermDocList> toHexMap(
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        Map<String, LineFileProcessingResult.HotTermDocList> out =
                new LinkedHashMap<String, LineFileProcessingResult.HotTermDocList>();
        for (LineFileProcessingResult.HotTermDocList item : hotTerms) {
            out.put(ByteArrayUtils.toHex(item.getTerm()), item);
        }
        return out;
    }

    private static boolean containsTerm(DocTerms docTerms, String term) {
        byte[] target = bytes(term);
        for (byte[] t : docTerms.getTerms()) {
            if (Arrays.equals(t, target)) {
                return true;
            }
        }
        return false;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
