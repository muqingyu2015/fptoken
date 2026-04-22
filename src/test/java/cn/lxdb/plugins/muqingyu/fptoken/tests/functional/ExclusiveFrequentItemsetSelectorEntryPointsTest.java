package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 闂ㄩ潰澶氬叆鍙ｄ竴鑷存€э細鍒楄〃鐗堜笌甯︾粺璁＄増搴旇繑鍥炵浉鍚岃瘝缁勫垪琛ㄣ€?*/
class ExclusiveFrequentItemsetSelectorEntryPointsTest {

    @Test
    void selectList_overloads_matchWithStatsGroups() {
        byte[] common = ByteArrayTestSupport.hex("CAFE");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, ByteArrayTestSupport.hex("01")));
        }
        int minSup = 10;
        int minLen = 2;
        int maxLen = 6;
        int maxCand = 10_000;

        List<SelectedGroup> a = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                rows, minSup, minLen, maxLen, maxCand);
        ExclusiveSelectionResult b = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSup, minLen, maxLen, maxCand);
        assertEquals(a.size(), b.getGroups().size());
        assertEquals(ByteArrayTestSupport.groupsFingerprint(a), ByteArrayTestSupport.groupsFingerprint(b.getGroups()));
    }

    @Test
    void selectList_defaultSizeParams_matchesWithStatsDefaults() {
        byte[] t = {1, 2, 3};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            rows.add(ByteArrayTestSupport.doc(i, t));
        }
        List<SelectedGroup> a = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 5, 1);
        List<SelectedGroup> b = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 1)
                .getGroups();
        assertEquals(ByteArrayTestSupport.groupsFingerprint(a), ByteArrayTestSupport.groupsFingerprint(b));
    }

    @Test
    void selectedGroupTermsAreCopies_mutatingDoesNotChangeSecondRunFingerprint() {
        byte[] common = ByteArrayTestSupport.hex("BEEF");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, ByteArrayTestSupport.hex("02")));
        }
        ExclusiveSelectionResult r1 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 2, 6, 20_000);
        String fp = ByteArrayTestSupport.groupsFingerprint(r1.getGroups());
        if (!r1.getGroups().isEmpty() && !r1.getGroups().get(0).getTerms().isEmpty()) {
            byte[] termCopy = r1.getGroups().get(0).getTerms().get(0);
            termCopy[0] = (byte) (termCopy[0] ^ 0x7F);
        }
        ExclusiveSelectionResult r2 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 2, 6, 20_000);
        assertEquals(fp, ByteArrayTestSupport.groupsFingerprint(r2.getGroups()));
        assertFalse(r1.getGroups().isEmpty(), "test needs non-empty groups");
    }

    @Test
    void readableAliasApis_matchLegacyEntryPoints() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] common = ByteArrayTestSupport.hex("C0FFEE");
        for (int i = 0; i < 35; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, ByteArrayTestSupport.hex("11")));
        }

        List<SelectedGroup> legacy = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 10, 2, 6, 10_000);
        List<SelectedGroup> alias = ExclusiveFrequentItemsetSelector.findMutuallyExclusivePatterns(rows, 10, 2, 6, 10_000);
        assertEquals(ByteArrayTestSupport.groupsFingerprint(legacy), ByteArrayTestSupport.groupsFingerprint(alias));

        ExclusiveSelectionResult legacyStats =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 10_000);
        ExclusiveSelectionResult aliasStats =
                ExclusiveFrequentItemsetSelector.findMutuallyExclusivePatternsWithStats(rows, 10, 2, 6, 10_000);
        assertEquals(ByteArrayTestSupport.groupsFingerprint(legacyStats.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(aliasStats.getGroups()));
        assertEquals(legacyStats.getCandidateCount(), aliasStats.getCandidateCount());
    }
}

