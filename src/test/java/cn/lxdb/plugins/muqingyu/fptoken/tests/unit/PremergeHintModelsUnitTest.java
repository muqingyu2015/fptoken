package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PremergeHintModelsUnitTest {

    @Test
    void premergeHint_shouldDefensivelyCopyAndExposeReadOnlyLists() {
        List<ByteRef> terms = new ArrayList<ByteRef>();
        terms.add(ref("A"));

        ExclusiveFpRowsProcessingApi.PremergeHint hint =
                new ExclusiveFpRowsProcessingApi.PremergeHint(terms);

        terms.clear();
        assertEquals(1, hint.getTermRefs().size());
        assertThrows(UnsupportedOperationException.class, () -> hint.getTermRefs().add(ref("B")));
    }

    @Test
    void premergeHintCandidate_shouldDefensivelyCopyAndExposeReadOnlyLists() {
        List<ByteRef> terms = new ArrayList<ByteRef>();
        terms.add(ref("A"));

        ExclusiveFrequentItemsetSelector.PremergeHintCandidate hint =
                new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(terms);

        terms.clear();
        assertEquals(1, hint.getTermRefs().size());
        assertThrows(UnsupportedOperationException.class, () -> hint.getTermRefs().add(ref("C")));
    }

    @Test
    void hintValidationMode_shouldContainStrictAndFilterOnly() {
        assertEquals(ExclusiveFpRowsProcessingApi.HintValidationMode.STRICT,
                ExclusiveFpRowsProcessingApi.HintValidationMode.valueOf("STRICT"));
        assertEquals(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY,
                ExclusiveFpRowsProcessingApi.HintValidationMode.valueOf("FILTER_ONLY"));
    }

    @Test
    void intArrayKey_shouldCompareByContentAndUseDefensiveCopy() throws Exception {
        Class<?> keyClass = Class.forName(
                "cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector$IntArrayKey");
        Constructor<?> ctor = keyClass.getDeclaredConstructor(int[].class);
        ctor.setAccessible(true);

        int[] source = new int[] {1, 2, 3};
        Object key1 = ctor.newInstance((Object) source);
        Object key2 = ctor.newInstance((Object) new int[] {1, 2, 3});
        Object key3 = ctor.newInstance((Object) new int[] {1, 3, 2});

        source[0] = 99;
        Object key4 = ctor.newInstance((Object) new int[] {1, 2, 3});

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
        assertEquals(key1, key4);
        assertNotEquals(key1, key3);
        assertTrue(key1.equals(key1));
    }

    private static ByteRef ref(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ByteRef(bytes, 0, bytes.length);
    }
}
