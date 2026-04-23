package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.OpenHashTable;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OpenHashTableTest {

    @Test
    void sameContentDifferentArrays_shouldReturnSameTermId() {
        OpenHashTable table = new OpenHashTable(8);
        byte[] a1 = new byte[] {0x11, 0x22};
        byte[] a2 = new byte[] {0x11, 0x22};

        int id1 = table.getOrPut(a1, ByteArrayUtils.hash(a1), true);
        int id2 = table.getOrPut(a2, ByteArrayUtils.hash(a2), true);

        assertEquals(id1, id2);
        assertEquals(1, table.size());
    }

    @Test
    void getKeyBytes_roundTripStillCorrectAfterRehash() {
        OpenHashTable table = new OpenHashTable(4);
        List<byte[]> inserted = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            byte[] key = ("term-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            inserted.add(key);
            ids.add(Integer.valueOf(table.getOrPut(key, ByteArrayUtils.hash(key), true)));
        }

        for (int i = 0; i < inserted.size(); i++) {
            int termId = ids.get(i).intValue();
            assertArrayEquals(inserted.get(i), table.getKeyBytes(termId));
        }
        assertTrue(table.size() >= inserted.size());
    }

    @Test
    void get_missingValue_shouldReturnMinusOne() {
        OpenHashTable table = new OpenHashTable();
        byte[] key = new byte[] {0x55};
        assertEquals(-1, table.get(key, ByteArrayUtils.hash(key)));
    }

    @Test
    void getKeyBytes_outOfRange_shouldThrow() {
        OpenHashTable table = new OpenHashTable();
        assertThrows(IllegalArgumentException.class, () -> table.getKeyBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> table.getKeyBytes(0));
    }

    @Test
    void byteRefLookup_shouldMatchBySliceContentWithoutTempArray() {
        OpenHashTable table = new OpenHashTable(8);
        byte[] source = new byte[] {9, 1, 2, 3, 1, 2, 3, 8};
        ByteRef ref1 = new ByteRef(source, 1, 3); // [1,2,3]
        ByteRef ref2 = new ByteRef(source, 4, 3); // [1,2,3]

        int id1 = table.getOrPut(ref1, ByteArrayUtils.hash(source, 1, 3), true);
        int id2 = table.getOrPut(ref2, ByteArrayUtils.hash(source, 4, 3), true);

        assertEquals(id1, id2);
        assertEquals(1, table.size());
        assertArrayEquals(new byte[] {1, 2, 3}, table.getKeyBytes(id1));
    }
}
