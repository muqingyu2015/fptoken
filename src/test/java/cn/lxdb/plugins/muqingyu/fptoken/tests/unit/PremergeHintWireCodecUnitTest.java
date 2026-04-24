package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.api.PremergeHintWireCodec;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PremergeHintWireCodecUnitTest {

    @Test
    void encodeDecode_roundtrip_shouldPreserveTerms() {
        List<ExclusiveFpRowsProcessingApi.PremergeHint> hints = new ArrayList<>();
        hints.add(new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B"))));
        hints.add(new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("S"))));

        String wire = PremergeHintWireCodec.encodeV1(hints);
        List<ExclusiveFpRowsProcessingApi.PremergeHint> decoded = PremergeHintWireCodec.decodeLenient(wire);

        assertEquals(2, decoded.size());
        assertHintTerms(decoded.get(0), "A", "B");
        assertHintTerms(decoded.get(1), "S");
    }

    @Test
    void decodeLenient_unknownFieldsAndBrokenLines_shouldNotThrow() {
        String wire = ""
                + "FPTOKEN_PREMERGE_HINTS\tv=1\tfutureField=x\n"
                + "hint\tterms=QQ==,Qg==\tkind=mutex\tunknown=zzz\n"
                + "hint\tfoo=bar\tterms=Uw==\textra=1\n"
                + "hint\tterms=\n"
                + "hint\tterms=###bad_base64###\tnewField=v2\n"
                + "garbage line\n";

        List<ExclusiveFpRowsProcessingApi.PremergeHint> decoded = PremergeHintWireCodec.decodeLenient(wire);
        assertEquals(3, decoded.size());
        assertHintTerms(decoded.get(0), "A", "B");
        assertHintTerms(decoded.get(1), "S");
        // bad base64 token falls back to plain-text token bytes but should still parse safely
        assertTrue(decoded.get(2).getTermRefs().size() == 1);
    }

    private static ByteRef ref(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ByteRef(bytes, 0, bytes.length);
    }

    private static void assertHintTerms(ExclusiveFpRowsProcessingApi.PremergeHint hint, String... terms) {
        assertEquals(terms.length, hint.getTermRefs().size());
        for (int i = 0; i < terms.length; i++) {
            assertEquals(terms[i], new String(hint.getTermRefs().get(i).copyBytes(), StandardCharsets.UTF_8));
        }
    }
}
