package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Lenient wire codec for pre-merge hints.
 *
 * <p>Format v1 (line-oriented):
 * <pre>
 * FPTOKEN_PREMERGE_HINTS\tv=1
 * hint\tterms=&lt;base64(term1)&gt;,&lt;base64(term2)&gt;\tkind=mutex
 * hint\tterms=&lt;base64(term)&gt;\tkind=single
 * </pre>
 *
 * <p>Forward-compatibility rule: unknown keys are ignored during decode.
 * Malformed lines/fields are skipped instead of throwing.</p>
 */
public final class PremergeHintWireCodec {
    private static final String MAGIC = "FPTOKEN_PREMERGE_HINTS";
    private static final String HEADER = MAGIC + "\tv=1";

    private PremergeHintWireCodec() {
    }

    public static String encodeV1(List<ExclusiveFpRowsProcessingApi.PremergeHint> hints) {
        if (hints == null || hints.isEmpty()) {
            return HEADER + "\n";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(HEADER).append('\n');
        for (int i = 0; i < hints.size(); i++) {
            ExclusiveFpRowsProcessingApi.PremergeHint hint = hints.get(i);
            if (hint == null || hint.getTermRefs().isEmpty()) {
                continue;
            }
            sb.append("hint\tterms=");
            appendEncodedTerms(sb, hint.getTermRefs());
            sb.append('\n');
        }
        return sb.toString();
    }

    public static List<ExclusiveFpRowsProcessingApi.PremergeHint> decodeLenient(String payload) {
        if (payload == null || payload.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String[] lines = payload.split("\\r?\\n");
        int start = 0;
        if (lines.length > 0 && lines[0].startsWith(MAGIC)) {
            start = 1;
        }
        List<ExclusiveFpRowsProcessingApi.PremergeHint> out = new ArrayList<>();
        for (int i = start; i < lines.length; i++) {
            ExclusiveFpRowsProcessingApi.PremergeHint hint = parseHintLine(lines[i]);
            if (hint != null) {
                out.add(hint);
            }
        }
        return out;
    }

    private static void appendEncodedTerms(StringBuilder sb, List<ByteRef> termRefs) {
        boolean first = true;
        for (int i = 0; i < termRefs.size(); i++) {
            ByteRef term = termRefs.get(i);
            if (term == null || term.getLength() == 0) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            byte[] raw = term.copyBytes();
            sb.append(Base64.getEncoder().encodeToString(raw));
        }
    }

    private static ExclusiveFpRowsProcessingApi.PremergeHint parseHintLine(String line) {
        if (line == null) {
            return null;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty() || !trimmed.startsWith("hint")) {
            return null;
        }
        String[] fields = trimmed.split("\\t");
        String termsValue = null;
        for (int i = 1; i < fields.length; i++) {
            int eq = fields[i].indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = fields[i].substring(0, eq).trim();
            String value = fields[i].substring(eq + 1).trim();
            if ("terms".equals(key)) {
                termsValue = value;
            }
            // Unknown keys are intentionally ignored (forward compatibility).
        }
        if (termsValue == null || termsValue.isEmpty()) {
            return null;
        }
        List<ByteRef> refs = decodeTermsLenient(termsValue);
        if (refs.isEmpty()) {
            return null;
        }
        return new ExclusiveFpRowsProcessingApi.PremergeHint(refs);
    }

    private static List<ByteRef> decodeTermsLenient(String encodedTerms) {
        String[] parts = encodedTerms.split(",");
        List<ByteRef> refs = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            String token = parts[i].trim();
            if (token.isEmpty()) {
                continue;
            }
            byte[] bytes = decodeBase64Lenient(token);
            if (bytes != null && bytes.length > 0) {
                refs.add(new ByteRef(bytes, 0, bytes.length));
            }
        }
        return refs;
    }

    private static byte[] decodeBase64Lenient(String token) {
        try {
            return Base64.getDecoder().decode(token);
        } catch (IllegalArgumentException ex) {
            // Optional fallback for old/plain-text emitters.
            byte[] plain = token.getBytes(StandardCharsets.UTF_8);
            return plain.length == 0 ? null : plain;
        }
    }
}
