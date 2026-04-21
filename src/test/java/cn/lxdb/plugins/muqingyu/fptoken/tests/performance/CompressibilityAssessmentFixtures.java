package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

final class CompressibilityAssessmentFixtures {
    private CompressibilityAssessmentFixtures() {
    }

    static List<DocTerms> longPatternDataset(int docs, long seed) {
        Random random = new Random(seed);
        List<byte[]> patternA = tokenRange(1000, 8);
        List<byte[]> patternB = tokenRange(2000, 7);
        List<byte[]> patternC = tokenRange(3000, 6);
        List<byte[]> shared = tokenRange(900, 2);

        List<DocTerms> rows = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            terms.addAll(shared);
            int mod = i % 10;
            if (mod <= 5) {
                terms.addAll(patternA);
            } else if (mod <= 8) {
                terms.addAll(patternB);
            } else {
                terms.addAll(patternC);
            }
            for (int n = 0; n < 6; n++) {
                terms.add(token(20000 + random.nextInt(6000)));
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }
        return rows;
    }

    static List<DocTerms> httpHeaderLikeDataset(int docs, long seed) {
        Random random = new Random(seed);
        List<byte[]> coreHeaders = List.of(
                ascii("GET"), ascii("HTTP/1.1"), ascii("HOST"),
                ascii("USER-AGENT"), ascii("ACCEPT"), ascii("CONNECTION"),
                ascii("KEEP-ALIVE"), ascii("CONTENT-TYPE"));
        List<byte[]> routes = List.of(ascii("/api/v1/user"), ascii("/api/v1/order"), ascii("/login"), ascii("/health"));
        List<DocTerms> out = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>(coreHeaders);
            terms.add(routes.get(i % routes.size()));
            terms.add(ascii("HOST:svc" + (i % 10)));
            terms.add(ascii("UA:v" + (i % 5)));
            for (int n = 0; n < 3; n++) {
                terms.add(token(24000 + random.nextInt(2000)));
            }
            out.add(ByteArrayTestSupport.doc(i, terms));
        }
        return out;
    }

    static List<DocTerms> mixedTrafficDataset(int docs, long seed) {
        Random random = new Random(seed);
        List<byte[]> httpCore = List.of(ascii("GET"), ascii("HTTP/1.1"), ascii("HOST"), ascii("ACCEPT"), ascii("CONN"));
        List<byte[]> dnsCore = List.of(ascii("DNS_Q"), ascii("TYPE_A"), ascii("CLASS_IN"), ascii("UDP_53"));
        List<DocTerms> out = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            int bucket = i % 10;
            if (bucket < 5) {
                terms.addAll(httpCore);
                terms.add(ascii("/p/" + (i % 20)));
            } else if (bucket < 8) {
                terms.addAll(dnsCore);
                terms.add(ascii("QNAME:" + (i % 100)));
            } else {
                for (int n = 0; n < 8; n++) {
                    terms.add(token(40000 + random.nextInt(20000)));
                }
            }
            terms.add(token(500 + (i % 32)));
            out.add(ByteArrayTestSupport.doc(i, terms));
        }
        return out;
    }

    static List<DocTerms> randomPayloadDataset(int docs, int termsPerDoc, long seed) {
        Random random = new Random(seed);
        List<DocTerms> out = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            for (int n = 0; n < termsPerDoc; n++) {
                terms.add(token(60000 + random.nextInt(30000)));
            }
            out.add(ByteArrayTestSupport.doc(i, terms));
        }
        return out;
    }

    private static List<byte[]> tokenRange(int base, int count) {
        List<byte[]> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(token(base + i));
        }
        return out;
    }

    private static byte[] token(int id) {
        return new byte[] {(byte) ((id >>> 16) & 0xFF), (byte) ((id >>> 8) & 0xFF), (byte) (id & 0xFF)};
    }

    private static byte[] ascii(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }
}
