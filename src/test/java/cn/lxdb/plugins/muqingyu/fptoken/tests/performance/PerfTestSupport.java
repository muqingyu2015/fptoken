package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

final class PerfTestSupport {
    private PerfTestSupport() {
    }

    static long elapsedMillis(Runnable runnable) {
        long t0 = System.nanoTime();
        runnable.run();
        return (System.nanoTime() - t0) / 1_000_000L;
    }

    static double throughputPerSecond(int count, long elapsedMs) {
        long safeMs = Math.max(1L, elapsedMs);
        return (count * 1000.0d) / safeMs;
    }

    static int intProp(String key, int fallback) {
        return Integer.getInteger(key, fallback);
    }

    static long longProp(String key, long fallback) {
        return Long.getLong(key, fallback);
    }

    static List<DocTerms> standardPcapRows(int records) {
        int recordLen = intProp("fptoken.perf.recordLen", 1024);
        int windowLen = intProp("fptoken.perf.windowLen", 128);
        // 默认步长从 1 调整为 8，降低默认性能套件的对象数量与堆压力，避免 OOM。
        // 需要极限压测时可通过 -Dfptoken.perf.windowStep=1 覆盖回高密度窗口。
        int step = intProp("fptoken.perf.windowStep", 8);
        if (step <= 0) {
            step = 1;
        }
        if (windowLen > recordLen) {
            windowLen = recordLen;
        }
        // records 很大时，按窗口展开会产生海量 DocTerms；默认把文档量控制在 3 万以内，
        // 让单个性能用例在常规机器上更容易控制在 10 秒预算。
        int maxDocs = intProp("fptoken.perf.maxDocs", 30_000);
        if (records > 0 && maxDocs > 0) {
            int minStep = minimumStepToRespectDocBudget(records, recordLen, windowLen, maxDocs);
            if (minStep > step) {
                step = minStep;
            }
        }
        return ByteArrayTestSupport.pcapLikeBatch(records, recordLen, windowLen, step);
    }

    private static int minimumStepToRespectDocBudget(int records, int recordLen, int windowLen, int maxDocs) {
        int maxWindowsPerRecord = Math.max(1, maxDocs / Math.max(1, records));
        int maxStart = Math.max(0, recordLen - windowLen);
        // windowsPerRecord = floor(maxStart / step) + 1 <= maxWindowsPerRecord
        // => step >= maxStart / max(1, maxWindowsPerRecord - 1)
        if (maxWindowsPerRecord <= 1) {
            return Math.max(1, maxStart + 1);
        }
        int denominator = Math.max(1, maxWindowsPerRecord - 1);
        return Math.max(1, (maxStart + denominator - 1) / denominator);
    }

    static List<DocTerms> rowsWithVocabulary(int docs, int vocabSize, int termsPerDoc, long seed) {
        Random random = new Random(seed);
        List<byte[]> vocab = new ArrayList<>(vocabSize);
        for (int i = 0; i < vocabSize; i++) {
            vocab.add(term3(i));
        }

        List<DocTerms> rows = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            for (int k = 0; k < termsPerDoc; k++) {
                int tid = random.nextInt(vocabSize);
                terms.add(vocab.get(tid));
            }
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        return rows;
    }

    static List<DocTerms> rowsWithPowerLawDistribution(int docs, int vocabSize, int termsPerDoc, long seed) {
        Random random = new Random(seed);
        List<byte[]> vocab = new ArrayList<>(vocabSize);
        for (int i = 0; i < vocabSize; i++) {
            vocab.add(term3(i));
        }

        int hotSize = Math.max(8, vocabSize / 20);
        List<DocTerms> rows = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            for (int k = 0; k < termsPerDoc; k++) {
                int tid;
                if (random.nextDouble() < 0.82d) {
                    tid = random.nextInt(hotSize);
                } else {
                    tid = hotSize + random.nextInt(Math.max(1, vocabSize - hotSize));
                }
                terms.add(vocab.get(tid));
            }
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        return rows;
    }

    static List<DocTerms> rowsWithDuplicateRatio(int docs, int vocabSize, int termsPerDoc, double duplicateRatio, long seed) {
        Random random = new Random(seed);
        List<byte[]> vocab = new ArrayList<>(vocabSize);
        for (int i = 0; i < vocabSize; i++) {
            vocab.add(term3(i));
        }

        List<DocTerms> rows = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            int base = random.nextInt(vocabSize);
            for (int k = 0; k < termsPerDoc; k++) {
                if (random.nextDouble() < duplicateRatio) {
                    terms.add(vocab.get(base));
                } else {
                    terms.add(vocab.get(random.nextInt(vocabSize)));
                }
            }
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        return rows;
    }

    static List<CandidateItemset> syntheticCandidates(
            int count,
            int dictionarySize,
            int docs,
            int minLen,
            int maxLen,
            long seed
    ) {
        Random random = new Random(seed);
        List<CandidateItemset> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = minLen + random.nextInt(Math.max(1, maxLen - minLen + 1));
            int[] termIds = distinctRandomTerms(random, dictionarySize, len);
            BitSet bits = new BitSet(docs);
            int support = 1 + random.nextInt(Math.max(1, Math.min(200, docs)));
            for (int s = 0; s < support; s++) {
                bits.set(random.nextInt(Math.max(1, docs)));
            }
            out.add(new CandidateItemset(termIds, bits, bits.cardinality()));
        }
        return out;
    }

    static List<DocTerms> protocolMixRows(int docs, long seed) {
        Random random = new Random(seed);
        byte[] httpGet = bytes("474554202f696e6465782e68746d6c20485454502f312e31");
        byte[] dnsQ = bytes("000100000001000000000000");
        byte[] tcpSyn = bytes("4500003c1c4640004006");
        byte[] tls = bytes("1603010200010001fc03");
        List<DocTerms> rows = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            int p = random.nextInt(4);
            List<byte[]> terms = new ArrayList<>();
            terms.add(term3(i & 0xFFFF));
            if (p == 0) {
                terms.add(slice(httpGet, 0, 3));
                terms.add(slice(httpGet, 4, 3));
                terms.add(new byte[] {(byte) 0x50, (byte) 0x4f, (byte) 0x53});
            } else if (p == 1) {
                terms.add(slice(dnsQ, 0, 3));
                terms.add(slice(dnsQ, 3, 3));
                terms.add(new byte[] {(byte) 0x00, (byte) 0x35});
            } else if (p == 2) {
                terms.add(slice(tcpSyn, 0, 3));
                terms.add(slice(tcpSyn, 2, 3));
                terms.add(new byte[] {(byte) 0x00, (byte) 0x50});
            } else {
                terms.add(slice(tls, 0, 3));
                terms.add(slice(tls, 3, 3));
                terms.add(new byte[] {(byte) 0x01, (byte) 0xbb});
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }
        return rows;
    }

    static List<DocTerms> highCardinalityRows(int docs, int termsPerDoc, long seed) {
        Random random = new Random(seed);
        List<DocTerms> out = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            for (int i = 0; i < termsPerDoc; i++) {
                int id = (d * 8191 + i * 131 + random.nextInt(1 << 16)) & 0xFFFFFF;
                terms.add(term3(id));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    static List<DocTerms> lowCardinalityRows(int docs, int termsPerDoc, long seed) {
        Random random = new Random(seed);
        List<byte[]> base = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            base.add(term3(i));
        }
        List<DocTerms> out = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            for (int i = 0; i < termsPerDoc; i++) {
                terms.add(base.get(random.nextInt(base.size())));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    static List<DocTerms> rowsWithEncryptedRatio(int docs, int termsPerDoc, double encryptedRatio, long seed) {
        Random random = new Random(seed);
        List<DocTerms> out = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            boolean encrypted = random.nextDouble() < encryptedRatio;
            for (int i = 0; i < termsPerDoc; i++) {
                if (encrypted) {
                    byte[] b = new byte[3];
                    random.nextBytes(b);
                    terms.add(b);
                } else {
                    int id = ((d + 1) * (i + 3)) & 0x7FFF;
                    terms.add(term3(id));
                }
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    static List<DocTerms> skewedRows(int docs, int termsPerDoc, double superNodeRatio, long seed) {
        Random random = new Random(seed);
        byte[] hotA = term3(0xABCD);
        byte[] hotB = term3(0x0F0F);
        int hotDocs = (int) Math.round(docs * superNodeRatio);
        List<DocTerms> out = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(termsPerDoc);
            if (d < hotDocs) {
                terms.add(hotA);
                terms.add(hotB);
            }
            for (int i = terms.size(); i < termsPerDoc; i++) {
                int id = random.nextInt(1 << 15);
                terms.add(term3(id));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        Collections.shuffle(out, random);
        return out;
    }

    static double supportCoverageRatio(int docs, int totalSupportAcrossGroups) {
        if (docs <= 0) {
            return 0d;
        }
        return Math.min(1d, (double) totalSupportAcrossGroups / docs);
    }

    static boolean termsContainAll(List<byte[]> haystack, List<byte[]> needles) {
        for (byte[] n : needles) {
            boolean found = false;
            for (byte[] h : haystack) {
                if (ByteArrayUtils.compareUnsigned(h, n) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    static long median(List<Long> values) {
        return percentile(values, 0.50d);
    }

    static long percentile(List<Long> values, double p) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        double bounded = Math.max(0d, Math.min(1d, p));
        int idx = (int) Math.floor((sorted.size() - 1) * bounded);
        return sorted.get(idx).longValue();
    }

    static double stddevMillis(List<Long> values) {
        if (values == null || values.size() <= 1) {
            return 0d;
        }
        double sum = 0d;
        for (Long v : values) {
            sum += v.longValue();
        }
        double mean = sum / values.size();
        double sq = 0d;
        for (Long v : values) {
            double d = v.longValue() - mean;
            sq += d * d;
        }
        return Math.sqrt(sq / values.size());
    }

    static void repeatWithinBudget(int maxIterations, long maxElapsedMs, Runnable runnable) {
        long safeBudgetMs = Math.max(1L, maxElapsedMs);
        long deadlineNs = System.nanoTime() + safeBudgetMs * 1_000_000L;
        int safeIterations = Math.max(0, maxIterations);
        for (int i = 0; i < safeIterations; i++) {
            if (System.nanoTime() >= deadlineNs) {
                break;
            }
            runnable.run();
        }
    }

    private static int[] distinctRandomTerms(Random random, int dictionarySize, int len) {
        BitSet used = new BitSet(dictionarySize);
        int[] out = new int[len];
        int i = 0;
        while (i < len) {
            int t = random.nextInt(Math.max(1, dictionarySize));
            if (!used.get(t)) {
                used.set(t);
                out[i++] = t;
            }
        }
        return out;
    }

    private static byte[] term3(int id) {
        return new byte[] {
                (byte) ((id >>> 16) & 0xFF),
                (byte) ((id >>> 8) & 0xFF),
                (byte) (id & 0xFF)
        };
    }

    private static byte[] bytes(String hex) {
        return ByteArrayTestSupport.hex(hex);
    }

    private static byte[] slice(byte[] src, int off, int len) {
        int n = Math.max(0, Math.min(len, src.length - off));
        byte[] out = new byte[n];
        System.arraycopy(src, off, out, 0, n);
        return out;
    }
}
