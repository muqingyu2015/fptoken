import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class PerfTest {
    
    // Cache optimization simulation
    private static final Map<String, Double> coverageCache = new HashMap<>();
    private static final int MAX_CACHE_SIZE = 1000;
    
    // Original method (simulated)
    private static double calculateCoverage(BitSet bitset1, BitSet bitset2) {
        // Simulate expensive calculation
        BitSet intersection = (BitSet) bitset1.clone();
        intersection.and(bitset2);
        int intersectSize = intersection.cardinality();
        int unionSize = bitset1.cardinality() + bitset2.cardinality() - intersectSize;
        
        try {
            Thread.sleep(1); // Simulate 1ms computation
        } catch (InterruptedException e) {}
        
        return unionSize == 0 ? 0.0 : (double) intersectSize / unionSize;
    }
    
    // Optimized method (with cache)
    private static double calculateCoverageWithCache(BitSet bitset1, BitSet bitset2) {
        String key = bitset1.toString() + "::" + bitset2.toString();
        
        if (coverageCache.containsKey(key)) {
            return coverageCache.get(key);
        }
        
        double result = calculateCoverage(bitset1, bitset2);
        
        // Simple LRU cache management
        if (coverageCache.size() >= MAX_CACHE_SIZE) {
            String firstKey = coverageCache.keySet().iterator().next();
            coverageCache.remove(firstKey);
        }
        
        coverageCache.put(key, result);
        return result;
    }
    
    // Generate test data
    private static BitSet generateBitSet(int size, int density) {
        BitSet bitset = new BitSet(size);
        for (int i = 0; i < size; i++) {
            if (Math.random() < density / 100.0) {
                bitset.set(i);
            }
        }
        return bitset;
    }
    
    public static void main(String[] args) {
        System.out.println("=== Performance Test Start ===");
        
        // Generate test data
        BitSet[] testData = new BitSet[50]; // Reduced from 100 to make test faster
        for (int i = 0; i < testData.length; i++) {
            testData[i] = generateBitSet(500, 10); // 500 bits, 10% density
        }
        
        // Test original method
        System.out.println("Testing original method (no cache)...");
        long startTime = System.currentTimeMillis();
        double total1 = 0;
        int calls1 = 0;
        for (int i = 0; i < testData.length - 1; i++) {
            for (int j = i + 1; j < testData.length; j++) {
                total1 += calculateCoverage(testData[i], testData[j]);
                calls1++;
            }
        }
        long time1 = System.currentTimeMillis() - startTime;
        System.out.println("Original method time: " + time1 + "ms, calls: " + calls1 + ", result: " + total1);
        
        // Test optimized method (with cache)
        System.out.println("\nTesting optimized method (with cache)...");
        coverageCache.clear();
        startTime = System.currentTimeMillis();
        double total2 = 0;
        int calls2 = 0;
        for (int i = 0; i < testData.length - 1; i++) {
            for (int j = i + 1; j < testData.length; j++) {
                total2 += calculateCoverageWithCache(testData[i], testData[j]);
                calls2++;
            }
        }
        long time2 = System.currentTimeMillis() - startTime;
        System.out.println("Optimized method time: " + time2 + "ms, calls: " + calls2 + ", result: " + total2);
        System.out.println("Cache size: " + coverageCache.size());
        
        // Performance improvement calculation
        double improvement = 100.0 * (time1 - time2) / time1;
        System.out.println("\n=== Performance Test Results ===");
        System.out.println("Performance improvement: " + String.format("%.1f", improvement) + "%");
        System.out.println("Time saved: " + (time1 - time2) + "ms");
        System.out.println("Cache hit ratio: " + (coverageCache.size() * 100.0 / calls2) + "%");
        System.out.println("Test completed!");
    }
}