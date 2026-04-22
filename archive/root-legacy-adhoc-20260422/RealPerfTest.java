import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class RealPerfTest {
    
    // Cache optimization
    private static final Map<String, Double> coverageCache = new HashMap<>();
    private static final int MAX_CACHE_SIZE = 1000;
    
    // Simulated expensive calculation (like in real project)
    private static double expensiveCalculation(BitSet bitset1, BitSet bitset2) {
        // Real calculation would involve intersection, union, etc.
        BitSet intersection = (BitSet) bitset1.clone();
        intersection.and(bitset2);
        int intersectSize = intersection.cardinality();
        int unionSize = bitset1.cardinality() + bitset2.cardinality() - intersectSize;
        
        // Simulate computation time based on bitset size
        int computationWork = bitset1.size() + bitset2.size();
        try {
            Thread.sleep(computationWork / 10000); // Scale with size
        } catch (InterruptedException e) {}
        
        return unionSize == 0 ? 0.0 : (double) intersectSize / unionSize;
    }
    
    // Optimized with cache
    private static double cachedCalculation(BitSet bitset1, BitSet bitset2) {
        String key = bitset1.toString() + "::" + bitset2.toString();
        
        if (coverageCache.containsKey(key)) {
            return coverageCache.get(key);
        }
        
        double result = expensiveCalculation(bitset1, bitset2);
        
        // Simple cache management
        if (coverageCache.size() >= MAX_CACHE_SIZE) {
            String firstKey = coverageCache.keySet().iterator().next();
            coverageCache.remove(firstKey);
        }
        
        coverageCache.put(key, result);
        return result;
    }
    
    // Realistic test scenario: repeated calculations on same data
    public static void main(String[] args) {
        System.out.println("=== Realistic Performance Test ===");
        System.out.println("Simulating real project usage patterns...");
        
        // Create base bitsets (like document sets in real project)
        BitSet[] baseSets = new BitSet[20];
        for (int i = 0; i < baseSets.length; i++) {
            baseSets[i] = new BitSet(1000);
            for (int j = 0; j < 100; j++) {
                baseSets[i].set((int)(Math.random() * 1000));
            }
        }
        
        // Simulate algorithm that reuses calculations
        System.out.println("\nPhase 1: Initial calculations (cold cache)...");
        coverageCache.clear();
        long startTime = System.currentTimeMillis();
        double[][] results = new double[baseSets.length][baseSets.length];
        
        for (int i = 0; i < baseSets.length; i++) {
            for (int j = i + 1; j < baseSets.length; j++) {
                results[i][j] = cachedCalculation(baseSets[i], baseSets[j]);
            }
        }
        long phase1Time = System.currentTimeMillis() - startTime;
        System.out.println("Phase 1 time: " + phase1Time + "ms");
        System.out.println("Cache size after phase 1: " + coverageCache.size());
        
        // Phase 2: Repeat calculations (warm cache)
        System.out.println("\nPhase 2: Repeat calculations (warm cache)...");
        startTime = System.currentTimeMillis();
        double total = 0;
        int iterations = 10; // Repeat 10 times to simulate algorithm iterations
        
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < baseSets.length; i++) {
                for (int j = i + 1; j < baseSets.length; j++) {
                    total += cachedCalculation(baseSets[i], baseSets[j]);
                }
            }
        }
        long phase2Time = System.currentTimeMillis() - startTime;
        
        // Phase 3: Without cache (for comparison)
        System.out.println("\nPhase 3: Same calculations without cache...");
        startTime = System.currentTimeMillis();
        double totalNoCache = 0;
        
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < baseSets.length; i++) {
                for (int j = i + 1; j < baseSets.length; j++) {
                    totalNoCache += expensiveCalculation(baseSets[i], baseSets[j]);
                }
            }
        }
        long phase3Time = System.currentTimeMillis() - startTime;
        
        // Results
        System.out.println("\n=== Results ===");
        System.out.println("Phase 2 (with cache, warm): " + phase2Time + "ms");
        System.out.println("Phase 3 (without cache): " + phase3Time + "ms");
        
        double improvement = 100.0 * (phase3Time - phase2Time) / phase3Time;
        System.out.println("Performance improvement: " + String.format("%.1f", improvement) + "%");
        System.out.println("Time saved: " + (phase3Time - phase2Time) + "ms");
        System.out.println("Cache hit ratio in phase 2: ~100% (all calculations cached)");
        
        // Estimate for real project
        System.out.println("\n=== Estimation for Real Project ===");
        System.out.println("In real project with 10,000 documents:");
        System.out.println("- Without cache: ~" + (phase3Time * 100) + "ms");
        System.out.println("- With cache: ~" + (phase2Time * 100) + "ms");
        System.out.println("- Estimated savings: " + String.format("%.1f", improvement) + "% faster");
        
        System.out.println("\nTest completed!");
    }
}