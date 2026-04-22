import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class PerformanceTest {
    
    // 模拟原类的缓存优化
    private static final Map<String, Double> coverageCache = new HashMap<>();
    private static final int MAX_CACHE_SIZE = 1000;
    
    // 原方法（模拟）
    private static double calculateCoverage(BitSet bitset1, BitSet bitset2) {
        // 模拟耗时计算
        BitSet intersection = (BitSet) bitset1.clone();
        intersection.and(bitset2);
        int intersectSize = intersection.cardinality();
        int unionSize = bitset1.cardinality() + bitset2.cardinality() - intersectSize;
        
        try {
            Thread.sleep(1); // 模拟1ms计算时间
        } catch (InterruptedException e) {}
        
        return unionSize == 0 ? 0.0 : (double) intersectSize / unionSize;
    }
    
    // 优化后的方法（带缓存）
    private static double calculateCoverageWithCache(BitSet bitset1, BitSet bitset2) {
        String key = bitset1.toString() + "::" + bitset2.toString();
        
        if (coverageCache.containsKey(key)) {
            return coverageCache.get(key);
        }
        
        double result = calculateCoverage(bitset1, bitset2);
        
        // 简单的LRU缓存管理
        if (coverageCache.size() >= MAX_CACHE_SIZE) {
            String firstKey = coverageCache.keySet().iterator().next();
            coverageCache.remove(firstKey);
        }
        
        coverageCache.put(key, result);
        return result;
    }
    
    // 生成测试数据
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
        System.out.println("=== 性能测试开始 ===");
        
        // 生成测试数据
        BitSet[] testData = new BitSet[100];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = generateBitSet(1000, 10); // 1000位，10%密度
        }
        
        // 测试原方法
        System.out.println("测试原方法（无缓存）...");
        long startTime = System.currentTimeMillis();
        double total1 = 0;
        for (int i = 0; i < testData.length - 1; i++) {
            for (int j = i + 1; j < testData.length; j++) {
                total1 += calculateCoverage(testData[i], testData[j]);
            }
        }
        long time1 = System.currentTimeMillis() - startTime;
        System.out.println("原方法耗时: " + time1 + "ms, 结果: " + total1);
        
        // 测试优化方法（有缓存）
        System.out.println("\n测试优化方法（有缓存）...");
        coverageCache.clear();
        startTime = System.currentTimeMillis();
        double total2 = 0;
        for (int i = 0; i < testData.length - 1; i++) {
            for (int j = i + 1; j < testData.length; j++) {
                total2 += calculateCoverageWithCache(testData[i], testData[j]);
            }
        }
        long time2 = System.currentTimeMillis() - startTime;
        System.out.println("优化方法耗时: " + time2 + "ms, 结果: " + total2);
        System.out.println("缓存命中率: " + (coverageCache.size() * 100.0 / 4950) + "%"); // 4950 = 100*99/2
        
        // 性能提升计算
        double improvement = 100.0 * (time1 - time2) / time1;
        System.out.println("\n=== 性能测试结果 ===");
        System.out.println("性能提升: " + String.format("%.1f", improvement) + "%");
        System.out.println("缓存大小: " + coverageCache.size());
        System.out.println("测试完成！");
    }
}