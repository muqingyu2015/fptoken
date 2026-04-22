package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门测试"尽量长的互斥候选频繁项集"目标的测试类
 * 
 * 测试重点：
 * 1. 长模式优先选择
 * 2. 互斥性保证
 * 3. 性能边界测试
 * 4. 参数调优验证
 */
class LongMutuallyExclusivePatternsTest {

    private static final int MAX_CANDIDATE_COUNT = 100_000;

    /**
     * 测试1：基础长模式发现
     * 场景：存在多个长度不同的频繁模式，验证算法优先选择长模式
     */
    @Test
    void testPrefersLongerPatterns() {
        // 创建测试数据：包含短模式(2项)和长模式(4项)
        List<DocTerms> rows = new ArrayList<>();
        int docs = 100;
        
        // 长模式：ABCD 在80个文档中共现
        byte[] A = ByteArrayTestSupport.hex("41"); // 'A'
        byte[] B = ByteArrayTestSupport.hex("42"); // 'B'
        byte[] C = ByteArrayTestSupport.hex("43"); // 'C'
        byte[] D = ByteArrayTestSupport.hex("44"); // 'D'
        
        // 短模式：EF 在90个文档中共现（支持度更高但长度更短）
        byte[] E = ByteArrayTestSupport.hex("45"); // 'E'
        byte[] F = ByteArrayTestSupport.hex("46"); // 'F'
        
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            if (i < 80) {
                // 包含长模式ABCD
                terms.add(A);
                terms.add(B);
                terms.add(C);
                terms.add(D);
            } else if (i < 90) {
                // 包含短模式EF
                terms.add(E);
                terms.add(F);
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }
        
        // 设置minItemsetSize=2，让算法可以选择2项或4项模式
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 70, 2, 6, MAX_CANDIDATE_COUNT);
        
        List<SelectedGroup> groups = result.getGroups();
        assertFalse(groups.isEmpty(), "应该找到至少一个模式");
        
        // 验证：在互斥约束下，应该优先选择长模式(ABCD)而不是短模式(EF)
        boolean foundLongPattern = false;
        for (SelectedGroup group : groups) {
            if (group.getTerms().size() == 4) {
                foundLongPattern = true;
                break;
            }
        }
        assertTrue(foundLongPattern, "应该优先选择长模式(4项)");
    }

    /**
     * 测试2：嵌套模式的互斥选择
     * 场景：存在嵌套模式(ABC包含AB)，验证算法正确选择互斥的最长模式
     */
    @Test
    void testNestedPatternsMutualExclusion() {
        List<DocTerms> rows = new ArrayList<>();
        int docs = 100;
        
        byte[] A = ByteArrayTestSupport.hex("41");
        byte[] B = ByteArrayTestSupport.hex("42");
        byte[] C = ByteArrayTestSupport.hex("43");
        
        // 模式1：ABC (3项，支持度80)
        // 模式2：AB (2项，支持度100，被ABC包含)
        
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            terms.add(A);
            terms.add(B);
            if (i < 80) {
                // 包含ABC模式
                terms.add(C);
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }
        
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 70, 2, 4, MAX_CANDIDATE_COUNT);
        
        List<SelectedGroup> groups = result.getGroups();
        
        // 验证互斥性：不应该同时选择ABC和AB（它们共享A和B）
        Set<ByteArrayTestSupport.ByteArrayWrapper> selectedTerms = new HashSet<>();
        for (SelectedGroup group : groups) {
            for (byte[] term : group.getTerms()) {
                ByteArrayTestSupport.ByteArrayWrapper wrapper = 
                    new ByteArrayTestSupport.ByteArrayWrapper(term);
                assertFalse(selectedTerms.contains(wrapper), 
                    "术语" + ByteArrayTestSupport.hexString(term) + "在多个组中出现，违反互斥性");
                selectedTerms.add(wrapper);
            }
        }
        
        // 验证应该选择ABC而不是AB（因为ABC更长）
        boolean foundABC = false;
        boolean foundAB = false;
        for (SelectedGroup group : groups) {
            if (group.getTerms().size() == 3) {
                foundABC = true;
            } else if (group.getTerms().size() == 2) {
                foundAB = true;
            }
        }
        
        assertTrue(foundABC, "应该选择更长的ABC模式");
        assertFalse(foundAB, "不应该选择被包含的AB模式");
    }

    /**
     * 测试3：最大长度边界测试
     * 场景：测试算法在达到maxItemsetSize时的行为
     */
    @Test
    void testMaxItemsetSizeBoundary() {
        List<DocTerms> rows = new ArrayList<>();
        int docs = 120;
        
        // 创建6项的长模式
        byte[][] longPattern = new byte[][] {
            ByteArrayTestSupport.hex("41"), // A
            ByteArrayTestSupport.hex("42"), // B
            ByteArrayTestSupport.hex("43"), // C
            ByteArrayTestSupport.hex("44"), // D
            ByteArrayTestSupport.hex("45"), // E
            ByteArrayTestSupport.hex("46")  // F
        };
        
        for (int i = 0; i < docs; i++) {
            if (i < 100) {
                rows.add(ByteArrayTestSupport.doc(i, longPattern));
            }
        }
        
        // 测试1：maxItemsetSize=4，应该找不到6项模式
        ExclusiveSelectionResult result1 = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 90, 2, 4, MAX_CANDIDATE_COUNT);
        
        for (SelectedGroup group : result1.getGroups()) {
            assertTrue(group.getTerms().size() <= 4, 
                "项集长度不应超过maxItemsetSize=4，实际长度：" + group.getTerms().size());
        }
        
        // 测试2：maxItemsetSize=6，应该能找到6项模式
        ExclusiveSelectionResult result2 = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 90, 2, 6, MAX_CANDIDATE_COUNT);
        
        boolean foundLongPattern = false;
        for (SelectedGroup group : result2.getGroups()) {
            if (group.getTerms().size() == 6) {
                foundLongPattern = true;
                break;
            }
        }
        assertTrue(foundLongPattern, "当maxItemsetSize=6时应该能找到6项模式");
    }

    /**
     * 测试4：性能边界 - 大量长模式候选
     * 场景：测试算法处理大量长模式候选时的性能和行为
     */
    @Test
    void testPerformanceWithManyLongCandidates() {
        List<DocTerms> rows = new ArrayList<>();
        int docs = 200;
        int vocabSize = 50; // 词汇表大小
        
        // 生成随机但包含一些长模式的数据
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            
            // 每个文档包含10-20个随机术语
            int termCount = 10 + (i % 11);
            for (int j = 0; j < termCount; j++) {
                byte term = (byte) ((i + j) % vocabSize);
                terms.add(new byte[]{term});
            }
            
            // 添加一些固定的长模式（每10个文档添加一次）
            if (i % 10 == 0) {
                for (int k = 0; k < 8; k++) {
                    terms.add(new byte[]{(byte) (100 + k)}); // 长模式术语
                }
            }
            
            rows.add(ByteArrayTestSupport.doc(i, terms.toArray(new byte[0][])));
        }
        
        // 测试性能：记录执行时间
        long startTime = System.currentTimeMillis();
        
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 5, 2, 8, MAX_CANDIDATE_COUNT);
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        
        System.out.println("性能测试 - 处理" + docs + "个文档，词汇表大小" + vocabSize);
        System.out.println("执行时间: " + executionTime + "ms");
        System.out.println("找到组数: " + result.getGroups().size());
        System.out.println("候选数: " + result.getCandidateCount());
        System.out.println("是否因候选限制截断: " + result.isTruncatedByCandidateLimit());
        
        // 验证：执行时间应该在合理范围内
        assertTrue(executionTime < 10000, "处理200个文档应在10秒内完成，实际：" + executionTime + "ms");
        
        // 验证：如果找到模式，应该是互斥的
        Set<ByteArrayTestSupport.ByteArrayWrapper> selectedTerms = new HashSet<>();
        for (SelectedGroup group : result.getGroups()) {
            for (byte[] term : group.getTerms()) {
                ByteArrayTestSupport.ByteArrayWrapper wrapper = 
                    new ByteArrayTestSupport.ByteArrayWrapper(term);
                assertFalse(selectedTerms.contains(wrapper), "违反互斥性");
                selectedTerms.add(wrapper);
            }
        }
    }

    /**
     * 测试5：评分函数对长模式的影响
     * 场景：测试不同的评分函数如何影响长模式的选择
     */
    @Test
    void testScoringFunctionImpactOnLongPatterns() {
        // 这个测试需要扩展当前的API以支持自定义评分函数
        // 目前先验证基本行为
        
        List<DocTerms> rows = new ArrayList<>();
        int docs = 100;
        
        // 模式1：AB (2项，支持度100)
        // 模式2：CDE (3项，支持度80)
        
        byte[] A = ByteArrayTestSupport.hex("41");
        byte[] B = ByteArrayTestSupport.hex("42");
        byte[] C = ByteArrayTestSupport.hex("43");
        byte[] D = ByteArrayTestSupport.hex("44");
        byte[] E = ByteArrayTestSupport.hex("45");
        
        for (int i = 0; i < docs; i++) {
            // 所有文档都包含AB
            rows.add(ByteArrayTestSupport.doc(i, A, B));
            
            // 80个文档包含CDE
            if (i < 80) {
                rows.add(ByteArrayTestSupport.doc(i, C, D, E));
            }
        }
        
        // 使用默认评分函数（应该优先考虑支持度）
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 70, 2, 4, MAX_CANDIDATE_COUNT);
        
        System.out.println("默认评分函数结果:");
        for (SelectedGroup group : result.getGroups()) {
            System.out.println("  模式长度: " + group.getTerms().size() + 
                             ", 术语: " + group.getTerms());
        }
        
        // 这里可以添加对自定义评分函数的测试
        // 当API支持时，测试长度优先的评分函数
    }
}