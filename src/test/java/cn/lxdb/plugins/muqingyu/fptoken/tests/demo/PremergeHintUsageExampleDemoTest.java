package cn.lxdb.plugins.muqingyu.fptoken.tests.demo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import cn.lxdb.plugins.muqingyu.fptoken.demo.PremergeHintUsageExample;
import org.junit.jupiter.api.Test;

class PremergeHintUsageExampleDemoTest {

    @Test
    void main_shouldRunWithoutThrowing() {
        assertDoesNotThrow(() -> PremergeHintUsageExample.main(new String[0]));
    }
}
