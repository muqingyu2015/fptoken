package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TryWithResourcesSuppressionUnitTest {

    @Test
    void tryWithResources_shouldKeepCloseFailureAsSuppressed() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            try (FailingCloseResource ignored = new FailingCloseResource()) {
                throw new RuntimeException("body-failure");
            }
        });

        assertEquals("body-failure", ex.getMessage());
        assertEquals(1, ex.getSuppressed().length);
        assertEquals("close-failure", ex.getSuppressed()[0].getMessage());
    }

    @Test
    void tryWithResources_shouldThrowCloseFailureWhenBodySucceeds() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            try (FailingCloseResource ignored = new FailingCloseResource()) {
                // no-op body
            }
        });

        assertEquals("close-failure", ex.getMessage());
        assertEquals(0, ex.getSuppressed().length);
        assertNotNull(ex);
    }

    private static final class FailingCloseResource implements AutoCloseable {
        @Override
        public void close() {
            throw new RuntimeException("close-failure");
        }
    }
}
