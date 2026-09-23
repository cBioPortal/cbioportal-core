package org.mskcc.cbio.portal.dao;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDaoCopyNumberSegmentFgaRetry {

    @Test
    public void retriesEmptyReplicaReadUntilAllSamplesAreVisible() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Map<Integer, String> result = DaoCopyNumberSegment.retryIncompleteFga(
                Set.of(1, 2), () -> attempts.incrementAndGet() == 1
                        ? Map.of() : Map.of(1, "0.2", 2, "0"), 3, 0);

        assertEquals(Map.of(1, "0.2", 2, "0"), result);
        assertEquals(2, attempts.get());
    }

    @Test
    public void retriesPartialReplicaRead() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        DaoCopyNumberSegment.retryIncompleteFga(Set.of(1, 2), () ->
                attempts.incrementAndGet() == 1 ? Map.of(1, "0.2")
                        : Map.of(1, "0.2", 2, "0.1"), 3, 0);
        assertEquals(2, attempts.get());
    }

    @Test
    public void checksSampleIdentityNotJustResultCount() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        DaoCopyNumberSegment.retryIncompleteFga(Set.of(1, 2), () ->
                attempts.incrementAndGet() == 1 ? Map.of(1, "0.2", 3, "0.1")
                        : Map.of(1, "0.2", 2, "0.1"), 3, 0);
        assertEquals(2, attempts.get());
    }

    @Test
    public void failsAfterBoundedAttemptsInsteadOfAcceptingMissingFga() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        try {
            DaoCopyNumberSegment.retryIncompleteFga(Set.of(1), () -> {
                attempts.incrementAndGet();
                return Map.of();
            }, 3, 0);
            fail("Expected incomplete FGA calculation to fail");
        } catch (DaoException e) {
            assertTrue(e.getMessage().contains("1 expected samples after 3 attempts"));
        }
        assertEquals(3, attempts.get());
    }

    @Test
    public void retainsAllSamplesModeWhenExpectedIdsAreNotProvided() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Map<Integer, String> result = DaoCopyNumberSegment.retryIncompleteFga(null, () -> {
            attempts.incrementAndGet();
            return Map.of(1, "0.2");
        }, 3, 0);
        assertEquals(Map.of(1, "0.2"), result);
        assertEquals(1, attempts.get());
    }
}
