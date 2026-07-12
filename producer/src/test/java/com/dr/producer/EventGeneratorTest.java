package com.dr.producer;

import com.dr.producer.model.CommitLogEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventGeneratorTest {

    private final EventGenerator generator = new EventGenerator();

    @Test
    void generatesAValidUuidEventId() {
        CommitLogEvent event = generator.generate(0);
        assertDoesNotThrow(() -> UUID.fromString(event.eventId()),
                "event_id must be a valid UUID string");
    }

    @Test
    void timestampIsCurrentEpochSeconds() {
        long before = Instant.now().getEpochSecond();
        CommitLogEvent event = generator.generate(0);
        long after = Instant.now().getEpochSecond();

        assertTrue(event.timestamp() >= before && event.timestamp() <= after,
                "timestamp should be epoch seconds captured at generation time");
    }

    @Test
    void opTypeCyclesThroughCreateUpdateDeleteByIndex() {
        assertEquals("CREATE", generator.generate(0).opType());
        assertEquals("UPDATE", generator.generate(1).opType());
        assertEquals("DELETE", generator.generate(2).opType());
        assertEquals("CREATE", generator.generate(3).opType(), "should wrap back to CREATE");
    }

    @Test
    void keyFollowsDocPrefixFormat() {
        CommitLogEvent event = generator.generate(0);
        assertTrue(event.key().matches("doc:[0-9a-f]{4}"),
                "key should look like 'doc:8f7b' per the assignment's schema example");
    }

    @Test
    void statusReflectsOpType() {
        assertEquals("active", generator.generate(0).status(), "CREATE -> active");
        assertEquals("archived", generator.generate(1).status(), "UPDATE -> archived");
        assertEquals("removed", generator.generate(2).status(), "DELETE -> removed");
    }

    @Test
    void generatesUniqueEventIdsAndKeysAcrossManyCalls() {
        Set<String> eventIds = new HashSet<>();
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            CommitLogEvent event = generator.generate(i);
            eventIds.add(event.eventId());
            keys.add(event.key());
        }
        assertEquals(200, eventIds.size(), "event_id must be unique per event");
        // keys are 4 hex chars = 65536 possibilities, collisions across 200 draws are
        // statistically near-impossible but not mathematically guaranteed -- assert
        // "mostly unique" rather than "all unique" to avoid a flaky test.
        assertTrue(keys.size() > 190, "keys should be unique in the overwhelming majority of cases");
    }

    @Test
    void handlesLargeIndexCyclesWithoutBreaking() {
        // Guards against int overflow / modulo edge cases at the top of the range --
        // a long-running producer (--count near Integer.MAX_VALUE) shouldn't crash
        // or silently misbehave once the index gets large.
        assertDoesNotThrow(() -> generator.generate(Integer.MAX_VALUE));
        assertDoesNotThrow(() -> generator.generate(Integer.MAX_VALUE - 1));

        // 2147483647 % 3 == 1 -> OP_TYPES[1] == "UPDATE" -- locks in the exact
        // cycling behavior at the boundary, not just "didn't throw"
        assertEquals("UPDATE", generator.generate(Integer.MAX_VALUE).opType());
    }
}