package com.dr.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ArgsTest {

    @Test
    void parsesCountAndUsesDefaultsForTheRest() {
        Args args = Args.parse(new String[]{"--count", "10"});
        assertEquals(10, args.count);
        assertEquals("localhost:9092", args.bootstrapServers);
        assertEquals("commit-log", args.topic);
    }

    @Test
    void parsesAllThreeFlags() {
        Args args = Args.parse(new String[]{
                "--count", "500", "--topic", "my-topic", "--bootstrap-servers", "broker:9092"});
        assertEquals(500, args.count);
        assertEquals("my-topic", args.topic);
        assertEquals("broker:9092", args.bootstrapServers);
    }

    @Test
    void missingCountIsRejected() {
        // --count is the one flag the assignment explicitly requires -- there is no
        // sensible default for "how many messages," so omitting it is a usage error,
        // not a fallback-to-default situation.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{}));
        assertTrue(ex.getMessage().toLowerCase().contains("count"));
    }

    @Test
    void nonPositiveCountIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count", "-5"}));
    }

    @Test
    void unknownFlagIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count", "10", "--bogus", "x"}));
    }

    @Test
    void rejectsMalformedCount() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count", "not-a-number"}));
        assertTrue(ex.getMessage().toLowerCase().contains("count"),
                "error should mention which flag was malformed, not just relay a raw parse error");
    }

    @Test
    void rejectsDanglingFlags() {
        // --count with nothing after it
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count"}));
        assertTrue(ex1.getMessage().contains("--count"));

        // trailing flag with no value at the end of the array
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
                () -> Args.parse(new String[]{"--count", "10", "--topic"}));
        assertTrue(ex2.getMessage().contains("--topic"));
    }
}