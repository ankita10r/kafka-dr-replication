package com.dr.producer;

import com.dr.producer.model.CommitLogEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EventJsonMapperTest {

    private final EventJsonMapper mapper = new EventJsonMapper();
    private final ObjectMapper parser = new ObjectMapper();

    @Test
    void serializesExactlyTheFieldsInTheAssignmentSchema() throws Exception {
        CommitLogEvent event = new CommitLogEvent(
                "a8a1c867-05c3-4d43-9884-f7b55f1f0a7c", 1724684407L, "UPDATE", "doc:8f7b", "archived");

        String json = mapper.toJson(event);
        JsonNode root = parser.readTree(json);

        assertEquals("a8a1c867-05c3-4d43-9884-f7b55f1f0a7c", root.get("event_id").asText());
        assertEquals(1724684407L, root.get("timestamp").asLong());
        assertEquals("UPDATE", root.get("op_type").asText());
        assertEquals("doc:8f7b", root.get("key").asText());
        assertEquals("archived", root.get("value").get("status").asText());
    }

    @Test
    void doesNotAddFieldsBeyondTheDocumentedSchema() throws Exception {
        CommitLogEvent event = new CommitLogEvent("id", 1L, "CREATE", "doc:0000", "active");
        JsonNode root = parser.readTree(mapper.toJson(event));

        Set<String> topLevelFields = Set.of("event_id", "timestamp", "op_type", "key", "value");
        Iterator<String> actualFields = root.fieldNames();
        int count = 0;
        while (actualFields.hasNext()) {
            String field = actualFields.next();
            assertTrue(topLevelFields.contains(field), "unexpected extra field: " + field);
            count++;
        }
        assertEquals(5, count, "schema has exactly 5 top-level fields");

        JsonNode value = root.get("value");
        assertEquals(1, value.size(), "value should contain only 'status' per the schema example");
    }

    @Test
    void correctlyEscapesSpecialCharacters() throws Exception {
        // Deliberately hostile input: quotes, backslashes, a newline, and non-ASCII/emoji.
        // Our own generator never produces these today, but the mapper shouldn't assume
        // that -- it should rely on Jackson's escaping, not manual string concatenation.
        CommitLogEvent event = new CommitLogEvent(
                "id-with-\"quotes\"-and-\\backslash\\-and-\nnewline",
                1L, "CREATE", "doc:0000", "unicode caf\u00e9 \uD83D\uDE00");

        String json = mapper.toJson(event);

        JsonNode root = parser.readTree(json);   // fails to parse at all if escaping is broken
        assertEquals(event.eventId(), root.get("event_id").asText());
        assertEquals(event.status(), root.get("value").get("status").asText());
    }
}