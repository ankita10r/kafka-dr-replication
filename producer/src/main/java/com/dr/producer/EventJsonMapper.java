package com.dr.producer;

import com.dr.producer.model.CommitLogEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serializes a CommitLogEvent to exactly the JSON shape specified in the
 * assignment's event schema -- nothing more, nothing less.
 */
public class EventJsonMapper {

    private final ObjectMapper mapper = new ObjectMapper();

    public String toJson(CommitLogEvent event) {
        ObjectNode root = mapper.createObjectNode();
        root.put("event_id", event.eventId());
        root.put("timestamp", event.timestamp());
        root.put("op_type", event.opType());
        root.put("key", event.key());

        ObjectNode value = mapper.createObjectNode();
        value.put("status", event.status());
        root.set("value", value);

        try {
            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            // ObjectNode serialization only fails on I/O errors, which can't happen
            // writing to an in-memory String -- wrapping as unchecked is appropriate here.
            throw new IllegalStateException("Failed to serialize event " + event.eventId(), e);
        }
    }
}
