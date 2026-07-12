package com.dr.producer;

import com.dr.producer.model.CommitLogEvent;

import java.time.Instant;
import java.util.UUID;

/**
 * Pure event-generation logic, deliberately kept free of any Kafka or I/O
 * dependency so it can be unit tested without a broker.
 */
public class EventGenerator {

    private static final String[] OP_TYPES = {"CREATE", "UPDATE", "DELETE"};
    private static final String[] STATUSES = {"active", "archived", "removed"};

    public CommitLogEvent generate(int index) {
        int cycle = index % OP_TYPES.length;
        String eventId = UUID.randomUUID().toString();
        String key = "doc:" + UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        long timestamp = Instant.now().getEpochSecond();

        return new CommitLogEvent(eventId, timestamp, OP_TYPES[cycle], key, STATUSES[cycle]);
    }
}
