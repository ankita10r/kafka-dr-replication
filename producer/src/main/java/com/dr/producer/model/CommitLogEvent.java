package com.dr.producer.model;

/**
 * A single commit-log event. Immutable by design -- once generated, an event
 * never changes, matching the write-ahead-log semantics described in the
 * assignment (events are appended, never mutated).
 */
public record CommitLogEvent(String eventId, long timestamp, String opType, String key, String status) {
}
