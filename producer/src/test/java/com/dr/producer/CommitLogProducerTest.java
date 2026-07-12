package com.dr.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogProducerTest {

    private final ObjectMapper json = new ObjectMapper();

    @Test
    void sendsExactlyCountRecordsToTheConfiguredTopic() throws Exception {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());

        CommitLogProducer.produceEvents(mockProducer, "commit-log", 25,
                new EventGenerator(), new EventJsonMapper());

        List<ProducerRecord<String, String>> sent = mockProducer.history();
        assertEquals(25, sent.size(), "should send exactly --count records, no more, no less");

        for (ProducerRecord<String, String> record : sent) {
            assertEquals("commit-log", record.topic());
            assertNotNull(record.key(), "every record should carry a key");
            JsonNode body = json.readTree(record.value());
            assertTrue(body.has("event_id"));
            assertTrue(body.has("timestamp"));
            assertTrue(body.has("op_type"));
        }

        mockProducer.close();
    }

    @Test
    void stopsAfterExactlyZeroRecordsIfCountIsZeroGuardIsBypassedInternally() {
        // produceEvents itself doesn't re-validate count>0 -- that's Args' job (see
        // ArgsTest). This test documents that boundary: produceEvents trusts its input.
        MockProducer<String, String> mockProducer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());

        CommitLogProducer.produceEvents(mockProducer, "commit-log", 0,
                new EventGenerator(), new EventJsonMapper());

        assertEquals(0, mockProducer.history().size());
        mockProducer.close();
    }

    @Test
    void throwsAndStopsOnBrokerFailureRatherThanSilentlyContinuing() {
        // Design decision: the assignment requires producing EXACTLY --count messages.
        // If a send fails partway through and we logged-and-continued, we'd silently
        // deliver fewer than --count with no signal -- exactly the silent-failure
        // pattern Task 2 exists to prevent, one layer up the stack. So: fail fast,
        // stop immediately, and let the exit code/exception make the shortfall loud.
        FailingProducer failingProducer = new FailingProducer(2); // fails on the 3rd send

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                CommitLogProducer.produceEvents(failingProducer, "commit-log", 5,
                        new EventGenerator(), new EventJsonMapper()));
        assertNotNull(ex.getCause(), "original broker exception should be preserved as the cause");

        // Exactly 2 successful sends should have gone through before the failure --
        // proves it stopped immediately rather than skipping the bad one and pressing on.
        assertEquals(2, failingProducer.history().size());
    }

    /**
     * A MockProducer that fails one specific send (by call order) with a broker-style
     * exception, then behaves normally for any sends before it. Avoids needing threads
     * or a real broker to test the failure path.
     */
    private static class FailingProducer extends MockProducer<String, String> {
        private final int failAtCallIndex;
        private int callCount = 0;

        FailingProducer(int failAtCallIndex) {
            super(true, null, new StringSerializer(), new StringSerializer());
            this.failAtCallIndex = failAtCallIndex;
        }

        @Override
        public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<String, String> record) {
            if (callCount++ == failAtCallIndex) {
                java.util.concurrent.CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> failed =
                        new java.util.concurrent.CompletableFuture<>();
                failed.completeExceptionally(
                        new org.apache.kafka.common.errors.TimeoutException("simulated broker failure"));
                return failed;
            }
            return super.send(record);
        }
    }
}