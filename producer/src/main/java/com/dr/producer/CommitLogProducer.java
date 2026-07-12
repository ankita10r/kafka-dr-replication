package com.dr.producer;

import com.dr.producer.model.CommitLogEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * CLI application that generates synthetic JSON commit-log events and writes
 * them to a Kafka topic.
 *
 * Usage:
 *   java -jar commit-log-producer.jar --count 1000 \
 *        --bootstrap-servers kafka-primary:9092 --topic commit-log
 */
public class CommitLogProducer {

    private static final Logger log = LoggerFactory.getLogger(CommitLogProducer.class);

    public static void main(String[] args) {
        Args parsed = Args.parse(args);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parsed.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        log.info("Starting CommitLogProducer: topic={}, count={}, bootstrap={}",
                parsed.topic, parsed.count, parsed.bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            produceEvents(producer, parsed.topic, parsed.count, new EventGenerator(), new EventJsonMapper());
        }

        log.info("Done. Produced {} messages to topic '{}'.", parsed.count, parsed.topic);
    }

    /**
     * The actual send loop, deliberately taking the {@link Producer} interface (not
     * the concrete {@link KafkaProducer}) so tests can pass Kafka's {@code MockProducer}
     * instead -- verifying send behavior without a live broker.
     */
    static void produceEvents(Producer<String, String> producer, String topic, int count,
                               EventGenerator generator, EventJsonMapper jsonMapper) {
        for (int i = 0; i < count; i++) {
            CommitLogEvent event = generator.generate(i);
            String json = jsonMapper.toJson(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.key(), json);

            try {
                RecordMetadata metadata = producer.send(record).get();
                if ((i + 1) % 100 == 0 || i == count - 1) {
                    log.info("Produced {}/{} messages (last offset={}, partition={})",
                            i + 1, count, metadata.offset(), metadata.partition());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while sending event " + event.eventId(), e);
            } catch (ExecutionException e) {
                log.error("Failed to send message {} (event_id={}): {}", i, event.eventId(), e.getMessage(), e);
                throw new IllegalStateException("Failed to send event " + event.eventId(), e);
            }
        }
        producer.flush();
    }
}
