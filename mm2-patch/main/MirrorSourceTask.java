/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    // --- Fault-tolerance additions -----------------------------------------
    // Admin client used purely to fetch each source topic's UUID, which lets us
    // tell a genuine delete+recreate (topic ID changes) apart from ordinary
    // retention-based truncation (topic ID stays the same, only old offsets vanish).
    private Admin sourceAdmin;
    private final Map<String, Uuid> knownTopicIds = new HashMap<>();
    private final ReplicationFailureClassifier failureClassifier = new ReplicationFailureClassifier();
    // -------------------------------------------------------------------------

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy, OffsetSyncWriter offsetSyncWriter, Admin sourceAdmin) {
        this(consumer, metrics, sourceClusterAlias, replicationPolicy, offsetSyncWriter);
        this.sourceAdmin = sourceAdmin;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }

        // Force explicit truncation detection: with the default "earliest", a consumer
        // whose next offset has been purged by retention would silently jump forward and
        // the resulting gap would never be reported. "none" makes the consumer throw
        // OffsetOutOfRangeException instead, which we handle explicitly below.
        Map<String, Object> consumerConfig = config.sourceConsumerConfig("replication-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumer = MirrorUtils.newConsumer(consumerConfig);

        if (sourceAdmin == null) {
            sourceAdmin = Admin.create(config.sourceAdminConfig("fault-tolerance-admin"));
        }

        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);
        primeKnownTopicIds(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        // Handle delayed and pending offset syncs only when offsetSyncWriter is available
        if (offsetSyncWriter != null) {
            // Offset syncs which were not emitted immediately due to their offset spacing should be sent periodically
            // This ensures that low-volume topics aren't left with persistent lag at the end of the topic
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            // Publish any offset syncs that we've queued up, but have not yet been able to publish
            // (likely because we previously reached our limit for number of outstanding syncs)
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(sourceAdmin, "source admin client");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (NoOffsetForPartitionException e) {
            // No committed offset exists yet for these partitions, and auto.offset.reset=none
            // gives no default to fall back on. This is expected the first time MM2 ever
            // replicates a partition -- nothing has been produced through this pipeline yet,
            // so there's nothing to have lost. Vanilla MM2 (default auto.offset.reset=earliest)
            // would have silently started from the beginning here; we do the same thing
            // explicitly instead of leaving it to a config default, and log it so it's visible
            // in the logs that this was a first-run start, not a swallowed error.
            log.info("No previously committed offset for {} -- first replication run for "
                    + "these partitions. Starting from the beginning.", e.partitions());
            consumer.seekToBeginning(e.partitions());
            return null;
        } catch (OffsetOutOfRangeException e) {
            // Either (a) retention purged data we hadn't replicated yet [truncation -> fail
            // fast], or (b) the topic was deleted and recreated [reset -> resubscribe from
            // the beginning]. handleOffsetOutOfRange tells the two apart and throws
            // MirrorDataLossException itself for case (a).
            handleOffsetOutOfRange(e);
            return null;
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        // Queue offset syncs only when offsetWriter is available
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            // We may be able to immediately publish an offset sync that we've queued up here
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    // --- Fault-tolerance additions -------------------------------------------------------

    /** Caches each assigned topic's current UUID so future resets (recreate) can be detected. */
    private void primeKnownTopicIds(Set<TopicPartition> topicPartitions) {
        Set<String> topics = topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
        fetchTopicIds(topics).forEach(knownTopicIds::put);
    }

    private Map<String, Uuid> fetchTopicIds(Set<String> topics) {
        Map<String, Uuid> result = new HashMap<>();
        if (topics.isEmpty()) {
            return result;
        }
        try {
            Map<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> futures =
                    sourceAdmin.describeTopics(topics).topicNameValues();
            for (Map.Entry<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    result.put(entry.getKey(), entry.getValue().get().topicId());
                } catch (ExecutionException | InterruptedException e) {
                    log.warn("Could not fetch topic ID for {} while checking for topic reset.", entry.getKey(), e);
                }
            }
        } catch (KafkaException e) {
            log.warn("Could not describe topics {} while checking for topic reset.", topics, e);
        }
        return result;
    }

    /**
     * Distinguishes truncation from a topic delete+recreate for every partition named in the
     * exception, and either fails fast (truncation) or resubscribes from the beginning (reset).
     * The actual classification decision is delegated to {@link ReplicationFailureClassifier}
     * (unit-tested in isolation) -- this method owns the I/O and side effects around that
     * decision (fetching UUIDs, logging, seeking, throwing).
     */
    private void handleOffsetOutOfRange(OffsetOutOfRangeException e) {
        Set<TopicPartition> affected = e.offsetOutOfRangePartitions().keySet();
        Set<String> affectedTopics = affected.stream().map(TopicPartition::topic).collect(Collectors.toSet());
        Map<String, Uuid> currentTopicIds = fetchTopicIds(affectedTopics);

        List<TopicPartition> resetPartitions = new ArrayList<>();

        for (TopicPartition tp : affected) {
            long requestedOffset = e.offsetOutOfRangePartitions().get(tp);
            Uuid previousId = knownTopicIds.get(tp.topic());
            Uuid currentId = currentTopicIds.get(tp.topic());

            ReplicationFailureClassifier.Decision decision = failureClassifier.classify(previousId, currentId);

            if (decision == ReplicationFailureClassifier.Decision.TOPIC_RESET) {
                log.warn("Detected topic reset for {}-{}: topic ID changed from {} to {} "
                                + "(topic was deleted and recreated) at {}. Resubscribing from the beginning offset.",
                        tp.topic(), tp.partition(), previousId, currentId, Instant.now());
                resetPartitions.add(tp);
                knownTopicIds.put(tp.topic(), currentId);
            } else {
                // Same topic identity, or we couldn't establish one/both IDs: the offset we
                // needed is simply gone and we can't prove this was a benign reset. That's an
                // undetected replication gap -- fail fast rather than silently skip ahead.
                long earliest = safeBeginningOffset(tp);
                log.error("Detected unrecoverable data loss for {}-{}: next required offset {} is no longer "
                                + "available on the source cluster (earliest available offset is now {}). "
                                + "This indicates retention purged data before it was replicated. Failing fast.",
                        tp.topic(), tp.partition(), requestedOffset, earliest);
                throw new MirrorDataLossException(String.format(
                        "Data loss detected on %s-%s: required offset %d but earliest available is %d. "
                                + "Data was purged by retention before MirrorMaker 2 replicated it.",
                        tp.topic(), tp.partition(), requestedOffset, earliest));
            }
        }

        if (!resetPartitions.isEmpty()) {
            consumer.seekToBeginning(resetPartitions);
        }
    }

    private long safeBeginningOffset(TopicPartition tp) {
        try {
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(Collections.singletonList(tp));
            Long offset = beginning.get(tp);
            return offset == null ? -1L : offset;
        } catch (KafkaException e) {
            log.warn("Could not fetch beginning offset for {} while reporting data loss.", tp, e);
            return -1L;
        }
    }

    // ---------------------------------------------------------------------------------------

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            // Do not call seek on partitions that don't have an existing offset committed.
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
        });
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}
