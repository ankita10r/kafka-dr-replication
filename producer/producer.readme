# Commit Log Producer (Task 1)

Java/Gradle CLI, built test-first. See [`../docs/DESIGN.md`](../docs/DESIGN.md)
for the full design reasoning.

**Schema produced:**
```json
{
  "event_id": "a8a1c867-05c3-4d43-9884-f7b55f1f0a7c",
  "timestamp": 1724684407,
  "op_type": "UPDATE",
  "key": "doc:8f7b",
  "value": { "status": "archived" }
}
```

## Setup
```bash
./gradlew build
```

## Test execution
```bash
./gradlew test
```
20 tests, 4 classes, all passing, no broker needed — send-loop tests use
Kafka's `MockProducer`. Report: `build/reports/tests/test/index.html`

## Manual verification against a real local broker

```bash
docker run -d --rm --name kafka-quick -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:4.0.0

./gradlew run --args="--count 10 --bootstrap-servers localhost:9092 --topic commit-log"

docker exec -it kafka-quick /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic commit-log --from-beginning --max-messages 10

docker stop kafka-quick
```

## Log analysis

- `Starting CommitLogProducer: topic=..., count=..., bootstrap=...` on startup
- `Produced N/count messages (last offset=..., partition=...)` every 100 messages
- `Failed to send message ... (event_id=...)` at ERROR — only on a real send
  failure, immediately followed by the process throwing and stopping
