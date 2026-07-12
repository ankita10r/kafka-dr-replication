# Kafka DR Replication Project

> **Status: Task 1 of 3 complete.**

A disaster-recovery replication pipeline for Kafka: a synthetic event
producer, a fault-tolerance patch for MirrorMaker 2, and the Docker
environment to run both end-to-end.

## Task breakdown

I split this into three tasks and I'm verifying each one before moving to
the next.

1. **Fake-data generator** — Java CLI, `--count N` produces exactly N fake
   JSON commit-log events to the primary cluster. **✅ Done.**
2. **Patch MirrorMaker 2** — fork Kafka, add a fail-fast rule for silent
   truncation and a self-healing rule for topic delete/recreate. **⬜ Not
   started.**
3. **Automate and package** — Docker Hub images, `docker-compose.yml`,
   `run_challenge.sh`, final README. **⬜ Not started** beyond an unverified
   compose draft.

---

## 1. Repository links

| What | Link |
|---|---|
| This repo | you're in it |
| Kafka fork (Tasks 2/3) | not yet pushed |
| PR against `apache/kafka` | not opened yet |

## 2. Docker Hub images

Not published yet — `producer/Dockerfile` is buildable but unpushed;
the MM2 image is blocked on Task 2/3.

## 3. Task 1 — Commit Log Producer

Java/Gradle CLI, built test-first. Source: [`producer/`](producer/).

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

### Setup
```bash
cd producer
./gradlew build
```

### Test execution
```bash
cd producer
./gradlew test
```
20 tests, 4 classes, all passing, no broker needed — send-loop tests use
Kafka's `MockProducer`. Report: `producer/build/reports/tests/test/index.html`

### Manual verification against a real local broker

I also ran this against an actual Kafka container to confirm the wiring
(bootstrap address, serializers) works, not just the mocked logic:

```bash
# one-off broker
docker run -d --rm --name kafka-quick -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:4.0.0

# run the producer against it
./gradlew run --args="--count 10 --bootstrap-servers localhost:9092 --topic commit-log"

# confirm the messages actually landed
docker exec -it kafka-quick /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic commit-log --from-beginning --max-messages 10

# clean up
docker stop kafka-quick
```

### Log analysis

- `Starting CommitLogProducer: topic=..., count=..., bootstrap=...` on startup
- `Produced N/count messages (last offset=..., partition=...)` every 100 messages
- `Failed to send message ... (event_id=...)` at ERROR — only on a real send failure, immediately followed by the process throwing and stopping

### Design rationale

- Event generation (`EventGenerator`/`EventJsonMapper`) is kept separate from
  Kafka I/O so the send loop is unit-testable with `MockProducer` instead of
  needing a live broker for every run.
- `--count` has no default — it's required by the assignment, so a silent
  default would hide a usage mistake.
- Send failures throw and stop rather than log-and-continue — the spec asks
  for exactly `--count` messages; silently under-delivering would defeat
  that.
- `acks=all` + idempotence on, since this producer is modeling a
  write-ahead log and durability mattered more than throughput here.



### AI usage
*(This is a running list for Task 1 only, will be updated as I code.)*

I used Claude to help structure and build this, and cross-checked some of
its suggestions with Gemini before accepting them, particularly on design
tradeoffs.

- Used it to turn my high-level idea into an ordered plan — splitting the assignment into the three tasks above.
- Helped with the Docker/Compose setup.
- Helped me get local Kafka running and test against it from IntelliJ.
- Helped tighten commit messages and this README's wording.
- Helped design test cases per task to keep this TDD.
- Used Gemini separately to sanity-check some of the above rather than
  taking one tool's output as final.

---

## Roadmap

- [ ] Apply MM2 patch to the Kafka fork, verify it compiles
- [ ] Docker Compose end-to-end
- [ ] Push both Docker Hub images
- [ ] `run_challenge.sh`
- [ ] Run all three test scenarios against the real cluster
- [ ] Open the PR
- [ ] Fill in links above
