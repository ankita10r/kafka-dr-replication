# MANUAL_TESTING.md

*Note: You can use `scripts/run_challenge.sh` to do all this automatically now, but I’m keeping these notes around for when I need to debug by hand.*

**Setup**
```bash
cd docker
docker compose up -d kafka-primary kafka-standby kafka-init
docker compose logs -f kafka-init   # wait for "commit-log topic ready.", Ctrl+C
docker compose up -d mm2
```

**Scenario 1: Normal Replication**
Just a sanity check. I fire off 100 events from the producer and check the DR cluster to make sure the offset matches exactly what I sent. If the numbers match, we're good.
```bash
docker compose run --rm producer --count 100 --bootstrap-servers kafka-primary:19092 --topic commit-log
docker exec -it kafka-standby /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-standby:19092 --topic primary.commit-log --time -1
```

**Scenario 2: Data Loss (The "Fail-Fast" Test)**
I stop MM2, send 50 events, and wait 90 seconds (longer than the 60s retention time). The primary cluster purges the data. When I turn MM2 back on, it *must* crash with a "Data loss detected" error. If it just keeps running, the test fails.
```bash
docker compose stop mm2
docker compose run --rm producer --count 50 --bootstrap-servers kafka-primary:19092 --topic commit-log
sleep 90

docker exec -it kafka-primary /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-primary:19092 --topic commit-log --time -2
# earliest offset should now be > 0 -- confirms retention actually purged something

docker compose start mm2
docker compose logs mm2 | grep -i "data loss"
```
To prove it actually stopped and isn't just logging a warning while carrying on, I send a few more events and check the DR offset didn't move:
```bash
docker compose run --rm producer --count 10 --bootstrap-servers kafka-primary:19092 --topic commit-log
docker exec -it kafka-standby /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-standby:19092 --topic primary.commit-log --time -1
```

**Scenario 3: Topic Reset (The "Self-Healing" Test)**
This one’s fun. I stop MM2, delete the topic on the primary cluster, and recreate it. Then, I send 20 new events to the fresh topic and restart MM2. It should notice the topic ID changed, log a "Detected topic reset" message, and automatically go back to offset 0.
```bash
docker compose restart mm2   # recovers from scenario 2 -- it'll fail again immediately, that's expected

docker compose stop mm2
docker exec -it kafka-primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-primary:19092 --delete --topic commit-log
sleep 5
docker exec -it kafka-primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-primary:19092 --create --topic commit-log \
  --partitions 1 --replication-factor 1 --config retention.ms=60000

docker compose run --rm producer --count 20 --bootstrap-servers kafka-primary:19092 --topic commit-log
docker compose start mm2
docker compose logs mm2 | grep -i "topic reset"
```
DR offset should grow by exactly 20 once it catches up. I also ran this scenario twice back-to-back once, since that's actually the case that broke my first version (old offset happened to still line up with the new topic's size, so nothing threw at all)—same result both times.

**Quick Reference Cheat Sheet**

| Action | Command |
|---|---|
| Check status | `docker compose ps` |
| Peek at logs | `docker compose logs mm2` |
| Check offsets | `kafka-get-offsets.sh --bootstrap-server <host> --topic <t> --time -2` (earliest) / `-1` (latest) |
| Clean slate | `docker compose down -v` |
