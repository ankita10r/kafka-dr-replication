#!/usr/bin/env bash
#
# run_challenge.sh-- orchestrates the three DR replication test scenarios
# against the Docker Compose stack (docker/docker-compose.yml):
#   1. Normal replication
#   2. Log truncation (fail-fast)
#   3. Topic reset (self-healing)
#
# This encodes exactly the commands and pass conditions verified manually
# against the real two-cluster stack -- see docs/MANUAL_TESTING.md for the
# original by-hand version and docs/DESIGN.md for why each check exists.
#
# Run from anywhere in the repo:
#   ./run_challenge.sh
#
# Exit code 0 = all scenarios passed. Non-zero = at least one failed --
# see the [FAIL] lines above the summary for which one and why.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../docker"

COMPOSE="docker compose"
PRIMARY_BOOTSTRAP="kafka-primary:19092"
STANDBY_BOOTSTRAP="kafka-standby:19092"
TOPIC="commit-log"
TARGET_TOPIC="primary.commit-log"

PASS_COUNT=0
FAIL_COUNT=0

log()  { echo -e "\033[1;34m[run_challenge]\033[0m $*"; }
pass() { echo -e "\033[1;32m[PASS]\033[0m $*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo -e "\033[1;31m[FAIL]\033[0m $*"; FAIL_COUNT=$((FAIL_COUNT + 1)); }

get_target_offset() {
    docker exec kafka-standby /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server "$STANDBY_BOOTSTRAP" --topic "$TARGET_TOPIC" --time -1 \
        2>/dev/null | awk -F: '{print $3}'
}

get_source_earliest_offset() {
    docker exec kafka-primary /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server "$PRIMARY_BOOTSTRAP" --topic "$TOPIC" --time -2 \
        2>/dev/null | awk -F: '{print $3}'
}

produce() {
    local count=$1
    $COMPOSE run --rm producer --count "$count" \
        --bootstrap-servers "$PRIMARY_BOOTSTRAP" --topic "$TOPIC" \
        > /tmp/run_challenge_producer.log 2>&1
}

wait_for_log() {
    # wait_for_log <pattern> <timeout_seconds>
    local pattern=$1
    local timeout=$2
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        if docker compose logs mm2 2>/dev/null | grep -qi "$pattern"; then
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    return 1
}

# ---------------------------------------------------------------------------
# Setup: clean stack
# ---------------------------------------------------------------------------
log "Bringing up a clean stack (docker compose down -v && up --build)..."
$COMPOSE down -v > /dev/null 2>&1
CACHEBUST=$(date +%s) $COMPOSE up -d --build

log "Waiting for kafka-init to create the commit-log topic..."
if ! wait_for_log "commit-log topic ready" 60; then
    log "WARNING: didn't see kafka-init's confirmation in time; continuing anyway"
fi
# wait_for_log greps mm2's logs, not kafka-init's -- kafka-init readiness is
# checked separately since it's a different container's log stream.
for i in $(seq 1 30); do
    if docker compose logs kafka-init 2>/dev/null | grep -q "commit-log topic ready."; then
        break
    fi
    sleep 2
done

log "Waiting for MM2 to finish starting..."
wait_for_log "replicating .* topic-partitions" 60 || \
    log "WARNING: didn't see MM2 startup confirmation in time; continuing anyway"
sleep 5

# ---------------------------------------------------------------------------
# Scenario 1: Normal replication
# ---------------------------------------------------------------------------
log "=== Scenario 1: Normal replication ==="
BEFORE=$(get_target_offset)
produce 1000
sleep 15
AFTER=$(get_target_offset)
EXPECTED=$((BEFORE + 1000))
if [ "$AFTER" -eq "$EXPECTED" ] 2>/dev/null; then
    pass "Scenario 1: replicated exactly 1000 messages ($BEFORE -> $AFTER)"
else
    fail "Scenario 1: expected offset $EXPECTED, got '$AFTER'"
fi

# ---------------------------------------------------------------------------
# Scenario 2: Log truncation (fail-fast)
# ---------------------------------------------------------------------------
log "=== Scenario 2: Log truncation (fail-fast) ==="
BEFORE=$(get_target_offset)
$COMPOSE stop mm2 > /dev/null
produce 50

log "Waiting 90s for retention (60s) to purge the unreplicated batch..."
sleep 90

EARLIEST=$(get_source_earliest_offset)
if [ "$EARLIEST" -gt 0 ] 2>/dev/null; then
    pass "Scenario 2: retention purged data (earliest offset now $EARLIEST)"
else
    fail "Scenario 2: retention did not purge anything (earliest offset still 0) -- test may be inconclusive"
fi

$COMPOSE start mm2 > /dev/null
if wait_for_log "Detected unrecoverable data loss" 30; then
    pass "Scenario 2: MirrorDataLossException correctly detected and logged"
else
    fail "Scenario 2: expected a 'Detected unrecoverable data loss' log line, not found within timeout"
fi

produce 10
sleep 10
AFTER=$(get_target_offset)
if [ "$AFTER" -eq "$BEFORE" ] 2>/dev/null; then
    pass "Scenario 2: replication correctly halted (offset stayed at $BEFORE, primary kept moving)"
else
    fail "Scenario 2: expected offset to stay at $BEFORE, got '$AFTER' -- task did not actually stop consuming"
fi

# ---------------------------------------------------------------------------
# Recover MM2 before Scenario 3
# ---------------------------------------------------------------------------
log "Restarting MM2 before Scenario 3..."
$COMPOSE restart mm2 > /dev/null
sleep 10
# MM2 will immediately re-detect the same unresolved data loss and fail again
# here -- that's correct (see DESIGN.md). Scenario 3 resolves it with an
# actual topic reset, not just a restart.

# ---------------------------------------------------------------------------
# Scenario 3: Topic reset (self-healing)
# ---------------------------------------------------------------------------
log "=== Scenario 3: Topic reset (self-healing) ==="
BEFORE=$(get_target_offset)
$COMPOSE stop mm2 > /dev/null

docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$PRIMARY_BOOTSTRAP" --delete --topic "$TOPIC"
sleep 5
docker exec kafka-primary /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$PRIMARY_BOOTSTRAP" --create --topic "$TOPIC" \
    --partitions 1 --replication-factor 1 --config retention.ms=60000

produce 20
$COMPOSE start mm2 > /dev/null

if wait_for_log "Detected topic reset" 30; then
    pass "Scenario 3: topic reset correctly detected"
else
    fail "Scenario 3: expected a 'Detected topic reset' log line, not found within timeout"
fi

sleep 10
AFTER=$(get_target_offset)
EXPECTED=$((BEFORE + 20))
if [ "$AFTER" -eq "$EXPECTED" ] 2>/dev/null; then
    pass "Scenario 3: recovered and replicated all 20 post-reset messages ($BEFORE -> $AFTER)"
else
    fail "Scenario 3: expected offset $EXPECTED, got '$AFTER'"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo
log "=== Summary: $PASS_COUNT passed, $FAIL_COUNT failed ==="
if [ "$FAIL_COUNT" -gt 0 ]; then
    log "Producer output from the last run is at /tmp/run_challenge_producer.log if needed."
    exit 1
fi
exit 0