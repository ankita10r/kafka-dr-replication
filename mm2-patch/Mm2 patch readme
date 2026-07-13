# mm2-patch/

# mm2-patch/ (Task 2)

Fault-tolerance patch for MirrorMaker 2: a fail-fast rule for silent
truncation and a self-healing rule for topic delete/recreate. Full design
reasoning, including three real bugs found and fixed by testing against a
live cluster, is in [`../docs/DESIGN.md`](../docs/DESIGN.md).

**What it does:**
- `auto.offset.reset=none` turns MM2's silent data-loss-by-default
  behavior into a catchable exception.
- A topic-UUID comparison (`ReplicationFailureClassifier`, unit-tested in
  isolation) tells a genuine retention-truncation apart from a topic
  delete+recreate.
- Truncation → logs the gap, throws `MirrorDataLossException`, task fails
  fast.
- Reset → logs it, resubscribes from offset 0 automatically. Detection is
  proactive (checked at every task startup, before seeking to any stored
  offset), not just reactive to an exception — see `DESIGN.md` for why
  that distinction turned out to matter.

## Two different things live in this folder

- **`0001`–`0004` `.patch` files** — apply these to a separate clone of my
  Kafka fork with `git apply`. This is the only place they actually do
  anything.
- **`main/` and `test/` `.java` files** — the same content as the patches,
  kept here as plain, readable source for review. They will **not**
  compile inside this repo (this project's classpath doesn't include
  Kafka's `connect:mirror` module) — that's expected, not a bug. The real
  compile and test run happens inside the fork, after the patches are
  applied there.

## Apply to the fork
```bash
cd kafka   # your fork, on feature/mm2-fault-tolerance
git apply /path/to/mm2-patch/0001-MirrorDataLossException.patch
git apply /path/to/mm2-patch/0002-ReplicationFailureClassifier.patch
git apply /path/to/mm2-patch/0003-ReplicationFailureClassifierTest.patch
git apply /path/to/mm2-patch/0004-MirrorSourceTask-fault-tolerance.patch
```

## Test execution
```bash
./gradlew :connect:mirror:test --tests "org.apache.kafka.connect.mirror.ReplicationFailureClassifierTest"
./gradlew :connect:mirror:test --tests "org.apache.kafka.connect.mirror.MirrorSourceTaskTest"
./gradlew :connect:mirror:compileJava
```
5 classifier tests (pure logic, no broker) plus Kafka's own pre-existing
`MirrorSourceTaskTest` suite, to confirm the patch didn't break anything
already there.

## Functional verification

All three scenarios (normal replication, truncation fail-fast, topic reset
recovery) were run against a real two-cluster Docker stack and passed, with
exact offset-count proof at each step, not just clean-looking logs. Full
commands are in [`../docs/MANUAL_TESTING.md`](../docs/MANUAL_TESTING.md).

## AI usage (Task 2 specific)

- Helped me understand why `.patch` files, not raw `.java` files, are the
  right way to submit fork changes, and why for this project specifically.
- Helped me understand why two separate repos are needed — one for the
  fork (where the patch actually applies and compiles), one for my
  submission (where the reference copies, docs, and everything else
  live) — and how to keep the two in sync without mixing them up.
- Helped with the git steps for getting the correct Kafka version — the
  fork covers Kafka's whole history, and the assignment specifically
  needed the `4.0.0` tag checked out, not just the default branch.
- Helped with branch naming and the actual `git apply` steps for the
  patch.
- For this task, AI wrote the patch files more directly than Task 1's code
  (where I wrote it myself and had AI review and fill gaps) — I verified
  correctness by actually running it against a real two-cluster Docker
  stack, which is where we found and fixed three real bugs together, not
  by reading the code alone.
- Helped with the Docker build/run steps for testing the images.

See `docs/DESIGN.md` for the full reasoning.
