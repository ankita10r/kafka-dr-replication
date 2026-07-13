# Kafka DR Replication Project

> **Status: All 3 tasks complete. PR open against apache/kafka.**

A disaster-recovery replication pipeline for Kafka: a synthetic event
producer, a fault-tolerance patch for MirrorMaker 2, and the Docker
environment to run both end-to-end.

## Task breakdown

1. **Fake-data generator** — Java CLI, `--count N` produces exactly N fake
   JSON commit-log events to the primary cluster. **✅ Done** — see
   [`producer/README.md`](producer/README.md).
2. **Patch MirrorMaker 2** — fork Kafka, add a fail-fast rule for silent
   truncation and a self-healing rule for topic delete/recreate. **✅
   Done, verified against a real two-cluster Docker stack** — see
   [`mm2-patch/README.md`](mm2-patch/README.md).
3. **Automate and package** — Docker Compose stack, `run_challenge.sh`,
   Docker Hub images. **✅ Done, 6/6 scenarios passing** — see
   [`scripts/README.md`](scripts/README.md).

Full design reasoning, including three real bugs found and fixed via
testing, is in [`docs/DESIGN.md`](docs/DESIGN.md).

---

## 1. Repository links

| What | Link |
|---|---|
| This repo | you're in it |
| Kafka fork | https://github.com/ankita10r/kafka, branch `feature/mm2-fault-tolerance` |
| Pull request against `apache/kafka` | https://github.com/apache/kafka/pull/22819 |

## 2. Docker Hub images

| Image | Link |
|---|---|
| Enhanced MirrorMaker 2 | [ankitafynarfin/mm2-enhanced](https://hub.docker.com/r/ankitafynarfin/mm2-enhanced) |
| Commit Log Producer | [ankitafynarfin/commit-log-producer](https://hub.docker.com/r/ankitafynarfin/commit-log-producer) |

## 3. Setup & test execution

- **Task 1 (producer):** [`producer/README.md`](producer/README.md)
- **Task 2 (MM2 patch):** [`mm2-patch/README.md`](mm2-patch/README.md)
- **Task 3 (automation):** [`scripts/README.md`](scripts/README.md)
- **Full 3-scenario walkthrough (manual):** [`docs/MANUAL_TESTING.md`](docs/MANUAL_TESTING.md)

## 4. Log analysis

Key lines to watch for in MM2's logs: `Detected unrecoverable data loss`
(truncation) and `Detected topic reset` (self-healing). Full detail in the
per-task docs linked above.

---

## AI usage

I used Claude to help structure and build this, and cross-checked some of
its suggestions with Gemini before accepting them, particularly on design
tradeoffs.

**General, across all tasks:**
- Used it to turn my high-level idea into an ordered plan — splitting the
  assignment into the three tasks above.
- I'd assumed the PR alone was the deliverable; it pointed out the README
  also needs the fork link.
- Helped with the Docker/Compose setup and local Kafka testing from
  IntelliJ.
- Helped design test cases per task to keep this TDD, and tightened
  commit messages and README wording.
- Used Gemini separately to sanity-check some of the above rather than
  taking one tool's output as final.

**Task 2 specific** (full list in [`mm2-patch/README.md`](mm2-patch/README.md)):
- Explained why `.patch` files, not raw `.java`, are the right way to
  submit fork changes, and why two separate repos (fork vs. submission)
  are needed.
- For this task, AI wrote the patch files more directly than Task 1's
  code — I verified correctness by running it against a real cluster,
  where we found and fixed three real bugs together.
- Helped debug and fix the code through manual testing of all three
  scenarios before the automation script was written — that's where the
  three real bugs actually surfaced and got fixed.

**Task 3 specific:**
- Wrote the first draft of `run_challenge.sh`, since I'm not confident
  with bash — I ran it and confirmed the 6/6 pass result myself.

**Getting the PR open:**
- Helped untangle a local git issue (a corrupted clone, then a squashed
  commit history, then a wrong-commit cherry-pick) that came up while
  rebasing onto the current `4.0` branch before the PR could be opened
  cleanly.

---

## Roadmap

- [x] Apply MM2 patch to the Kafka fork, verify it compiles
- [x] Run all three test scenarios against the real cluster
- [x] Commit `docker/` and `scripts/run_challenge.sh` to this repo
- [x] Push both Docker Hub images
- [x] Open the PR
