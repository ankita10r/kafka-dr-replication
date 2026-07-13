# Kafka DR Replication Project

> **Status: Tasks 1 & 2 of 3 complete.**

A disaster-recovery replication pipeline for Kafka: a synthetic event
producer, a fault-tolerance patch for MirrorMaker 2, and the Docker
environment to run both end-to-end.

## Task breakdown

I split this into three tasks and I'm verifying each one before moving to
the next.

1. **Fake-data generator** — Java CLI, `--count N` produces exactly N fake
   JSON commit-log events to the primary cluster. **✅ Done** — see
   [`producer/README.md`](producer/README.md).
2. **Patch MirrorMaker 2** — fork Kafka, add a fail-fast rule for silent
   truncation and a self-healing rule for topic delete/recreate. **✅
   Done, verified against a real two-cluster Docker stack** — see
   [`mm2-patch/README.md`](mm2-patch/README.md).
3. **Automate and package** — Docker Hub images, `docker-compose.yml`,
   `run_challenge.sh`, final README. **⬜ Not started** — verification
   happened, but `docker/` and `scripts/` aren't committed to this repo
   yet (coming next).

Full design reasoning and every real bug found along the way lives in
[`docs/DESIGN.md`](docs/DESIGN.md).

---

## 1. Repository links

| What | Link |
|---|---|
| This repo | you're in it |
| Kafka fork (Tasks 2/3) | not yet pushed |
| PR against `apache/kafka` | not opened yet |

## 2. Docker Hub images

Not published yet — `producer/Dockerfile` is buildable but unpushed; the
MM2 image builds successfully (verified locally via Docker Compose) but
hasn't been pushed to a registry.

## 3. Setup & test execution

Per-task setup, test commands, and manual verification steps:

- **Task 1 (producer):** [`producer/README.md`](producer/README.md)
- **Task 2 (MM2 patch):** [`mm2-patch/README.md`](mm2-patch/README.md)
- **Full 3-scenario walkthrough:** [`docs/MANUAL_TESTING.md`](docs/MANUAL_TESTING.md)

## 4. Log analysis

Covered per-task in the docs linked above — the key lines to watch for are
`Detected unrecoverable data loss` (truncation) and `Detected topic reset`
(self-healing), both in MM2's logs.

---

## AI usage

I used Claude to help structure and build this, and cross-checked some of
its suggestions with Gemini before accepting them, particularly on design
tradeoffs.

**General, across both tasks:**
- Used it to turn my high-level idea into an ordered plan — splitting the
  assignment into the three tasks above.
- I'd assumed the PR alone was the deliverable; it pointed out the README
  also needs the fork link.
- Helped with the Docker/Compose setup and local Kafka testing from
  IntelliJ.
- Helped design test cases per task to keep this TDD, and tightened
  commit messages and README wording.
- Modified an early, crudely-written README into a proper one.
- Used Gemini separately to sanity-check some of the above rather than
  taking one tool's output as final.

**Task 2 specific** (full list in [`mm2-patch/README.md`](mm2-patch/README.md)):
- Explained why `.patch` files, not raw `.java`, are the right way to
  submit fork changes, and why two separate repos (fork vs. submission)
  are needed.
- Helped with git steps for checking out the correct Kafka version
  (`4.0.0` tag) and branch naming.
- For this task, AI wrote the patch files more directly than Task 1's
  code — I verified correctness by running it against a real cluster,
  where we found and fixed three real bugs together.

---

## Roadmap

- [x] Apply MM2 patch to the Kafka fork, verify it compiles
- [x] Run all three test scenarios against the real cluster
- [ ] Commit `docker/` and `scripts/run_challenge.sh` to this repo
- [ ] Push both Docker Hub images
- [ ] Open the PR
- [ ] Fill in links above
