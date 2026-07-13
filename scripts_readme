# run_challenge.sh (Task 3)

Automates all three test scenarios against the Docker Compose stack:
normal replication, truncation fail-fast, topic reset recovery. Encodes
exactly the commands and pass conditions verified manually — see
[`../docs/MANUAL_TESTING.md`](../docs/MANUAL_TESTING.md) for the by-hand
version this replaces.

## Run
```bash
cd ..   # repo root, or anywhere -- the script locates its own path
./scripts/run_challenge.sh
```
Takes ~3-4 minutes, mostly the 90s truncation wait. Brings the stack up
clean (`down -v` + rebuild) before starting.

## What "pass" means

Each scenario has a real check, not just "didn't crash":

| Scenario | Pass condition |
|---|---|
| Normal replication | DR offset advances by exactly 1000 |
| Truncation | `Detected unrecoverable data loss` logged, and DR offset stays frozen after resuming MM2 and producing more |
| Topic reset | `Detected topic reset` logged, DR offset advances by exactly 20 post-reset |

Exits `0` if all pass, `1` otherwise, with `[PASS]`/`[FAIL]` per check and a
summary count at the end.

## Result

6/6 passing on the current patch — verified end to end, including a
back-to-back double-reset edge case (see `docs/DESIGN.md` for why that
specific case mattered).
