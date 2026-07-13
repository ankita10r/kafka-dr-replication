# mm2-patch/

Two different things live in this folder, for two different purposes:

- **`0001`–`0004` `.patch` files** — apply these to a separate clone of my
  Kafka fork with `git apply`. This is the only place they actually do
  anything.
- **`main/` and `test/` `.java` files** — the same content as the patches,
  kept here as plain, readable source for review. They will **not**
  compile inside this repo (this project's classpath doesn't include
  Kafka's `connect:mirror` module) — that's expected, not a bug. The real
  compile and test run happens inside the fork, after the patches are
  applied there:

