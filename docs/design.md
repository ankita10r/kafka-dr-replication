# DESIGN.md

**My Personal Dev Notes**
*Check out the PR here: [https://github.com/apache/kafka/pull/22819](https://github.com/apache/kafka/pull/22819)*

**The Big Picture**
I’m building a pipeline: `My Generator -> Primary Kafka -> Enhanced MirrorMaker 2 -> DR Kafka`. 

Standard MirrorMaker 2 (MM2) is great, but it’s a bit too quiet when things break. If data disappears, it just skips it. If you delete a topic, it freezes. I’ve added two rules to fix that:

| Failure Type | Standard MM2 | My Enhanced Version |
|---|---|---|
| **Data gets purged early** | Stays silent and skips the gap | It screams (throws an error) and crashes |
| **Topic deleted/recreated** | Stalls out | Notices the change and restarts from offset 0 |

**Task 1: The Producer**
I kept the data-making logic (`EventGenerator`/`EventJsonMapper`) totally separate from the Kafka networking code. This was a deliberate choice so I could test the logic with a `MockProducer` without needing to actually start up a Kafka cluster. It makes testing 100x faster. Also, I made `--count` mandatory—if you don't tell it how much to send, it shouldn't guess.

**Task 2/3: The MM2 Patches**
To make MM2 actually catch these errors, I set `auto.offset.reset=none`. This forces it to throw an exception instead of trying to "guess" where to jump. 

The trickiest part was figuring out if an error meant "data was lost" or "the topic was reset." I solved this by checking the topic's unique ID (UUID). If the ID changed, it’s a reset (benign). If the ID stayed the same but the data is gone, that’s data loss (critical). I pulled that comparison into its own little class (`ReplicationFailureClassifier`) so I could unit test just the decision logic without needing a broker at all—same trick as the producer.

I also went with `.patch` files instead of raw `.java` files for the PR submission. It's just cleaner—shows exactly what changed line by line, and `git apply` will complain if it doesn't match up with upstream instead of silently overwriting something.

**Three Bugs I Only Found By Actually Running It**
Unit tests alone didn't catch any of these—they only showed up once I ran the real two-cluster setup:

1. A brand new partition's very first run threw a different exception (`NoOffsetForPartitionException`, not the one I was catching) and looked like a crash instead of a normal startup.
2. My topic-ID cache lived in memory only, so a container restart wiped it—meaning a reset that already happened before the restart looked exactly like no reset at all.
3. If the old offset happened to still be a valid position in the recreated topic, no exception fired at all, and MM2 just silently sat there doing nothing.

All three are fixed now, but they're a good reminder that green unit tests don't mean the real system works.

**A Quick Note on the PR process**
I ran into a nightmare with my git branches. My fork got corrupted and I accidentally cherry-picked some commits from the wrong version. I finally fixed it by going back to the original `4.0` branch and starting fresh. The final PR is just one clean commit.

**AI Tool Usage**
I used Claude and Gemini to bounce design ideas back and forth. They were super helpful for structuring the tasks, figuring out the Docker configuration, and—most importantly—helping me untangle my git mess when I broke my branch. I verified all the code by running it against a real cluster, which is how I caught the three bugs above.
