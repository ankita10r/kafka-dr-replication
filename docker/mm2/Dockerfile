# Builds the enhanced MirrorMaker 2 image by:
#   1. cloning YOUR fork (with the patch applied and committed)
#   2. compiling only the connect-mirror module (fast — not the whole distribution)
#   3. swapping that one jar into the official apache/kafka runtime image
#
# Build with:
#   docker build \
#     --build-arg KAFKA_FORK_URL=https://github.com/<your-username>/kafka.git \
#     --build-arg KAFKA_FORK_BRANCH=feature/mm2-fault-tolerance \
#     -t <your-dockerhub-username>/mm2-enhanced:1.0 \
#     -f docker/mm2/Dockerfile .

# --- Build stage: compile only the patched module ---
FROM gradle:8.10-jdk17 AS build
ARG KAFKA_FORK_URL
ARG KAFKA_FORK_BRANCH=feature/mm2-fault-tolerance
# Docker only invalidates a RUN layer's cache when the instruction text itself
# changes -- it has no way to know the remote branch's content changed since
# last build. Without this, `docker compose up --build` can silently keep
# building from stale, previously-cloned source even after a fresh push.
# Pass a fresh value (e.g. --build-arg CACHEBUST=$(date +%s)) whenever you
# want to guarantee a real re-clone.
ARG CACHEBUST=1

RUN echo "cache bust: ${CACHEBUST}" && \
    git clone --depth 1 --branch ${KAFKA_FORK_BRANCH} ${KAFKA_FORK_URL} /kafka-src
WORKDIR /kafka-src
# Builds connect/mirror/build/libs/connect-mirror-4.0.0.jar (includes our patched
# MirrorSourceTask.class and the new MirrorDataLossException.class)
RUN ./gradlew :connect:mirror:jar -x test --no-daemon -q

# --- Runtime stage: official Kafka image, patched module swapped in ---
FROM apache/kafka:4.0.0

# Drop in our patched jar. It has the same artifact name as the stock one, so this
# replaces it on the classpath rather than adding a duplicate.
COPY --from=build /kafka-src/connect/mirror/build/libs/connect-mirror-*.jar /opt/kafka/libs/

COPY docker/mm2/mm2.properties /opt/kafka/config/mm2.properties

# Override the image's default entrypoint (which starts a broker) — this container
# runs MirrorMaker 2 in dedicated mode instead.
ENTRYPOINT ["/opt/kafka/bin/connect-mirror-maker.sh"]
CMD ["/opt/kafka/config/mm2.properties"]