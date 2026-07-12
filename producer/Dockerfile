# --- Build stage ---
FROM gradle:8.10-jdk17 AS build
WORKDIR /build
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle shadowJar --no-daemon -q

# --- Runtime stage ---
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/build/libs/commit-log-producer.jar /app/commit-log-producer.jar

ENTRYPOINT ["java", "-jar", "/app/commit-log-producer.jar"]
CMD ["--count", "100"]
