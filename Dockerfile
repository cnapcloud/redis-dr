FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew --no-daemon --version
COPY src src
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 dr
WORKDIR /app
COPY --from=build /src/build/install/redis-dr/ /app/
USER 10001

# All settings and their defaults are in application.yaml. Override any of them with an environment variable named
# REDIS_DR_ + the property path in upper case (dots and dashes as underscores), e.g. REDIS_DR_MODE=cluster or
# REDIS_DR_SENTINEL_SOURCE_NODES=sentinel-a:26379.

ENTRYPOINT ["/app/bin/redis-dr"]
