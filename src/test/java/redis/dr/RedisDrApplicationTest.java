package redis.dr;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import redis.dr.service.DrEngine;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Starts the whole application against unreachable Redis addresses (nothing listens on 127.0.0.1:1): checks that the
 * wiring (engine, schedulers, jobs, error handler) is consistent and that the engine starts and stops cleanly.
 */
class RedisDrApplicationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(RedisDrApplication.class)
        .withPropertyValues("spring.main.web-application-type=none", "redis-dr.scheduler.source-watch-ms=100",
            "redis-dr.scheduler.stat-report-ms=100", "redis-dr.scheduler.layout-check-ms=100");

    @Test
    void sentinelModeStartsOneWorker() {
        runner.withPropertyValues("redis-dr.mode=sentinel", "redis-dr.sentinel.source.nodes=127.0.0.1:1",
                "redis-dr.sentinel.target.nodes=127.0.0.1:1")
            .run(ctx -> {
                assertThat(ctx).hasNotFailed();
                DrEngine engine = ctx.getBean(DrEngine.class);
                await().atMost(Duration.ofSeconds(10)).until(() -> engine.workers().size() == 1);
                assertThat(engine.workers().get(0).partition().index()).isZero();
            });
    }

    @Test
    void clusterModeWaitsForUnreachableClustersWithoutStartingWorkers() throws Exception {
        runner.withPropertyValues("redis-dr.mode=cluster", "redis-dr.cluster.source.nodes=127.0.0.1:1",
                "redis-dr.cluster.target.nodes=127.0.0.1:1")
            .run(ctx -> {
                assertThat(ctx).hasNotFailed();
                DrEngine engine = ctx.getBean(DrEngine.class);
                Thread.sleep(1500); // at least one bootstrap retry and several job runs
                assertThat(engine.workers()).isEmpty();
            });
    }
}
