package redis.dr.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Every setting can be overridden by an environment variable named REDIS_DR_ + the property path. */
class EnvironmentBindingTest {
    private ApplicationContextRunner runnerWithEnv(Map<String, Object> env) {
        return new ApplicationContextRunner()
            .withUserConfiguration(DrConfiguration.class)
            .withInitializer(ctx -> {
                ctx.getEnvironment().addActiveProfile("tool");
                ctx.getEnvironment().getPropertySources().addFirst(
                    new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, env));
            });
    }

    @Test
    void sentinelSettingsFromEnvironmentVariables() {
        runnerWithEnv(Map.of(
            "REDIS_DR_SENTINEL_SOURCE_NODES", "sa1:26379,sa2:26379",
            "REDIS_DR_SENTINEL_SOURCE_MASTER", "ma",
            "REDIS_DR_SENTINEL_SOURCE_DOMAIN_SUFFIX", ".ns-a",
            "REDIS_DR_SENTINEL_TARGET_NODES", "sb:26379",
            "REDIS_DR_SENTINEL_FULLSYNC_FALLBACK_SECONDS", "45")).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            DrProperties p = ctx.getBean(DrProperties.class);
            assertThat(p.sentinel().source().nodes()).containsExactly("sa1:26379", "sa2:26379");
            assertThat(p.sentinel().source().master()).isEqualTo("ma");
            assertThat(p.sentinel().source().domainSuffix()).isEqualTo(".ns-a");
            assertThat(p.sentinel().target().nodes()).containsExactly("sb:26379");
            assertThat(p.fullsyncFallbackSeconds()).isEqualTo(45);
        });
    }

    @Test
    void clusterSettingsFromEnvironmentVariables() {
        runnerWithEnv(Map.of(
            "REDIS_DR_MODE", "cluster",
            "REDIS_DR_CLUSTER_SOURCE_NODES", "a1:6379,a2:6379",
            "REDIS_DR_CLUSTER_TARGET_NODES", "b1:6379",
            "REDIS_DR_CLUSTER_FULLSYNC_FALLBACK_SECONDS", "60",
            "REDIS_DR_SCHEDULER_SOURCE_WATCH_MS", "250",
            "REDIS_DR_REPLICATION_SOURCE_PING_PERIOD_SECONDS", "5",
            "REDIS_DR_REPLICATION_READ_TIMEOUT_SECONDS", "40")).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            DrProperties p = ctx.getBean(DrProperties.class);
            assertThat(p.mode()).isEqualTo(DrProperties.Mode.CLUSTER);
            assertThat(p.cluster().source().nodes()).containsExactly("a1:6379", "a2:6379");
            assertThat(p.cluster().target().nodes()).containsExactly("b1:6379");
            assertThat(p.fullsyncFallbackSeconds()).isEqualTo(60);
            assertThat(p.scheduler().sourceWatchMs()).isEqualTo(250);
            assertThat(p.replication().sourcePingPeriodSeconds()).isEqualTo(5);
            assertThat(p.replication().readTimeoutSeconds()).isEqualTo(40);
        });
    }
}
