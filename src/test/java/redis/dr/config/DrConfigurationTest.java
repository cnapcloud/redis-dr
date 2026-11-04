package redis.dr.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import redis.dr.service.LayoutValidator;
import redis.dr.service.SourceTopology;
import redis.dr.service.TargetWriterFactory;
import redis.dr.service.impl.cluster.ClusterSourceTopologyImpl;
import redis.dr.service.impl.sentinel.SentinelSourceTopologyImpl;

import static org.assertj.core.api.Assertions.assertThat;

/** Mode selection and property binding. The "tool" profile keeps the engine itself (and its scheduler) out. */
class DrConfigurationTest {
    private static final String[] SENTINELS = {
        "redis-dr.sentinel.source.nodes=sa:26379", "redis-dr.sentinel.target.nodes=sb:26379"};

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(DrConfiguration.class)
        .withInitializer(ctx -> ctx.getEnvironment().addActiveProfile("tool"));

    @Test
    void sentinelIsTheDefaultMode() {
        runner.withPropertyValues(SENTINELS).run(ctx -> {
            assertThat(ctx).hasSingleBean(SourceTopology.class);
            assertThat(ctx.getBean(SourceTopology.class)).isInstanceOf(SentinelSourceTopologyImpl.class);
            assertThat(ctx).hasSingleBean(TargetWriterFactory.class);
            assertThat(ctx).doesNotHaveBean(LayoutValidator.class);
            DrProperties p = ctx.getBean(DrProperties.class);
            assertThat(p.mode()).isEqualTo(DrProperties.Mode.SENTINEL);
            assertThat(p.fullsyncFallbackSeconds()).isZero();
            assertThat(p.scheduler().workerRetryMs()).isEqualTo(200);
            assertThat(p.scheduler().waitLogMs()).isEqualTo(20000);
        });
    }

    @Test
    void sentinelSettingsAreBound() {
        runner.withPropertyValues("redis-dr.mode=sentinel",
                "redis-dr.sentinel.source.nodes=sa1:26379,sa2:26380", "redis-dr.sentinel.source.master=ma",
                "redis-dr.sentinel.source.domain-suffix=.ns-a", "redis-dr.sentinel.target.nodes=sb:26379")
            .run(ctx -> {
                DrProperties.Sentinel s = ctx.getBean(DrProperties.class).sentinel();
                assertThat(s.source().nodes()).containsExactly("sa1:26379", "sa2:26380");
                assertThat(s.source().master()).isEqualTo("ma");
                assertThat(s.source().domainSuffix()).isEqualTo(".ns-a");
                assertThat(s.target().nodes()).containsExactly("sb:26379");
                assertThat(s.target().master()).isEqualTo("mymaster");
                assertThat(s.target().domainSuffix()).isEmpty();
            });
    }

    @Test
    void sentinelModeWithoutNodesOrWithABadAddressFailsAtStartup() {
        runner.run(ctx -> assertThat(ctx).hasFailed());
        runner.withPropertyValues("redis-dr.sentinel.source.nodes=sentinel-a", "redis-dr.sentinel.target.nodes=sb:26379")
            .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void clusterModeWiresTheClusterImplementationsAndTheLayoutValidator() {
        runner.withPropertyValues("redis-dr.mode=cluster",
                "redis-dr.cluster.source.nodes=a1:6379,a2:6379", "redis-dr.cluster.target.nodes=b1:6379")
            .run(ctx -> {
                assertThat(ctx.getBean(SourceTopology.class)).isInstanceOf(ClusterSourceTopologyImpl.class);
                assertThat(ctx).hasSingleBean(TargetWriterFactory.class);
                assertThat(ctx).hasSingleBean(LayoutValidator.class);
                DrProperties p = ctx.getBean(DrProperties.class);
                assertThat(p.cluster().source().nodes()).containsExactly("a1:6379", "a2:6379");
                assertThat(p.cluster().target().nodes()).containsExactly("b1:6379");
                assertThat(p.fullsyncFallbackSeconds()).isEqualTo(30);
            });
    }

    @Test
    void clusterModeWithoutNodesFailsAtStartup() {
        runner.withPropertyValues("redis-dr.mode=cluster").run(ctx -> assertThat(ctx).hasFailed());
    }
}
