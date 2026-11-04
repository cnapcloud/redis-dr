package redis.dr.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.TaskScheduler;
import redis.dr.client.ClusterTopologyClient;
import redis.dr.client.SentinelClient;
import redis.dr.scheduler.SchedulerErrorHandler;
import redis.dr.service.DrEngine;
import redis.dr.service.LayoutValidator;
import redis.dr.service.OperatorService;
import redis.dr.service.SourceSelector;
import redis.dr.service.SourceTopology;
import redis.dr.service.TargetWriterFactory;
import redis.dr.service.impl.DrEngineImpl;
import redis.dr.service.impl.OperatorServiceImpl;
import redis.dr.service.impl.SourceSelectorImpl;
import redis.dr.service.impl.cluster.ClusterSourceTopologyImpl;
import redis.dr.service.impl.cluster.ClusterTargetWriterImpl;
import redis.dr.service.impl.cluster.LayoutValidatorImpl;
import redis.dr.service.impl.sentinel.SentinelSourceTopologyImpl;
import redis.dr.service.impl.sentinel.SentinelTargetWriterImpl;

/**
 * Wires the single engine. {@code redis-dr.mode} only decides which {@link SourceTopology} and
 * {@link TargetWriterFactory} (and, for cluster, {@link LayoutValidator}) are used.
 */
@Configuration
@EnableConfigurationProperties(DrProperties.class)
public class DrConfiguration {

    @Bean
    public SourceSelector sourceSelector(SourceTopology topology) {
        return new SourceSelectorImpl(topology);
    }

    /** Not part of the "tool" profile, which only runs the test commands and must not replicate. */
    @Bean
    @Profile("!tool")
    public DrEngine drEngine(DrProperties properties, SourceTopology topology, SourceSelector selector,
                             TargetWriterFactory targetWriterFactory, ObjectProvider<LayoutValidator> layoutValidator,
                             @Qualifier("taskScheduler") TaskScheduler scheduler,
                             SchedulerErrorHandler errorHandler, ConfigurableApplicationContext context) {
        return new DrEngineImpl(properties, topology, selector, targetWriterFactory,
            layoutValidator.getIfAvailable(), scheduler, errorHandler, context);
    }

    @Bean
    @Profile("!tool")
    public OperatorService operatorService(DrEngine engine, DrProperties properties) {
        return new OperatorServiceImpl(engine, properties);
    }

    @Configuration
    @ConditionalOnProperty(prefix = "redis-dr", name = "mode", havingValue = "sentinel", matchIfMissing = true)
    static class SentinelModeConfiguration {
        private static SentinelClient client(DrProperties.Site site) {
            return new SentinelClient(site.nodes(), site.master(), site.domainSuffix());
        }

        @Bean
        SourceTopology sourceTopology(DrProperties properties) {
            return new SentinelSourceTopologyImpl(client(properties.sentinel().source()));
        }

        @Bean
        TargetWriterFactory targetWriterFactory(DrProperties properties) {
            SentinelClient targetSentinel = client(properties.sentinel().target());
            return partition -> new SentinelTargetWriterImpl(targetSentinel);
        }
    }

    @Configuration
    @ConditionalOnProperty(prefix = "redis-dr", name = "mode", havingValue = "cluster")
    static class ClusterModeConfiguration {
        @Bean
        ClusterTopologyClient clusterTopologyClient() {
            return new ClusterTopologyClient();
        }

        @Bean
        SourceTopology sourceTopology(DrProperties properties, ClusterTopologyClient topologyClient) {
            return new ClusterSourceTopologyImpl(topologyClient, properties.cluster().source().nodes());
        }

        @Bean
        TargetWriterFactory targetWriterFactory(DrProperties properties, ClusterTopologyClient topologyClient) {
            return partition -> new ClusterTargetWriterImpl(partition, topologyClient, properties.cluster().target().nodes());
        }

        @Bean
        LayoutValidator layoutValidator(DrProperties properties, ClusterTopologyClient topologyClient) {
            return new LayoutValidatorImpl(topologyClient, properties.cluster().source().nodes(),
                properties.cluster().target().nodes());
        }
    }
}
