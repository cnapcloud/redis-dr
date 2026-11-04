package redis.dr.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import redis.dr.model.NodeAddress;

import java.util.List;

/** All settings of the engine, from {@code application.yaml} (prefix {@code redis-dr}). */
@ConfigurationProperties(prefix = "redis-dr")
public record DrProperties(
    @DefaultValue("sentinel") Mode mode,
    @DefaultValue Sentinel sentinel,
    @DefaultValue Cluster cluster,
    @DefaultValue Scheduler scheduler) {

    public enum Mode { SENTINEL, CLUSTER }

    /**
     * @param fullsyncFallbackSeconds see {@link #fullsyncFallbackSeconds()}
     * @param source                  Sentinel of the active site (A): where the source replica is chosen
     * @param target                  Sentinel of the DR site (B): where the DR master address is tracked
     */
    public record Sentinel(@DefaultValue("0") int fullsyncFallbackSeconds,
                           @DefaultValue Site source, @DefaultValue Site target) {
    }

    /**
     * @param nodes        Sentinel nodes (host:port); the first one that answers is used
     * @param master       name of the monitored master
     * @param domainSuffix appended to host names with a single dot (e.g. {@code redis-a-0.redis-a}) that Sentinel announces
     */
    public record Site(List<String> nodes, @DefaultValue("mymaster") String master, String domainSuffix) {
        public Site {
            nodes = nodes == null ? List.of() : List.copyOf(nodes);
            if (domainSuffix == null) domainSuffix = "";
        }
    }

    /**
     * @param source cluster of the active site (A)
     * @param target cluster of the DR site (B)
     */
    public record Cluster(@DefaultValue Endpoint source, @DefaultValue Endpoint target,
                          @DefaultValue("30") int fullsyncFallbackSeconds) {
    }

    /** @param nodes any nodes of the cluster (host:port); CLUSTER SHARDS is asked to the first one that answers */
    public record Endpoint(List<String> nodes) {
        public Endpoint {
            nodes = nodes == null ? List.of() : List.copyOf(nodes);
        }
    }

    /**
     * Periods in milliseconds of the scheduled work.
     *
     * @param waitLogMs how often a "still waiting" reminder is logged while B does not accept writes or no replica of A
     *                  can be read (0 = never)
     */
    public record Scheduler(@DefaultValue("200") long workerRetryMs, @DefaultValue("1000") long sourceWatchMs,
                            @DefaultValue("5000") long statReportMs, @DefaultValue("5000") long layoutCheckMs,
                            @DefaultValue("20000") long waitLogMs) {
    }

    public DrProperties {
        if (mode == Mode.CLUSTER) {
            requireNodes("redis-dr.cluster.source.nodes", cluster.source().nodes());
            requireNodes("redis-dr.cluster.target.nodes", cluster.target().nodes());
        } else {
            requireNodes("redis-dr.sentinel.source.nodes", sentinel.source().nodes());
            requireNodes("redis-dr.sentinel.target.nodes", sentinel.target().nodes());
        }
    }

    private static void requireNodes(String property, List<String> nodes) {
        if (nodes.isEmpty()) throw new IllegalArgumentException(property + " is required (host:port, comma separated)");
        nodes.forEach(NodeAddress::parse); // fail at startup on a malformed address
    }

    /**
     * Seconds that healthy replicas may all be unable to serve the checkpoint (their backlog starts after it, e.g. a
     * replaced replica that did a full sync) before the partition falls back to a full sync of its B master.
     * 0 disables the fallback.
     */
    public int fullsyncFallbackSeconds() {
        return mode == Mode.CLUSTER ? cluster.fullsyncFallbackSeconds() : sentinel.fullsyncFallbackSeconds();
    }
}
