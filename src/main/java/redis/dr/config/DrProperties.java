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
    @DefaultValue Scheduler scheduler,
    @DefaultValue Replication replication) {

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

    /**
     * The replication socket to the source replica of the active site (A). B is written like any client and has no
     * replication stream, so none of this applies to it.
     *
     * @param sourcePingPeriodSeconds {@code repl-ping-replica-period} of A's Redis. redis-dr does not set it; this only
     *                                declares what A uses. While nothing is written, the master's PING is the only data
     *                                on the replication socket
     * @param readTimeoutSeconds      how long that socket may stay silent before the session is dropped and the source
     *                                is selected again
     */
    public record Replication(@DefaultValue("10") int sourcePingPeriodSeconds,
                              @DefaultValue("30") int readTimeoutSeconds) {
        public static final int DEFAULT_READ_TIMEOUT_SECONDS = 30;

        public Replication {
            if (sourcePingPeriodSeconds < 1) {
                throw new IllegalArgumentException("redis-dr.replication.source-ping-period-seconds must be at least 1");
            }
        }

        /**
         * The timeout must be more than twice the ping period: equal or close values race, and the session is dropped
         * whenever the next PING is a few milliseconds late. 0 (no timeout) is not accepted either, since a connection
         * that died silently would then never be noticed.
         */
        public boolean readTimeoutValid() {
            return readTimeoutSeconds > 2L * sourcePingPeriodSeconds;
        }

        /** The timeout in force: the configured one if valid, else the larger of the default and 3 x the ping period. */
        public int effectiveReadTimeoutSeconds() {
            return readTimeoutValid() ? readTimeoutSeconds
                : Math.max(DEFAULT_READ_TIMEOUT_SECONDS, 3 * sourcePingPeriodSeconds);
        }
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
