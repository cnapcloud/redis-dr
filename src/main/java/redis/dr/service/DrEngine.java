package redis.dr.service;

import redis.dr.exception.DrFatalException;
import redis.dr.model.Partition;

import java.util.List;

/**
 * The DR sync engine, the same for Redis Sentinel and Redis Cluster.
 *
 * <p>Site A is the active site, site B the standby. One {@link ReplicationWorker} per {@link Partition} (the whole
 * Sentinel deployment, or one Cluster shard) reads the replication stream of one replica of A (PSYNC with a
 * replid+offset checkpoint) and applies it to the matching B master as an ordinary client. The checkpoint lives in
 * B and is updated in the same Lua script as the data. What differs between the modes sits behind
 * {@link SourceTopology} (where A's master and replicas are) and {@link TargetWriter} (how B is written).
 */
public interface DrEngine {
    /** Workers of all partitions; empty until the engine has started. */
    List<ReplicationWorker> workers();

    /** Stops the engine and exits the process with the exception's exit code (once, whoever reports first). */
    void fail(DrFatalException cause);
}
