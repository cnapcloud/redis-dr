package redis.dr.service;

import redis.dr.model.Partition;

/** Creates the {@link TargetWriter} of a partition (cluster mode: one per shard, each with its own B master). */
@FunctionalInterface
public interface TargetWriterFactory {
    TargetWriter create(Partition partition);
}
