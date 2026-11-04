package redis.dr.service;

import redis.dr.model.Checkpoint;

import java.util.Map;

/** Decides, from {@code INFO replication} of A's nodes, which replica can serve the checkpoint. */
public interface SourceSelector {
    /** What to do with the checkpoint after the master of A changed. */
    enum Reconciliation {
        /** The checkpoint is still valid as is. */
        KEEP,
        /** Same position, now under the new master's replication id. */
        RE_ANCHOR,
        /** The checkpoint is beyond the failover point: A's history diverged, a full sync is needed. */
        DIVERGED
    }

    /**
     * @param info     INFO replication of the candidate replica
     * @param masterIp IP of the current master of the partition
     * @return null when the replica can serve a PSYNC from the checkpoint, else the reason it cannot
     */
    String unusable(Map<String, String> info, String masterIp, Checkpoint checkpoint);

    /** @param masterInfo INFO replication of the current master of the partition */
    Reconciliation reconcile(Map<String, String> masterInfo, Checkpoint checkpoint);

    /** A healthy replica that only lacks the history after the checkpoint, as opposed to being down or lagging links. */
    boolean cannotServeCheckpoint(String why);
}
