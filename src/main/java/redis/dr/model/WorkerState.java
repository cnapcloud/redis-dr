package redis.dr.model;

/** What a worker is doing, as shown to the operator. */
public enum WorkerState {
    /** Connecting to B and reading the persisted checkpoint and pause flag. */
    INITIALIZING,
    /** Looking for a replica of A that can serve the checkpoint (or for a master / any replica). */
    WAITING_SOURCE,
    /** A write to B failed (B down, read-only, out of memory, ...): the session is closed and retried; see `detail`. */
    WAITING_TARGET,
    /** Loading the snapshot of A into B: B was flushed and the DR is unavailable for this partition. */
    FULL_SYNC,
    /** Applying the replication stream of A to B. */
    STREAMING,
    /** Stopped by the operator; the checkpoint is kept. */
    PAUSED
}
