package redis.dr.service;

import redis.dr.model.Partition;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerStatus;

/**
 * Replicates one partition from A to B. Driven by the scheduler: {@link #runOnce()} makes one step and, once a
 * source replica is chosen, blocks for the whole replication session.
 */
public interface ReplicationWorker {
    Partition partition();

    /** One step of the worker: initialize, pick a source, or run a replication session until it ends. */
    void runOnce();

    /** Switches away from the current source when Sentinel/CLUSTER SHARDS report it promoted to master. */
    void checkSource();

    /** Logs a stat line while a session is running. */
    void reportStat();

    /** Ends the current session and makes {@link #runOnce()} a no-op. Final: used when the process shuts down. */
    void stop();

    // ---- operator

    /** Current status from memory and from the values of the last {@link #refreshStatus()}; calls neither A nor B. */
    WorkerStatus status();

    /** Measures what the status shows but memory does not know: checkpoint in B, master offset of A, lag, rate. */
    void refreshStatus();

    /**
     * Ends the session and stops the worker, keeping the checkpoint. The pause is stored in B, so a restart stays
     * paused. Idempotent.
     *
     * @param force pause even while a full sync is loading (the snapshot is lost: B stays flushed until a full sync)
     * @throws redis.dr.exception.ConflictException    a full sync is in progress and {@code force} is false
     * @throws redis.dr.exception.UnavailableException still connecting to B, or B cannot store the flag
     */
    void pause(boolean force);

    /** Whether {@link #resume()} would continue from the checkpoint. Changes nothing. */
    ResumeCheck resumeCheck();

    /**
     * Continues from the checkpoint stored in B. Idempotent. Never starts a full sync by itself.
     *
     * @throws redis.dr.exception.ConflictException the checkpoint cannot be continued; use {@link #fullSync()}
     */
    void resume();

    /** Deletes the checkpoint in B and restarts the worker, which then flushes B and loads a snapshot. */
    void fullSync();
}
