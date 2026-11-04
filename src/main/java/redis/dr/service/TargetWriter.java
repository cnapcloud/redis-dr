package redis.dr.service;

import redis.dr.model.StoredCheckpoint;

import java.io.IOException;

/**
 * Writes one partition to its B master as an ordinary client and keeps the replication checkpoint inside B itself.
 *
 * <p>Each streamed command is applied together with the checkpoint update in one Lua script, so the data and the
 * checkpoint cannot diverge, and a retried command is skipped. A full sync marks the state SYNCING first; a restart
 * that finds SYNCING redoes the full sync.
 */
public interface TargetWriter {
    /** Prepares the writer (e.g. picks the checkpoint key); retried by the caller until B is reachable. */
    void init() throws IOException;

    /** @return the persisted checkpoint, or null when B holds none. */
    StoredCheckpoint loadCheckpoint() throws IOException;

    /** @return true when the operator paused this partition; kept in B so that a restart stays paused. */
    boolean loadPaused() throws IOException;

    void savePaused(boolean paused) throws IOException;

    /** Deletes the checkpoint, so that the next start is a full sync. Only called while the partition is paused. */
    void clearCheckpoint() throws IOException;

    /** Empties B (this partition only) and records that a full sync is in progress. */
    void beginFullSync() throws IOException;

    void completeFullSync(String replId, long offset) throws IOException;

    /** Loads one key of the snapshot; {@code absoluteTtlMillis} is 0 for no expiry. */
    void restore(int db, byte[] key, long absoluteTtlMillis, byte[] dump) throws IOException;

    /** Applies one streamed command and moves the checkpoint to {@code offset} atomically. */
    void apply(String replId, long offset, int db, byte[] command, byte[][] args) throws IOException;

    void close();
}
