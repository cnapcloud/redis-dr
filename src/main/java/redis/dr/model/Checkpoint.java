package redis.dr.model;

/**
 * Replication position of one partition: where the next PSYNC continues from.
 *
 * <p>Empty ({@code replId == null}) means there is no usable checkpoint and a full sync is needed. Read by the
 * worker, the event listener and the periodic jobs, hence the volatile fields.
 */
public final class Checkpoint {
    private volatile String replId = null;
    private volatile long offset = -1;

    public boolean isEmpty() {
        return replId == null;
    }

    public String replId() {
        return replId;
    }

    public long offset() {
        return offset;
    }

    public void set(String replId, long offset) {
        this.replId = replId;
        this.offset = offset;
    }

    /** Same position under the new master's history id (after a failover of the source). */
    public void reanchor(String newReplId) {
        this.replId = newReplId;
    }

    public void clear() {
        this.replId = null;
        this.offset = -1;
    }
}
