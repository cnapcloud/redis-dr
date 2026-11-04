package redis.dr.model;

/**
 * The checkpoint as persisted in B.
 *
 * @param state  OK, or SYNCING while a full sync is in progress
 * @param replId replication id the offset belongs to
 * @param offset last applied replication offset
 */
public record StoredCheckpoint(String state, String replId, Long offset) {
    /** True when the checkpoint can be resumed from (a SYNCING one cannot: the full sync is redone). */
    public boolean usable() {
        return "OK".equals(state) && replId != null && offset != null;
    }
}
