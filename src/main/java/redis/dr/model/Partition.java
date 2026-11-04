package redis.dr.model;

/**
 * One unit of replication: the whole Sentinel deployment (one partition), or one Cluster shard.
 *
 * @param index position of the partition (shard order by first slot)
 * @param name  slot ranges of the shard ("0-5460"), which identify it across A and B; null in Sentinel mode
 */
public record Partition(int index, String name) {
    /** Log prefix; empty when there is only one partition. */
    public String label() {
        return name == null ? "" : "[shard " + index + "] ";
    }
}
