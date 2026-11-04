package redis.dr.model;

/** A node as reported by CLUSTER SHARDS. */
public record ClusterNode(String id, String ip, int port, String role, String health) {
    public boolean online() {
        return health.equals("online");
    }
}
