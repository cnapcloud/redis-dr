package redis.dr.service;

import redis.dr.model.Partition;
import redis.dr.model.SourceView;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Where site A's partitions, masters and replicas are: asked to Sentinel or to CLUSTER SHARDS. */
public interface SourceTopology {
    /** Partitions of A in a stable order: one in Sentinel mode, one per shard in Cluster mode. */
    List<Partition> partitions() throws IOException;

    /** Current master and healthy replicas of the partition. */
    SourceView view(Partition partition) throws IOException;

    /** Resolves a {@code master_host} announced by a replica to the IP form used in {@link SourceView}. */
    String resolveIp(String host) throws IOException;

    /** {@code INFO replication} of an A node. */
    Map<String, String> replicationInfo(String ip, int port) throws IOException;
}
