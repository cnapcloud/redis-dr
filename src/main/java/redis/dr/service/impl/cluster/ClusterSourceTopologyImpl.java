package redis.dr.service.impl.cluster;

import redis.dr.client.ClusterTopologyClient;
import redis.dr.client.RespClient;
import redis.dr.model.ClusterNode;
import redis.dr.model.ClusterShard;
import redis.dr.model.Partition;
import redis.dr.model.SourceNode;
import redis.dr.model.SourceView;
import redis.dr.service.SourceTopology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Site A as a Redis Cluster: one partition per shard, taken from CLUSTER SHARDS. A shard is identified by its slot
 * ranges, never by pod name.
 */
public class ClusterSourceTopologyImpl implements SourceTopology {
    private final ClusterTopologyClient topologyClient;
    private final List<String> seeds;

    public ClusterSourceTopologyImpl(ClusterTopologyClient topologyClient, List<String> seeds) {
        this.topologyClient = topologyClient;
        this.seeds = seeds;
    }

    @Override
    public List<Partition> partitions() throws IOException {
        List<Partition> partitions = new ArrayList<>();
        List<ClusterShard> shards = topologyClient.fetch(seeds);
        for (int i = 0; i < shards.size(); i++) partitions.add(new Partition(i, shards.get(i).slotKey()));
        return partitions;
    }

    @Override
    public SourceView view(Partition partition) throws IOException {
        for (ClusterShard shard : topologyClient.fetch(seeds)) {
            if (!shard.slotKey().equals(partition.name())) continue;
            ClusterNode master = shard.master();
            List<SourceNode> replicas = new ArrayList<>();
            for (ClusterNode replica : shard.replicas()) replicas.add(new SourceNode(replica.ip(), replica.port()));
            return new SourceView(master == null ? null : new SourceNode(master.ip(), master.port()), replicas);
        }
        throw new IllegalStateException("A has no shard with slots " + partition.name() + " (resharded?)");
    }

    @Override
    public String resolveIp(String host) {
        return host; // CLUSTER SHARDS and master_host both announce IPs
    }

    @Override
    public Map<String, String> replicationInfo(String ip, int port) throws IOException {
        return RespClient.infoReplication(ip, port);
    }
}
