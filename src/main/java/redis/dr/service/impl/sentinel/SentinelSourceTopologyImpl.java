package redis.dr.service.impl.sentinel;

import redis.dr.client.RespClient;
import redis.dr.client.SentinelClient;
import redis.dr.model.Partition;
import redis.dr.model.SourceNode;
import redis.dr.model.SourceView;
import redis.dr.service.SourceTopology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Site A as a Sentinel deployment: a single partition, master and replicas asked to Sentinel A. */
public class SentinelSourceTopologyImpl implements SourceTopology {
    private final SentinelClient sentinel;

    public SentinelSourceTopologyImpl(SentinelClient sentinel) {
        this.sentinel = sentinel;
    }

    @Override
    public List<Partition> partitions() {
        return List.of(new Partition(0, null));
    }

    @Override
    public SourceView view(Partition partition) throws IOException {
        String[] address = sentinel.masterAddress();
        String masterIp = sentinel.ipOf(address[0]);
        List<SourceNode> replicas = new ArrayList<>();
        for (Map<String, String> replica : sentinel.replicas()) {
            String flags = replica.getOrDefault("flags", "");
            if (replica.get("ip").equals(masterIp) || flags.contains("s_down")
                || flags.contains("o_down") || flags.contains("disconnected")) {
                continue;
            }
            replicas.add(new SourceNode(replica.get("ip"), Integer.parseInt(replica.get("port"))));
        }
        replicas.sort(Comparator.comparing(SourceNode::ip));
        return new SourceView(new SourceNode(masterIp, Integer.parseInt(address[1])), replicas);
    }

    @Override
    public String resolveIp(String host) throws IOException {
        return sentinel.ipOf(host);
    }

    @Override
    public Map<String, String> replicationInfo(String ip, int port) throws IOException {
        return RespClient.infoReplication(ip, port);
    }
}
