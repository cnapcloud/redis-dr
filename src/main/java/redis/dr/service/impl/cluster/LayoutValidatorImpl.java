package redis.dr.service.impl.cluster;

import redis.dr.client.ClusterTopologyClient;
import redis.dr.model.ClusterShard;
import redis.dr.service.LayoutValidator;

import java.io.IOException;
import java.util.List;

public class LayoutValidatorImpl implements LayoutValidator {
    private final ClusterTopologyClient topologyClient;
    private final List<String> sourceSeeds;
    private final List<String> targetSeeds;

    public LayoutValidatorImpl(ClusterTopologyClient topologyClient, List<String> sourceSeeds, List<String> targetSeeds) {
        this.topologyClient = topologyClient;
        this.sourceSeeds = sourceSeeds;
        this.targetSeeds = targetSeeds;
    }

    @Override
    public String problem() throws IOException {
        List<ClusterShard> a;
        List<ClusterShard> b;
        try {
            a = topologyClient.fetch(sourceSeeds);
        } catch (IOException e) {
            throw new IOException("cluster A: " + e.getMessage(), e);
        }
        try {
            b = topologyClient.fetch(targetSeeds);
        } catch (IOException e) {
            throw new IOException("cluster B: " + e.getMessage(), e);
        }
        return layoutProblem(a, b);
    }

    /** @return null when A and B have the same layout, else a description of the difference. */
    static String layoutProblem(List<ClusterShard> a, List<ClusterShard> b) {
        if (a.size() != b.size()) return "shard count differs: A=" + a.size() + " B=" + b.size();
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).slotKey().equals(b.get(i).slotKey())) {
                return "slot layout differs at shard " + i + ": A=" + a.get(i).slotKey() + " B=" + b.get(i).slotKey();
            }
        }
        return null;
    }
}
