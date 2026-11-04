package redis.dr.client;

import redis.dr.model.ClusterNode;
import redis.dr.model.NodeAddress;
import redis.dr.model.ClusterShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Cluster topology from CLUSTER SHARDS. */
public final class ClusterTopologyClient {
    /** Asks the seeds (host:port) in order; shards without slots are left out; the result is sorted by first slot. */
    public List<ClusterShard> fetch(List<String> seeds) throws IOException {
        IOException last = new IOException("no seeds");
        for (String seed : seeds) {
            NodeAddress node = NodeAddress.parse(seed);
            try {
                Object reply = RespClient.once(node.host(), node.port(), "CLUSTER", "SHARDS");
                return parse((List<?>) reply);
            } catch (IOException e) {
                last = e;
            }
        }
        throw last;
    }

    static List<ClusterShard> parse(List<?> reply) {
        List<ClusterShard> shards = new ArrayList<>();
        for (Object s : reply) {
            Map<String, Object> shard = pairs((List<?>) s);
            List<?> slots = (List<?>) shard.get("slots");
            if (slots == null || slots.isEmpty()) continue;
            List<long[]> ranges = new ArrayList<>();
            for (int i = 0; i + 1 < slots.size(); i += 2) {
                ranges.add(new long[]{((Number) slots.get(i)).longValue(), ((Number) slots.get(i + 1)).longValue()});
            }
            List<ClusterNode> nodes = new ArrayList<>();
            for (Object n : (List<?>) shard.get("nodes")) {
                Map<String, Object> node = pairs((List<?>) n);
                nodes.add(new ClusterNode(String.valueOf(node.get("id")), String.valueOf(node.get("ip")),
                    Integer.parseInt(String.valueOf(node.get("port"))), String.valueOf(node.get("role")),
                    String.valueOf(node.get("health"))));
            }
            shards.add(new ClusterShard(ranges, nodes));
        }
        shards.sort(Comparator.comparingLong(sh -> sh.ranges().get(0)[0]));
        return shards;
    }

    private static Map<String, Object> pairs(List<?> list) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < list.size(); i += 2) map.put(String.valueOf(list.get(i)), list.get(i + 1));
        return map;
    }
}
