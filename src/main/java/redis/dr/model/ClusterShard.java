package redis.dr.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A shard as reported by CLUSTER SHARDS: its slot ranges and nodes. */
public record ClusterShard(List<long[]> ranges, List<ClusterNode> nodes) {
    /** Slot ranges as text ("0-5460"); identifies a shard across A and B. */
    public String slotKey() {
        StringBuilder sb = new StringBuilder();
        for (long[] r : ranges) sb.append(sb.length() == 0 ? "" : ",").append(r[0]).append('-').append(r[1]);
        return sb.toString();
    }

    public boolean owns(int slot) {
        for (long[] r : ranges) if (slot >= r[0] && slot <= r[1]) return true;
        return false;
    }

    /** The online master, or null while it is down and no replica has been promoted yet. */
    public ClusterNode master() {
        for (ClusterNode n : nodes) if (n.role().equals("master") && n.online()) return n;
        return null;
    }

    public List<ClusterNode> replicas() {
        List<ClusterNode> out = new ArrayList<>();
        for (ClusterNode n : nodes) if (n.role().equals("replica") && n.online()) out.add(n);
        out.sort(Comparator.comparing(ClusterNode::ip));
        return out;
    }
}
