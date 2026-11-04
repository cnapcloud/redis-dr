package redis.dr.client;

import redis.dr.model.NodeAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Queries one Sentinel deployment (master address, replicas), asking its Sentinel nodes in order. */
public final class SentinelClient {
    private final List<NodeAddress> nodes = new ArrayList<>();
    private final String masterName;
    private final String domainSuffix;

    /** @param nodes Sentinel nodes (host:port); the first one that answers is used */
    public SentinelClient(List<String> nodes, String masterName, String domainSuffix) {
        for (String node : nodes) this.nodes.add(NodeAddress.parse(node));
        this.masterName = masterName;
        this.domainSuffix = domainSuffix;
    }

    private Object query(Object... args) throws IOException {
        IOException last = new IOException("no sentinel nodes");
        for (NodeAddress node : nodes) {
            try {
                return RespClient.once(node.host(), node.port(), args);
            } catch (IOException e) {
                last = e;
            }
        }
        throw last;
    }

    /** @return {host, port} as announced by Sentinel (host may be a short DNS name). */
    public String[] masterAddress() throws IOException {
        List<?> reply = (List<?>) query("sentinel", "get-master-addr-by-name", masterName);
        if (reply == null || reply.size() < 2) { // Sentinel does not know that master name (nil reply)
            throw new IOException("Sentinel does not know a master named '" + masterName + "'");
        }
        return new String[]{(String) reply.get(0), (String) reply.get(1)};
    }

    public List<Map<String, String>> replicas() throws IOException {
        List<Map<String, String>> out = new ArrayList<>();
        for (Object item : (List<?>) query("sentinel", "replicas", masterName)) {
            List<?> fields = (List<?>) item;
            Map<String, String> replica = new HashMap<>();
            for (int i = 0; i + 1 < fields.size(); i += 2) {
                replica.put(String.valueOf(fields.get(i)), String.valueOf(fields.get(i + 1)));
            }
            out.add(replica);
        }
        return out;
    }

    /** Resolves a host announced by Sentinel to an IP, completing short names with the site's domain suffix. */
    public String ipOf(String host) throws IOException {
        long dots = host.chars().filter(c -> c == '.').count();
        if (dots == 1 && !Character.isDigit(host.charAt(0))) host += domainSuffix;
        return InetAddress.getByName(host).getHostAddress();
    }
}
