package redis.dr.client;

import redis.dr.exception.RespError;
import redis.dr.model.ClusterNode;
import redis.dr.model.ClusterShard;
import redis.dr.util.SlotCalculator;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Slot-routing client for the test runner: sends a command to the master that owns the key's slot. */
public final class ClusterClient implements Closeable {
    private final ClusterTopologyClient topologyClient;
    private final List<String> seeds;
    private List<ClusterShard> shards;
    private final Map<String, RespClient> conns = new HashMap<>();

    public ClusterClient(ClusterTopologyClient topologyClient, List<String> seeds) throws IOException {
        this.topologyClient = topologyClient;
        this.seeds = seeds;
        this.shards = topologyClient.fetch(seeds);
    }

    /** Runs {@code args} on the master owning {@code key}; retries across failovers for a few seconds. */
    public Object command(String key, Object... args) throws IOException {
        int slot = SlotCalculator.slot(key);
        IOException last = null;
        for (int attempt = 0; attempt < 12; attempt++) {
            try {
                ClusterNode master = null;
                for (ClusterShard shard : shards) if (shard.owns(slot)) master = shard.master();
                if (master == null) throw new IOException("no online master for slot " + slot);
                String addr = master.ip() + ":" + master.port();
                RespClient conn = conns.get(addr);
                if (conn == null) {
                    conn = new RespClient(master.ip(), master.port());
                    conns.put(addr, conn);
                }
                return conn.command(args);
            } catch (RespError e) {
                String m = e.getMessage();
                if (!(m.startsWith("MOVED") || m.startsWith("CLUSTERDOWN") || m.startsWith("READONLY")
                    || m.startsWith("TRYAGAIN") || m.startsWith("LOADING"))) {
                    throw e;
                }
                last = e;
            } catch (IOException e) {
                last = e;
            }
            closeConnections();
            try {
                Thread.sleep(500);
                shards = topologyClient.fetch(seeds);
            } catch (IOException ignored) {
                // topology unavailable for now; retry
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }
        throw last;
    }

    private void closeConnections() {
        for (RespClient c : conns.values()) c.close();
        conns.clear();
    }

    @Override
    public void close() {
        closeConnections();
    }
}
