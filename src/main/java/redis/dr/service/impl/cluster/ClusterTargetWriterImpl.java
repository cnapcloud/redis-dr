package redis.dr.service.impl.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.dr.client.ClusterTopologyClient;
import redis.dr.client.RespClient;
import redis.dr.exception.DrFatalException;
import redis.dr.exception.RespError;
import redis.dr.model.ClusterNode;
import redis.dr.model.ClusterShard;
import redis.dr.model.Partition;
import redis.dr.model.StoredCheckpoint;
import redis.dr.service.TargetWriter;
import redis.dr.util.SlotCalculator;

import java.io.IOException;
import java.util.List;

/**
 * Writes one A shard's data to the matching B shard master as an ordinary client and keeps that shard's checkpoint on
 * the same master.
 *
 * <p>The checkpoint is a hash with fields {@code state} (OK / SYNCING), {@code replid} and {@code offset}. Its key
 * carries a hash tag chosen so that it falls into a slot of this B shard; data keys of A shard i only go to B shard i,
 * so data and checkpoint are on the same node and one Lua script ({@code allow-cross-slot-keys}) updates both.
 * The B shard is found by the slot ranges of the A shard, never by pod name. Cluster mode has db 0 only.
 */
public class ClusterTargetWriterImpl implements TargetWriter {
    private static final Logger log = LoggerFactory.getLogger(ClusterTargetWriterImpl.class);

    /** ARGV: replid, new offset, command, args... */
    private static final String APPLY_SCRIPT =
        "#!lua flags=allow-cross-slot-keys\n"
            + "local cur = redis.call('HGET', KEYS[1], 'offset')\n"
            + "if cur and tonumber(cur) >= tonumber(ARGV[2]) then return 'DUP' end\n"
            + "local r = redis.call(unpack(ARGV, 3))\n"
            + "redis.call('HSET', KEYS[1], 'state', 'OK', 'replid', ARGV[1], 'offset', ARGV[2])\n"
            + "return r";

    private final Partition partition;
    private final ClusterTopologyClient topologyClient;
    private final List<String> seeds;
    private String checkpointKey;
    /** Same hash tag as the checkpoint, so that it lives on the same B shard. Exists while the operator paused. */
    private String pauseKey;
    private RespClient conn;

    public ClusterTargetWriterImpl(Partition partition, ClusterTopologyClient topologyClient, List<String> seeds) {
        this.partition = partition;
        this.topologyClient = topologyClient;
        this.seeds = seeds;
    }

    private ClusterShard shardB() throws IOException {
        for (ClusterShard shard : topologyClient.fetch(seeds)) {
            if (shard.slotKey().equals(partition.name())) return shard;
        }
        throw new IOException("no shard with slots " + partition.name() + " in B");
    }

    /** Picks the checkpoint key: a hash tag whose slot belongs to this B shard. */
    @Override
    public synchronized void init() throws IOException {
        ClusterShard shard = shardB();
        for (int n = 0; n < 1_000_000; n++) {
            String tag = "s" + partition.index() + "x" + n;
            if (shard.owns(SlotCalculator.slot(tag))) {
                checkpointKey = "{" + tag + "}:dr:checkpoint:shard-" + partition.index();
                pauseKey = "{" + tag + "}:dr:paused:shard-" + partition.index();
                return;
            }
        }
        throw new IOException("no checkpoint tag found for slots " + partition.name());
    }

    private static boolean retryable(String message) {
        return message.startsWith("MOVED") || message.startsWith("READONLY") || message.startsWith("CLUSTERDOWN")
            || message.startsWith("TRYAGAIN") || message.startsWith("LOADING");
    }

    /** Runs a command on the current B shard master, re-resolving the master after a failure or failover. */
    private synchronized Object exec(Object... args) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                if (conn == null) {
                    ClusterNode master = shardB().master();
                    if (master == null) throw new IOException("B shard " + partition.name() + " has no online master");
                    conn = new RespClient(master.ip(), master.port());
                    log.info("B master connected: host={}", master.ip());
                }
                return conn.command(args);
            } catch (RespError e) {
                if (!retryable(e.getMessage())) throw e;
                last = e;
            } catch (IOException e) {
                last = e;
            }
            if (conn != null) conn.close();
            conn = null;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }
        throw last;
    }

    @Override
    public synchronized StoredCheckpoint loadCheckpoint() throws IOException {
        List<?> fields = (List<?>) exec("HGETALL", checkpointKey);
        if (fields == null || fields.isEmpty()) return null;
        String state = null, replId = null;
        Long offset = null;
        for (int i = 0; i + 1 < fields.size(); i += 2) {
            String name = String.valueOf(fields.get(i)), value = String.valueOf(fields.get(i + 1));
            if (name.equals("state")) state = value;
            else if (name.equals("replid")) replId = value;
            else if (name.equals("offset")) offset = Long.parseLong(value);
        }
        return new StoredCheckpoint(state, replId, offset);
    }

    @Override
    public boolean loadPaused() throws IOException {
        return ((Number) exec("EXISTS", pauseKey)).longValue() > 0;
    }

    @Override
    public void savePaused(boolean paused) throws IOException {
        if (paused) exec("SET", pauseKey, "1");
        else exec("DEL", pauseKey);
    }

    @Override
    public void clearCheckpoint() throws IOException {
        exec("DEL", checkpointKey);
    }

    /** Empties this B shard master only (other shards keep replicating) and records that a full sync is running. */
    @Override
    public synchronized void beginFullSync() throws IOException {
        exec("FLUSHALL");
        exec("HSET", checkpointKey, "state", "SYNCING");
    }

    @Override
    public synchronized void completeFullSync(String replId, long offset) throws IOException {
        exec("HSET", checkpointKey, "state", "OK", "replid", replId, "offset", offset);
    }

    @Override
    public void restore(int db, byte[] key, long absoluteTtlMillis, byte[] dump) throws IOException {
        requireDb0(db);
        exec("RESTORE", key, absoluteTtlMillis, dump, "REPLACE", "ABSTTL");
    }

    @Override
    public void apply(String replId, long offset, int db, byte[] command, byte[][] args) throws IOException {
        requireDb0(db);
        Object[] eval = new Object[args.length + 7];
        eval[0] = "EVAL";
        eval[1] = APPLY_SCRIPT;
        eval[2] = 1;
        eval[3] = checkpointKey;
        eval[4] = replId;
        eval[5] = offset;
        eval[6] = command;
        System.arraycopy(args, 0, eval, 7, args.length);
        exec(eval);
    }

    /** Not an outage of B but data this mode cannot replicate: fatal, retrying would not help. */
    private static void requireDb0(int db) {
        if (db != 0) {
            throw new DrFatalException("cluster mode has db 0 only, got db " + db, DrFatalException.EXIT_FATAL);
        }
    }

    @Override
    public synchronized void close() {
        if (conn != null) conn.close();
        conn = null;
    }
}
