package redis.dr.service.impl.sentinel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.dr.client.RespClient;
import redis.dr.client.SentinelClient;
import redis.dr.exception.RespError;
import redis.dr.model.StoredCheckpoint;
import redis.dr.service.TargetWriter;

import java.io.IOException;
import java.util.List;

/**
 * Writes to the DR (B) master, found through Sentinel B, as an ordinary client.
 *
 * <p>The checkpoint is a hash {@value #CHECKPOINT_KEY} in db 0 with fields {@code state} (OK / SYNCING),
 * {@code replid} and {@code offset}. Each streamed command is applied together with the checkpoint update in
 * one Lua script, so the data and the checkpoint cannot diverge, and a retried command is skipped.
 */
public class SentinelTargetWriterImpl implements TargetWriter {
    private static final Logger log = LoggerFactory.getLogger(SentinelTargetWriterImpl.class);

    static final String CHECKPOINT_KEY = "{dr}:checkpoint";
    /** Exists while the operator has paused the replication. */
    static final String PAUSE_KEY = "{dr}:paused";

    /** ARGV: replid, new offset, db, command, args... */
    private static final String APPLY_SCRIPT =
        "redis.call('SELECT', 0)\n"
            + "local cur = redis.call('HGET', KEYS[1], 'offset')\n"
            + "if cur and tonumber(cur) >= tonumber(ARGV[2]) then return 'DUP' end\n"
            + "redis.call('SELECT', ARGV[3])\n"
            + "local r = redis.call(unpack(ARGV, 4))\n"
            + "redis.call('SELECT', 0)\n"
            + "redis.call('HSET', KEYS[1], 'state', 'OK', 'replid', ARGV[1], 'offset', ARGV[2])\n"
            + "return r";

    /** ARGV: db, key, ttl(abs ms or 0), dump. */
    private static final String RESTORE_SCRIPT =
        "redis.call('SELECT', ARGV[1])\n"
            + "return redis.call('RESTORE', ARGV[2], ARGV[3], ARGV[4], 'REPLACE', 'ABSTTL')";

    private final SentinelClient sentinel;
    private RespClient conn;

    public SentinelTargetWriterImpl(SentinelClient sentinel) {
        this.sentinel = sentinel;
    }

    @Override
    public void init() {
        // nothing to prepare: the checkpoint key is fixed
    }

    /** Runs a command on the current B master, reconnecting through Sentinel B once on a connection failure. */
    private synchronized Object exec(Object... args) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                if (conn == null) {
                    String[] master = sentinel.masterAddress();
                    conn = new RespClient(master[0], Integer.parseInt(master[1]));
                    log.info("B master connected: host={}", master[0]);
                }
                return conn.command(args);
            } catch (RespError e) {
                throw e;
            } catch (IOException e) {
                last = e;
                if (conn != null) conn.close();
                conn = null;
            }
        }
        throw last;
    }

    @Override
    public synchronized StoredCheckpoint loadCheckpoint() throws IOException {
        List<?> fields = (List<?>) exec("HGETALL", CHECKPOINT_KEY);
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
        return ((Number) exec("EXISTS", PAUSE_KEY)).longValue() > 0;
    }

    @Override
    public void savePaused(boolean paused) throws IOException {
        if (paused) exec("SET", PAUSE_KEY, "1");
        else exec("DEL", PAUSE_KEY);
    }

    @Override
    public void clearCheckpoint() throws IOException {
        exec("DEL", CHECKPOINT_KEY);
    }

    @Override
    public synchronized void beginFullSync() throws IOException {
        exec("FLUSHALL");
        exec("HSET", CHECKPOINT_KEY, "state", "SYNCING");
    }

    @Override
    public synchronized void completeFullSync(String replId, long offset) throws IOException {
        exec("HSET", CHECKPOINT_KEY, "state", "OK", "replid", replId, "offset", offset);
    }

    @Override
    public void restore(int db, byte[] key, long absoluteTtlMillis, byte[] dump) throws IOException {
        exec("EVAL", RESTORE_SCRIPT, 1, CHECKPOINT_KEY, db, key, absoluteTtlMillis, dump);
    }

    @Override
    public void apply(String replId, long offset, int db, byte[] command, byte[][] args) throws IOException {
        Object[] eval = new Object[args.length + 8];
        eval[0] = "EVAL";
        eval[1] = APPLY_SCRIPT;
        eval[2] = 1;
        eval[3] = CHECKPOINT_KEY;
        eval[4] = replId;
        eval[5] = offset;
        eval[6] = db;
        eval[7] = command;
        System.arraycopy(args, 0, eval, 8, args.length);
        exec(eval);
    }

    @Override
    public synchronized void close() {
        if (conn != null) conn.close();
        conn = null;
    }
}
