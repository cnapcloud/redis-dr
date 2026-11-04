package redis.dr.service.impl;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SelectCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PreCommandSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.dr.model.Checkpoint;
import redis.dr.service.EventApplier;
import redis.dr.service.TargetWriter;
import redis.dr.util.ReplIds;

import java.nio.charset.StandardCharsets;

public class EventApplierImpl implements EventApplier {
    private static final Logger log = LoggerFactory.getLogger(EventApplierImpl.class);

    private final Checkpoint checkpoint;
    private final TargetWriter target;

    private volatile long applied = 0;
    private volatile long restored = 0;
    private volatile long writes = 0;
    private volatile long fullSyncStartedAt = 0;
    private volatile int currentDb = 0;

    public EventApplierImpl(Checkpoint checkpoint, TargetWriter target) {
        this.checkpoint = checkpoint;
        this.target = target;
    }

    @Override
    public void resetSession() {
        currentDb = 0;
        fullSyncStartedAt = 0;
    }

    @Override
    public boolean fullSyncInProgress() {
        return fullSyncStartedAt != 0;
    }

    @Override
    public long applied() {
        return applied;
    }

    @Override
    public long restored() {
        return restored;
    }

    @Override
    public long writes() {
        return writes;
    }

    @Override
    public void onEvent(Configuration cfg, Event event) throws Exception {
        if (event instanceof PreRdbSyncEvent) {
            // Any FULLRESYNC (first start, diverged history, checkpoint refused): rebuild B from the snapshot.
            fullSyncStartedAt = System.currentTimeMillis();
            restored = 0;
            checkpoint.clear();
            cfg.setReplId("?"); // a retry in the middle of the snapshot must not PSYNC from the old checkpoint
            cfg.setReplOffset(-1);
            target.beginFullSync();
            writes++;
            log.info("full sync started: B flushed, DR unavailable until the snapshot is loaded");
        } else if (event instanceof DumpKeyValuePair) {
            DumpKeyValuePair kv = (DumpKeyValuePair) event;
            long ttl = 0;
            if (kv.getExpiredType() == ExpiredType.SECOND) ttl = kv.getExpiredValue() * 1000;
            else if (kv.getExpiredType() == ExpiredType.MS) ttl = kv.getExpiredValue();
            int db = kv.getDb() == null ? 0 : (int) kv.getDb().getDbNumber();
            target.restore(db, kv.getKey(), ttl, kv.getValue());
            restored++;
            writes++;
        } else if (event instanceof PreCommandSyncEvent) {
            if (fullSyncStartedAt != 0) {
                target.completeFullSync(cfg.getReplId(), cfg.getReplOffset());
                writes++;
                checkpoint.set(cfg.getReplId(), cfg.getReplOffset());
                log.info("full sync done: keys={} elapsed_ms={} replid={} offset={}", restored,
                    System.currentTimeMillis() - fullSyncStartedAt, ReplIds.abbreviate(checkpoint.replId()),
                    checkpoint.offset());
                fullSyncStartedAt = 0;
            }
        } else if (event instanceof SelectCommand) {
            currentDb = ((SelectCommand) event).getIndex();
        } else if (event instanceof DefaultCommand) {
            DefaultCommand cmd = (DefaultCommand) event;
            String name = new String(cmd.getCommand(), StandardCharsets.US_ASCII).toUpperCase();
            switch (name) {
                case "PING":
                case "REPLCONF":
                case "MULTI": // transactions are applied command by command, not atomically
                case "EXEC":
                    return;
                case "SELECT":
                    currentDb = Integer.parseInt(new String(cmd.getArgs()[0], StandardCharsets.US_ASCII));
                    return;
                case "PUBLISH":
                    if (cmd.getArgs().length > 0
                        && new String(cmd.getArgs()[0], StandardCharsets.US_ASCII).startsWith("__sentinel__")) {
                        return; // would make B's Sentinels discover A's Sentinels
                    }
                    break;
                default:
                    break;
            }
            long end = cmd.getContext().getOffsets().getV2();
            target.apply(cfg.getReplId(), end, currentDb, cmd.getCommand(), cmd.getArgs());
            applied++;
            writes++;
        }
    }
}
