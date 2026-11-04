package redis.dr.service.impl;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.parser.DefaultCommandParser;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.dr.client.RespClient;
import redis.dr.exception.ConflictException;
import redis.dr.exception.DrFatalException;
import redis.dr.exception.UnavailableException;
import redis.dr.log.LogContext;
import redis.dr.model.Checkpoint;
import redis.dr.model.Partition;
import redis.dr.model.SourceNode;
import redis.dr.model.SourceView;
import redis.dr.model.ResumeCheck;
import redis.dr.model.StoredCheckpoint;
import redis.dr.model.WorkerState;
import redis.dr.model.WorkerStatus;
import redis.dr.service.EventApplier;
import redis.dr.service.ReplicationWorker;
import redis.dr.service.SourceSelector;
import redis.dr.service.SourceTopology;
import redis.dr.service.TargetWriter;
import redis.dr.util.ReplIds;
import redis.dr.util.ReplicationInfoParser;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Replicates one partition from A to B.
 *
 * <p>Reads the replication stream of one replica of the partition (PSYNC with a replid+offset checkpoint) and applies
 * it to the B master. When the source replica dies or is promoted it moves to another replica and continues from the
 * checkpoint; when the checkpoint can no longer be honoured it empties only this partition of B and does a full sync.
 * The engine never reads from the master.
 *
 * <p>The scheduler calls {@link #runOnce()} repeatedly. Waiting (for B, for a usable replica) is a state kept in the
 * worker and a {@code notBefore} time, never a sleeping thread; only a replication session blocks the thread.
 *
 * <p>The operator can pause a worker. The pause is stored in B (see {@link TargetWriter#savePaused}), so that a
 * restarted process stays paused. {@code paused} and {@code busy} are the two sides of a handshake: the operator
 * thread sets {@code paused} and then waits for {@code busy} to drop, while the worker thread sets {@code busy} and
 * then looks at {@code paused}; one of them always sees the other.
 */
public class ReplicationWorkerImpl implements ReplicationWorker {
    private static final Logger log = LoggerFactory.getLogger(ReplicationWorkerImpl.class);

    private static final long INIT_RETRY_MS = 1000;
    private static final long PICK_RETRY_MS = 500;
    private static final long PAUSE_WAIT_MS = 10_000;

    private enum State { INIT, SELECTING }

    private record Session(String host, int port, Configuration cfg, Replicator replicator, Instant startedAt,
                           String mode) {
    }

    /** Values measured by {@link #refreshStatus()}; the status itself never calls A or B. */
    private record Measured(String persistedState, Long persistedOffset, Long masterOffset, Long lagBytes,
                            Double appliedPerSecond, long atMs, long appliedCount, Instant asOf, String error) {
        static final Measured NONE = new Measured(null, null, null, null, null, 0, 0, null, null);
    }

    private record Probe(ResumeCheck check, StoredCheckpoint stored) {
    }

    private final Partition partition;
    private final SourceTopology topology;
    private final SourceSelector selector;
    private final Checkpoint checkpoint;
    private final TargetWriter target;
    private final EventApplier applier;
    private final int fullsyncFallbackSeconds;
    private final long waitLogMs;
    private final Consumer<DrFatalException> onFatal;
    private final LongSupplier clock;

    private final Object control = new Object(); // serializes pause / resume / full sync requests

    private volatile boolean stopped = false;
    private volatile boolean paused = false;
    private volatile boolean busy = false;
    private volatile Session session = null;
    private volatile State state = State.INIT;
    private volatile long notBefore = 0;
    private volatile long unservableSince = 0;
    private volatile String lastWait = "";
    private volatile String lastInitError = "";
    /** Why the last write to B failed; null while writes work. Shown as the state WAITING_TARGET. */
    private volatile String targetError = null;
    private volatile long writesAtFailure = 0;
    private volatile long targetErrorSince = 0;
    private volatile long lastTargetReminder = 0;
    /** Start of the current wait for a source replica (0 = not waiting) and the last reminder logged about it. */
    private volatile long waitingSince = 0;
    private volatile long lastWaitReminder = 0;
    private volatile boolean targetFailed = false;
    private volatile Long expectedKeys = null;
    private volatile Measured measured = Measured.NONE;
    private boolean targetReady = false;
    private long selectStartedAt = 0;

    public ReplicationWorkerImpl(Partition partition, SourceTopology topology, SourceSelector selector,
                                 Checkpoint checkpoint, TargetWriter target, EventApplier applier,
                                 int fullsyncFallbackSeconds, long waitLogMs, Consumer<DrFatalException> onFatal) {
        this(partition, topology, selector, checkpoint, target, applier, fullsyncFallbackSeconds, waitLogMs,
            onFatal, System::currentTimeMillis);
    }

    ReplicationWorkerImpl(Partition partition, SourceTopology topology, SourceSelector selector,
                          Checkpoint checkpoint, TargetWriter target, EventApplier applier,
                          int fullsyncFallbackSeconds, long waitLogMs, Consumer<DrFatalException> onFatal,
                          LongSupplier clock) {
        this.partition = partition;
        this.topology = topology;
        this.selector = selector;
        this.checkpoint = checkpoint;
        this.target = target;
        this.applier = applier;
        this.fullsyncFallbackSeconds = fullsyncFallbackSeconds;
        this.waitLogMs = waitLogMs;
        this.onFatal = onFatal;
        this.clock = clock;
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public void runOnce() {
        if (stopped || clock.getAsLong() < notBefore) return;
        busy = true;
        try {
            if (paused && state != State.INIT) return;
            try (LogContext ignored = LogContext.partition(partition)) {
                if (state == State.INIT) initialize();
                else select();
            }
        } finally {
            busy = false;
        }
    }

    @Override
    public void stop() {
        stopped = true;
        Session s = session;
        if (s != null) {
            try {
                s.replicator().close();
            } catch (Exception ignored) {
                // already closed
            }
        }
        target.close();
    }

    // ---------------------------------------------------------------- init: B reachable, persisted checkpoint

    private void initialize() {
        try {
            if (!targetReady) {
                target.init();
                targetReady = true;
            }
            loadPersistedCheckpoint(target.loadCheckpoint());
            if (target.loadPaused()) {
                paused = true;
                log.info("paused by the operator (flag stored in B), waiting for resume");
            }
            lastInitError = "";
            state = State.SELECTING;
        } catch (Exception e) {
            String error = e.toString();
            if (!error.equals(lastInitError)) {
                log.warn("target not ready, retrying: stage={} cause={}", targetReady ? "checkpoint" : "connect", error);
            }
            lastInitError = error;
            notBefore = clock.getAsLong() + INIT_RETRY_MS;
        }
    }

    /** B is the truth: the position in memory is replaced by what B holds (also after a failed write to B). */
    private void loadPersistedCheckpoint(StoredCheckpoint cp) {
        boolean retry = targetError != null; // already reported; do not repeat it every second
        if (cp != null && cp.usable()) {
            checkpoint.set(cp.replId(), cp.offset());
            if (!retry) {
                log.info("resume from persisted checkpoint: replid={} offset={}", ReplIds.abbreviate(cp.replId()), cp.offset());
            }
        } else if (cp == null) {
            checkpoint.clear();
            if (!retry) log.info("no checkpoint in B, full sync");
        } else {
            checkpoint.clear();
            if (!retry) log.warn("checkpoint in B is not usable, full sync: state={}", cp.state());
        }
    }

    // ---------------------------------------------------------------- source selection

    private void select() {
        if (selectStartedAt == 0) selectStartedAt = clock.getAsLong();
        SourceNode source = null;
        try {
            source = pickSource();
        } catch (Exception e) {
            waiting("source pick failed: " + e);
        }
        if (source == null) {
            notBefore = clock.getAsLong() + PICK_RETRY_MS;
            return;
        }
        if (paused) return;
        waitingSince = 0;
        log.info("source selected: host={} mode={} elapsed_ms={}", source.ip(),
            checkpoint.isEmpty() ? "full_sync" : "resume", clock.getAsLong() - selectStartedAt);
        selectStartedAt = 0;
        runSession(source);
    }

    private void waiting(String reason) {
        if (waitingSince == 0) {
            waitingSince = clock.getAsLong();
            lastWaitReminder = waitingSince;
        }
        if (!reason.equals(lastWait)) log.warn("waiting for source: {}", reason);
        lastWait = reason;
    }

    /**
     * Finds a replica of the partition that can serve the checkpoint, or null when there is none yet. When healthy
     * replicas exist but none of them can serve the checkpoint for {@code fullsyncFallbackSeconds} (typically a
     * replaced replica that did a full sync, so its backlog starts after the checkpoint), the checkpoint is dropped
     * and the partition does a full sync of its B master.
     */
    private SourceNode pickSource() throws Exception {
        SourceView view = topology.view(partition);
        SourceNode master = view.master();
        boolean unservable = false;
        if (master == null) {
            waiting("no online master (failover in progress)");
        } else {
            reconcileWithMaster(master);
            if (view.replicas().isEmpty()) waiting("no online replica yet, waiting");
            List<String> reasons = new ArrayList<>();
            for (SourceNode replica : view.replicas()) {
                String why = unusable(replica, master);
                if (why == null) {
                    lastWait = "";
                    return replica;
                }
                reasons.add(replica.ip() + " " + brief(why));
                if (selector.cannotServeCheckpoint(why)) unservable = true;
            }
            // listed but not usable (down but not yet marked so by Sentinel / CLUSTER SHARDS, lagging, wrong history)
            if (!reasons.isEmpty()) waiting("no usable replica: " + String.join(", ", reasons));
        }
        if (fullsyncFallbackSeconds > 0 && !checkpoint.isEmpty() && unservable) {
            long now = clock.getAsLong();
            if (unservableSince == 0) {
                unservableSince = now;
            } else if (now - unservableSince >= fullsyncFallbackSeconds * 1000L) {
                log.warn("no replica can serve the checkpoint, dropping it and full sync (B will be flushed): offset={} wait_s={}",
                    checkpoint.offset(), fullsyncFallbackSeconds);
                checkpoint.clear();
                unservableSince = 0;
                return pickSource();
            }
        } else {
            unservableSince = 0;
        }
        return null;
    }

    /** The reason without the numbers that change all the time, so that the same wait is logged once. */
    private static String brief(String why) {
        if (why.startsWith("backlog starts")) return "backlog starts after the checkpoint";
        int paren = why.indexOf(" (");
        return paren < 0 ? why : why.substring(0, paren);
    }

    private String unusable(SourceNode replica, SourceNode master) {
        return unusable(replica, master, checkpoint);
    }

    private String unusable(SourceNode replica, SourceNode master, Checkpoint position) {
        Map<String, String> info;
        try {
            info = topology.replicationInfo(replica.ip(), replica.port());
        } catch (Exception e) {
            return "unreachable";
        }
        return selector.unusable(info, master.ip(), position);
    }

    /** Aligns the checkpoint with the new master's history after a failover, or drops it when A diverged. */
    private void reconcileWithMaster(SourceNode master) throws IOException {
        if (checkpoint.isEmpty()) return;
        Map<String, String> info = topology.replicationInfo(master.ip(), master.port());
        String oldReplId = checkpoint.replId();
        long offset = checkpoint.offset();
        switch (selector.reconcile(info, checkpoint)) {
            case KEEP -> {
            }
            case RE_ANCHOR -> {
                log.info("checkpoint re-anchored after failover: replid={} -> {} failover_offset={} offset={}",
                    ReplIds.abbreviate(oldReplId), ReplIds.abbreviate(info.get("master_replid")),
                    info.get("second_repl_offset"), offset);
                checkpoint.reanchor(info.get("master_replid"));
            }
            case DIVERGED -> {
                log.warn("source history diverged, dropping checkpoint and full sync (B will be flushed): offset={} failover_offset={}",
                    offset, info.get("second_repl_offset"));
                checkpoint.clear();
            }
        }
    }

    // ---------------------------------------------------------------- one replication session

    private DrFatalException fatal(String message, Exception e) {
        DrFatalException fatal = new DrFatalException(message, e, DrFatalException.EXIT_FATAL);
        onFatal.accept(fatal);
        return fatal;
    }

    private void runSession(SourceNode source) {
        Configuration cfg = Configuration.defaultSetting().setRetries(1).setRetryTimeInterval(200)
            .setConnectionTimeout(1500).setReadTimeout(10000);
        if (!checkpoint.isEmpty()) {
            cfg.setReplId(checkpoint.replId());
            cfg.setReplOffset(checkpoint.offset());
        }
        String mode = checkpoint.isEmpty() ? "full_sync" : "resume";
        Replicator replicator = new RedisReplicator(source.ip(), source.port(), cfg);
        replicator.setRdbVisitor(new DumpRdbVisitor(replicator));
        registerAllCommands(replicator, source);
        applier.resetSession();
        replicator.addEventListener((rep, event) -> onEvent(cfg, event));

        expectedKeys = null;
        session = new Session(source.ip(), source.port(), cfg, replicator, Instant.now(), mode);
        try {
            if (!stopped && !paused) replicator.open();
        } catch (Exception e) {
            if (!stopped) log.warn("source session ended, reselecting: host={} cause={}", source.ip(), e.toString());
        } finally {
            endSession(cfg);
            try {
                replicator.close();
            } catch (Exception ignored) {
                // already closed
            }
        }
    }

    /**
     * One event of the replication stream. A failure to write to B (an {@link IOException}: B down, READONLY, OOM, ...)
     * is not fatal: it is reported, the session is closed and the worker starts over from the checkpoint stored in B.
     * Anything else (an event that cannot be understood or applied) is fatal.
     */
    void onEvent(Configuration cfg, Event event) {
        if (targetFailed) return; // the session is being closed; the checkpoint in B is where it continues
        try (LogContext ignored = LogContext.partition(partition)) {
            applier.onEvent(cfg, event);
            if (targetError != null && applier.writes() > writesAtFailure) {
                log.info("writing to B works again");
                targetError = null;
            }
        } catch (IOException e) {
            targetFailed(e);
        } catch (Exception e) {
            fatal("applying event " + event.getClass().getSimpleName(), e);
        }
    }

    private void targetFailed(IOException e) {
        try (LogContext ignored = LogContext.partition(partition)) {
            String cause = e.toString();
            if (!cause.equals(targetError)) {
                log.error("cannot write to B, closing the session and retrying from the checkpoint stored in B: cause={}", cause);
            }
            if (targetError == null) {
                targetErrorSince = clock.getAsLong();
                lastTargetReminder = targetErrorSince;
            }
            targetError = cause;
            writesAtFailure = applier.writes();
            targetFailed = true;
            notBefore = clock.getAsLong() + INIT_RETRY_MS;
            Session s = session;
            if (s != null) {
                try {
                    s.replicator().close();
                } catch (Exception closed) {
                    // already closed
                }
            }
        }
    }

    /** Called when a session is over, whatever the reason. */
    void endSession(Configuration cfg) {
        session = null;
        if (targetFailed) {
            // the offset is already past the command that could not be written: B knows where to continue
            targetFailed = false;
            lastInitError = "";
            state = State.INIT;
            return;
        }
        if (!"?".equals(cfg.getReplId()) && cfg.getReplOffset() >= 0 && !applier.fullSyncInProgress()) {
            checkpoint.set(cfg.getReplId(), cfg.getReplOffset());
        }
    }

    /**
     * The library skips commands it has no parser for (the offset advances but nothing is delivered), which would
     * silently lose data. Register the raw-command parser for every command the source supports.
     */
    private void registerAllCommands(Replicator replicator, SourceNode source) {
        try {
            for (Object name : (List<?>) RespClient.once(source.ip(), source.port(), "COMMAND", "LIST")) {
                replicator.addCommandParser(CommandName.name(String.valueOf(name)), new DefaultCommandParser());
            }
        } catch (Exception e) {
            throw fatal("cannot list the source's commands (Redis 7+ required)", e);
        }
    }

    // ---------------------------------------------------------------- operator: status, pause, resume, full sync

    @Override
    public WorkerStatus status() {
        Session s = session;
        Measured m = measured;
        WorkerState workerState;
        String detail;
        String writeError = targetError;
        if (paused) {
            workerState = WorkerState.PAUSED;
            detail = null;
        } else if (writeError != null) {
            workerState = WorkerState.WAITING_TARGET;
            detail = targetDetail(writeError);
        } else if (state == State.INIT) {
            workerState = WorkerState.INITIALIZING;
            detail = blankToNull(lastInitError);
        } else if (s != null) {
            workerState = applier.fullSyncInProgress() ? WorkerState.FULL_SYNC : WorkerState.STREAMING;
            detail = null;
        } else {
            workerState = WorkerState.WAITING_SOURCE;
            detail = blankToNull(lastWait);
        }
        if (detail == null) detail = m.error();

        String replId = null;
        Long offset = null;
        if (s != null) {
            String id = s.cfg().getReplId();
            if (id != null && !"?".equals(id)) replId = id;
            if (s.cfg().getReplOffset() >= 0) offset = s.cfg().getReplOffset();
        } else if (!checkpoint.isEmpty()) {
            replId = checkpoint.replId();
            offset = checkpoint.offset();
        }

        boolean fullSync = workerState == WorkerState.FULL_SYNC;
        Long expected = expectedKeys;
        Integer percent = null;
        if (fullSync && expected != null && expected > 0) {
            percent = (int) Math.min(100, applier.restored() * 100 / expected);
        }
        return new WorkerStatus(partition.index(), partition.name(), workerState, detail,
            s == null ? null : s.host(), s == null ? null : s.mode(), s == null ? null : s.startedAt(),
            replId, offset, m.persistedState(), m.persistedOffset(), m.masterOffset(),
            fullSync ? null : m.lagBytes(), applier.applied(), m.appliedPerSecond(), applier.restored(),
            fullSync ? expected : null, percent, m.asOf());
    }

    @Override
    public void refreshStatus() {
        if (stopped || state == State.INIT) return;
        try (LogContext ignored = LogContext.partition(partition)) {
            Measured prev = measured;
            String persistedState = prev.persistedState();
            Long persistedOffset = prev.persistedOffset();
            Long masterOffset = prev.masterOffset();
            StringBuilder error = new StringBuilder();
            try {
                StoredCheckpoint cp = target.loadCheckpoint();
                persistedState = cp == null ? null : cp.state();
                persistedOffset = cp == null ? null : cp.offset();
            } catch (Exception e) {
                error.append("B: ").append(e);
            }
            try {
                SourceNode master = topology.view(partition).master();
                if (master != null) {
                    String value = topology.replicationInfo(master.ip(), master.port()).get("master_repl_offset");
                    masterOffset = value == null ? null : Long.parseLong(value);
                }
            } catch (Exception e) {
                if (error.length() > 0) error.append("; ");
                error.append("A: ").append(e);
            }

            Session s = session;
            if (s != null && applier.fullSyncInProgress() && expectedKeys == null) {
                try {
                    expectedKeys = ReplicationInfoParser.keyCount((String) RespClient.once(s.host(), s.port(), "INFO", "keyspace"));
                } catch (Exception e) {
                    // no estimate; the restored count is still shown
                }
            }

            Long live = null;
            if (s != null) {
                if (s.cfg().getReplOffset() >= 0) live = s.cfg().getReplOffset();
            } else if (!checkpoint.isEmpty()) {
                live = checkpoint.offset();
            }
            Long lag = masterOffset != null && live != null ? Math.max(0, masterOffset - live) : null;

            long nowMs = clock.getAsLong();
            long applied = applier.applied();
            Double rate = null;
            if (prev.atMs() != 0 && nowMs > prev.atMs() && applied >= prev.appliedCount()) {
                rate = Math.round((applied - prev.appliedCount()) * 10_000.0 / (nowMs - prev.atMs())) / 10.0;
            }
            measured = new Measured(persistedState, persistedOffset, masterOffset, lag, rate, nowMs, applied,
                Instant.now(), error.length() == 0 ? null : error.toString());
        }
    }

    private String targetDetail(String writeError) {
        return "cannot write to B: " + writeError
            + (blankToNull(lastInitError) == null ? "" : "; reconnecting: " + lastInitError);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void requireInitialized() {
        String writeError = targetError;
        if (writeError != null) {
            throw new UnavailableException(partition.label() + "B does not take writes now, try again when it does: " + writeError);
        }
        if (stopped || state == State.INIT) {
            throw new UnavailableException(partition.label() + "still connecting to B, try again shortly");
        }
    }

    @Override
    public void pause(boolean force) {
        synchronized (control) {
            requireInitialized();
            if (paused) return;
            if (applier.fullSyncInProgress() && !force) {
                throw new ConflictException(partition.label() + "a full sync is in progress: B was flushed and the snapshot "
                    + "cannot be continued, so resume would start it over. Pause with force=true if you still want to");
            }
            try (LogContext ignored = LogContext.partition(partition)) {
                storePaused(true);
                paused = true;
                awaitIdle();
                log.warn("paused by the operator: force={}", force);
            }
        }
    }

    @Override
    public ResumeCheck resumeCheck() {
        synchronized (control) {
            requireInitialized();
            return probe().check();
        }
    }

    @Override
    public void resume() {
        synchronized (control) {
            requireInitialized();
            if (!paused) return;
            Probe probe = probe();
            if (!probe.check().resumable()) throw new ConflictException(partition.label() + probe.check().reason());
            try (LogContext ignored = LogContext.partition(partition)) {
                storePaused(false);
                // the position stored in B is the one to continue from, whatever the memory held when the pause came
                checkpoint.set(probe.stored().replId(), probe.stored().offset());
                resetWaiting();
                paused = false;
                log.info("resumed by the operator: {}", probe.check().reason());
            }
        }
    }

    @Override
    public void fullSync() {
        synchronized (control) {
            requireInitialized();
            try (LogContext ignored = LogContext.partition(partition)) {
                storePaused(true);
                paused = true;
                awaitIdle();
                try {
                    target.clearCheckpoint();
                    checkpoint.clear();
                    target.savePaused(false);
                } catch (IOException e) {
                    throw new UnavailableException(partition.label() + "cannot reach B; the partition stays paused, "
                        + "retry the full sync: " + e, e);
                }
                resetWaiting();
                paused = false;
                log.warn("full sync requested by the operator: checkpoint deleted, B will be flushed when the snapshot starts");
            }
        }
    }

    private void resetWaiting() {
        waitingSince = 0;
        unservableSince = 0;
        lastWait = "";
        notBefore = 0;
    }

    private void storePaused(boolean value) {
        try {
            target.savePaused(value);
        } catch (IOException e) {
            throw new UnavailableException(partition.label() + "cannot store the pause flag in B: " + e, e);
        }
    }

    /** Ends the running session (if any) and waits for the worker thread to leave its step. */
    private void awaitIdle() {
        long deadline = System.nanoTime() + PAUSE_WAIT_MS * 1_000_000L;
        while (true) {
            Session s = session;
            if (s != null) {
                try {
                    s.replicator().close();
                } catch (Exception ignored) {
                    // already closed
                }
            }
            if (!busy) return;
            if (System.nanoTime() > deadline) {
                throw new UnavailableException(partition.label() + "the pause is recorded but the session did not end within "
                    + PAUSE_WAIT_MS / 1000 + "s; check the status again");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UnavailableException(partition.label() + "interrupted while waiting for the session to end");
            }
        }
    }

    /** Looks at B and at A without changing anything: could this paused partition continue from its checkpoint? */
    private Probe probe() {
        if (!paused) return new Probe(new ResumeCheck(true, "not paused"), null);
        StoredCheckpoint cp;
        try {
            cp = target.loadCheckpoint();
        } catch (IOException e) {
            throw new UnavailableException(partition.label() + "cannot read the checkpoint in B: " + e, e);
        }
        if (cp == null || !cp.usable()) {
            return new Probe(new ResumeCheck(false, "B has no usable checkpoint (" + (cp == null ? "none" : "state=" + cp.state())
                + "): resuming would start a full sync that flushes B. Use full-sync if that is what you want"), cp);
        }
        Checkpoint position = new Checkpoint();
        position.set(cp.replId(), cp.offset());
        try {
            SourceView view = topology.view(partition);
            SourceNode master = view.master();
            if (master == null) {
                return new Probe(new ResumeCheck(true, "A has no online master now; the worker waits for it"), cp);
            }
            Map<String, String> masterInfo = topology.replicationInfo(master.ip(), master.port());
            switch (selector.reconcile(masterInfo, position)) {
                case DIVERGED -> {
                    return new Probe(new ResumeCheck(false, "the history of A diverged from the checkpoint "
                        + "(offset=" + cp.offset() + "): resuming would start a full sync that flushes B. "
                        + "Use full-sync if that is what you want"), cp);
                }
                case RE_ANCHOR -> position.reanchor(masterInfo.get("master_replid"));
                case KEEP -> {
                }
            }
            boolean unservable = false;
            for (SourceNode replica : view.replicas()) {
                String why = unusable(replica, master, position);
                if (why == null) {
                    return new Probe(new ResumeCheck(true, "replica " + replica.ip() + " can serve offset " + cp.offset()), cp);
                }
                if (selector.cannotServeCheckpoint(why)) unservable = true;
            }
            if (unservable) {
                return new Probe(new ResumeCheck(false, "no replica of A can serve offset " + cp.offset()
                    + " any more (the backlog was passed): resuming would start a full sync that flushes B "
                    + "after fullsync-fallback-seconds. Use full-sync if that is what you want"), cp);
            }
            return new Probe(new ResumeCheck(true, "no usable replica of A right now; the worker waits for one"), cp);
        } catch (IOException e) {
            throw new UnavailableException(partition.label() + "cannot read the topology of A: " + e, e);
        }
    }

    // ---------------------------------------------------------------- periodic checks (called by the scheduler)

    @Override
    public void checkSource() {
        Session s = session;
        if (s == null) return;
        try (LogContext ignored = LogContext.partition(partition)) {
            SourceNode master = topology.view(partition).master();
            if (master != null && master.ip().equals(s.host())) {
                log.info("source became master, switching: host={}", s.host());
                s.replicator().close();
            }
        } catch (Exception ignored) {
            // transient topology errors; the next run retries
        }
    }

    /**
     * While B does not accept writes the cause is logged once when it changes; this repeats it every
     * {@code waitLogMs} so that a long outage stays visible in the logs.
     *
     * @return true when a reminder was logged
     */
    boolean remindTargetError() {
        String writeError = targetError;
        if (writeError == null || paused || waitLogMs <= 0) return false;
        long now = clock.getAsLong();
        if (now - lastTargetReminder < waitLogMs) return false;
        lastTargetReminder = now;
        try (LogContext ignored = LogContext.partition(partition)) {
            log.warn("B has not accepted writes for {}: {}", duration(now - targetErrorSince), targetDetail(writeError));
        }
        return true;
    }

    /**
     * Same for the wait for a replica of A to read from: the reason is logged when it changes, and repeated here.
     *
     * @return true when a reminder was logged
     */
    boolean remindSourceWait() {
        String reason = lastWait;
        long since = waitingSince;
        if (since == 0 || reason == null || reason.isBlank() || paused || session != null || waitLogMs <= 0) return false;
        long now = clock.getAsLong();
        if (now - lastWaitReminder < waitLogMs) return false;
        lastWaitReminder = now;
        try (LogContext ignored = LogContext.partition(partition)) {
            log.warn("still waiting for source for {}: {}", duration(now - since), reason);
        }
        return true;
    }

    private static String duration(long ms) {
        long seconds = ms / 1000;
        return seconds < 60 ? seconds + "s" : seconds / 60 + "m" + seconds % 60 + "s";
    }

    @Override
    public void reportStat() {
        remindTargetError();
        remindSourceWait();
        Session s = session;
        if (s == null) return;
        try (LogContext ignored = LogContext.partition(partition)) {
            StoredCheckpoint cp = target.loadCheckpoint();
            log.info("stat: src={} replid={} offset={} cp={} applied={} restored={}", s.host(),
                ReplIds.abbreviate(s.cfg().getReplId()), s.cfg().getReplOffset(),
                cp == null ? "-" : cp.state() + "@" + cp.offset(), applier.applied(), applier.restored());
        } catch (Exception ignored) {
            // transient B errors; the next run retries
        }
    }
}
