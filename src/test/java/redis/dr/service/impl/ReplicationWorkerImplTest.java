package redis.dr.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.event.Event;
import redis.dr.exception.ConflictException;
import redis.dr.exception.DrFatalException;
import redis.dr.exception.RespError;
import redis.dr.exception.UnavailableException;
import redis.dr.model.Checkpoint;
import redis.dr.model.Partition;
import redis.dr.model.SourceNode;
import redis.dr.model.SourceView;
import redis.dr.model.ResumeCheck;
import redis.dr.model.StoredCheckpoint;
import redis.dr.model.WorkerState;
import redis.dr.model.WorkerStatus;
import redis.dr.service.EventApplier;
import redis.dr.service.SourceTopology;
import redis.dr.service.TargetWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** State transitions of the worker, with a fake A topology, a fake B writer and a manual clock. */
class ReplicationWorkerImplTest {
    private static final String REPLID = "a".repeat(40);
    private static final String NEW_REPLID = "b".repeat(40);
    private static final Partition PARTITION = new Partition(0, null);
    /** Nothing listens here, so a worker that gets as far as opening a session fails fast with a fatal error. */
    private static final SourceNode DEAD_REPLICA = new SourceNode("127.0.0.1", 1);

    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final Checkpoint checkpoint = new Checkpoint();
    private final FakeTarget target = new FakeTarget();
    private final FakeTopology topology = new FakeTopology();
    private final List<DrFatalException> fatals = new ArrayList<>();

    @BeforeEach
    void healthyMaster() {
        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of());
        topology.infos.put("10.0.0.1", masterInfo(REPLID, "0".repeat(40), "-1"));
    }

    private ReplicationWorkerImpl worker(int fallbackSeconds) {
        return new ReplicationWorkerImpl(PARTITION, topology, new SourceSelectorImpl(topology), checkpoint, target,
            new EventApplierImpl(checkpoint, target), fallbackSeconds, 30_000, fatals::add, clock::get);
    }

    private void tick(ReplicationWorkerImpl worker, long advanceMs) {
        clock.addAndGet(advanceMs);
        worker.runOnce();
    }

    private static Map<String, String> masterInfo(String replId, String replId2, String secondOffset) {
        Map<String, String> info = new HashMap<>();
        info.put("role", "master");
        info.put("master_replid", replId);
        info.put("master_replid2", replId2);
        info.put("second_repl_offset", secondOffset);
        return info;
    }

    /** A replica of 10.0.0.1 whose backlog starts after offset 1000: it cannot serve a checkpoint at 1000. */
    private static Map<String, String> replicaWithShortBacklog() {
        Map<String, String> info = new HashMap<>();
        info.put("role", "slave");
        info.put("master_link_status", "up");
        info.put("master_host", "10.0.0.1");
        info.put("master_repl_offset", "5000");
        info.put("repl_backlog_active", "1");
        info.put("repl_backlog_first_byte_offset", "4000");
        info.put("master_replid", REPLID);
        info.put("master_replid2", "0".repeat(40));
        info.put("second_repl_offset", "-1");
        return info;
    }

    // ---------------------------------------------------------------- initialization

    @Test
    void resumesFromAUsablePersistedCheckpoint() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        ReplicationWorkerImpl worker = worker(0);

        worker.runOnce();

        assertThat(target.initCalls.get()).isEqualTo(1);
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(checkpoint.offset()).isEqualTo(1000);
    }

    @Test
    void aSyncingCheckpointMeansFullSync() {
        target.stored = new StoredCheckpoint("SYNCING", null, null);
        worker(0).runOnce();
        assertThat(checkpoint.isEmpty()).isTrue();
    }

    @Test
    void noCheckpointMeansFullSync() {
        worker(0).runOnce();
        assertThat(checkpoint.isEmpty()).isTrue();
        assertThat(topology.viewCalls.get()).isZero(); // initialization and selection are separate steps
    }

    @Test
    void retriesInitializationOnceASecondWhileBIsDown() {
        target.failLoads = 2;
        ReplicationWorkerImpl worker = worker(0);

        worker.runOnce();                 // fails
        worker.runOnce();                 // too early: not even tried
        assertThat(target.loadCalls.get()).isEqualTo(1);
        tick(worker, 1000);               // fails again
        assertThat(target.loadCalls.get()).isEqualTo(2);
        target.stored = new StoredCheckpoint("OK", REPLID, 7L);
        tick(worker, 1000);               // succeeds
        assertThat(target.loadCalls.get()).isEqualTo(3);
        assertThat(checkpoint.offset()).isEqualTo(7);
        assertThat(target.initCalls.get()).isEqualTo(1); // init is not repeated once it worked
    }

    // ---------------------------------------------------------------- source selection

    @Test
    void waitsWhileThereIsNoReplicaAndKeepsTheCheckpoint() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        ReplicationWorkerImpl worker = worker(30);
        worker.runOnce(); // init

        for (int i = 0; i < 5; i++) tick(worker, 600);

        assertThat(topology.viewCalls.get()).isEqualTo(5);
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(fatals).isEmpty();
    }

    @Test
    void waitsWhileTheMasterIsMissing() {
        topology.view = new SourceView(null, List.of());
        ReplicationWorkerImpl worker = worker(30);
        worker.runOnce();
        tick(worker, 600);
        assertThat(topology.viewCalls.get()).isEqualTo(1);
        assertThat(fatals).isEmpty();
    }

    @Test
    void pickErrorsAreRetriedNotFatal() {
        topology.viewFailure = new IOException("sentinel down");
        ReplicationWorkerImpl worker = worker(30);
        worker.runOnce();
        tick(worker, 600);
        tick(worker, 600);
        assertThat(topology.viewCalls.get()).isEqualTo(2);
        assertThat(fatals).isEmpty();
    }

    @Test
    void failoverWithinTheHistoryReanchorsTheCheckpoint() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        topology.infos.put("10.0.0.1", masterInfo(NEW_REPLID, REPLID, "1500"));
        ReplicationWorkerImpl worker = worker(30);
        worker.runOnce();

        tick(worker, 600);

        assertThat(checkpoint.replId()).isEqualTo(NEW_REPLID);
        assertThat(checkpoint.offset()).isEqualTo(1000);
    }

    @Test
    void divergedHistoryDropsTheCheckpoint() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        topology.infos.put("10.0.0.1", masterInfo(NEW_REPLID, REPLID, "900"));
        ReplicationWorkerImpl worker = worker(30);
        worker.runOnce();

        tick(worker, 600);

        assertThat(checkpoint.isEmpty()).isTrue();
    }

    // ---------------------------------------------------------------- full sync fallback

    /** The only replica has a backlog that starts after the checkpoint; it also is where a full sync would connect. */
    private ReplicationWorkerImpl workerWithAReplicaThatCannotServe(int fallbackSeconds) {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of(DEAD_REPLICA));
        topology.infos.put(DEAD_REPLICA.ip(), replicaWithShortBacklog());
        ReplicationWorkerImpl worker = worker(fallbackSeconds);
        worker.runOnce(); // init
        return worker;
    }

    @Test
    void keepsWaitingBeforeTheFallbackTimeout() {
        ReplicationWorkerImpl worker = workerWithAReplicaThatCannotServe(30);
        tick(worker, 600);  // starts the timer
        tick(worker, 20_000);
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(fatals).isEmpty();
    }

    @Test
    void dropsTheCheckpointOnceTheFallbackTimeoutPassedThenStartsAFullSync() {
        ReplicationWorkerImpl worker = workerWithAReplicaThatCannotServe(30);
        tick(worker, 600);  // starts the timer
        assertThat(checkpoint.replId()).isEqualTo(REPLID);

        // 31s later the checkpoint is dropped and the same replica is picked for a full sync; its session cannot be
        // opened (nothing listens on it), which is what ends the step
        assertThatThrownBy(() -> tick(worker, 31_000)).isInstanceOf(DrFatalException.class);

        assertThat(checkpoint.isEmpty()).isTrue();
    }

    @Test
    void neverFallsBackWhenTheTimeoutIsZero() {
        ReplicationWorkerImpl worker = workerWithAReplicaThatCannotServe(0);
        for (int i = 0; i < 5; i++) tick(worker, 3_600_000);
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(checkpoint.offset()).isEqualTo(1000);
        assertThat(fatals).isEmpty();
    }

    @Test
    void saysWhyAListedReplicaIsNotUsable() {
        // listed (Sentinel has not marked it down yet) but nothing answers there
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);

        tick(worker, 600);

        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_SOURCE);
        assertThat(worker.status().detail()).isEqualTo("no usable replica: 127.0.0.1 unreachable");
    }

    @Test
    void theWaitReasonDoesNotChangeWhileOnlyTheOffsetsMove() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), replicaWithShortBacklog());

        tick(worker, 600);
        String first = worker.status().detail();
        topology.infos.get(DEAD_REPLICA.ip()).put("master_repl_offset", "6000");
        topology.infos.get(DEAD_REPLICA.ip()).put("repl_backlog_first_byte_offset", "4500");
        tick(worker, 600);

        assertThat(first).isEqualTo("no usable replica: 127.0.0.1 backlog starts after the checkpoint");
        assertThat(worker.status().detail()).isEqualTo(first); // same text: logged once, not every 500 ms
    }

    @Test
    void waitsAgainOnceTheOnlyReplicaIsGone() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);
        tick(worker, 600);
        assertThat(worker.status().detail()).startsWith("no usable replica");

        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of());
        tick(worker, 600);

        assertThat(worker.status().detail()).isEqualTo("no online replica yet, waiting");
        assertThat(checkpoint.offset()).isEqualTo(1000); // nothing is dropped meanwhile
        assertThat(fatals).isEmpty();
    }

    // ---------------------------------------------------------------- session and shutdown

    private static Map<String, String> healthyReplicaOf(String masterIp) {
        Map<String, String> info = replicaWithShortBacklog();
        info.put("master_host", masterIp);
        return info;
    }

    @Test
    void aFailingSessionStartIsReportedAsFatal() {
        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of(DEAD_REPLICA));
        topology.infos.put("127.0.0.1", healthyReplicaOf("10.0.0.1"));
        ReplicationWorkerImpl worker = worker(0);
        worker.runOnce(); // init, no checkpoint: any healthy replica will do

        assertThatThrownBy(() -> tick(worker, 600)).isInstanceOf(DrFatalException.class)
            .hasMessageContaining("cannot list the source's commands");

        assertThat(fatals).hasSize(1);
        assertThat(fatals.get(0).exitCode()).isEqualTo(DrFatalException.EXIT_FATAL);
    }

    @Test
    void stoppedWorkerDoesNothingAndClosesTheTarget() {
        ReplicationWorkerImpl worker = worker(0);
        worker.stop();

        worker.runOnce();

        assertThat(target.initCalls.get()).isZero();
        assertThat(target.closed).isTrue();
    }

    @Test
    void periodicChecksAreNoOpsWithoutASession() {
        ReplicationWorkerImpl worker = worker(0);
        worker.checkSource();
        worker.reportStat();
        assertThat(topology.viewCalls.get()).isZero();
        assertThat(target.loadCalls.get()).isZero();
    }

    // ---------------------------------------------------------------- operator: status, pause, resume, full sync

    /** A replica of 10.0.0.1 whose backlog covers the checkpoint at 1000. */
    private static Map<String, String> servingReplica() {
        Map<String, String> info = replicaWithShortBacklog();
        info.put("repl_backlog_first_byte_offset", "1");
        return info;
    }

    private ReplicationWorkerImpl initializedWorker(StoredCheckpoint stored, Map<String, String> replicaInfo) {
        target.stored = stored;
        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of(DEAD_REPLICA));
        if (replicaInfo != null) topology.infos.put(DEAD_REPLICA.ip(), replicaInfo);
        ReplicationWorkerImpl worker = worker(0);
        worker.runOnce(); // init
        return worker;
    }

    @Test
    void statusFollowsTheStatesOfTheWorker() {
        ReplicationWorkerImpl worker = worker(0);
        assertThat(worker.status().state()).isEqualTo(WorkerState.INITIALIZING);

        target.failLoads = 1;
        worker.runOnce();
        assertThat(worker.status().state()).isEqualTo(WorkerState.INITIALIZING);
        assertThat(worker.status().detail()).contains("B down");

        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        tick(worker, 1000);
        tick(worker, 600); // no replica
        WorkerStatus status = worker.status();
        assertThat(status.state()).isEqualTo(WorkerState.WAITING_SOURCE);
        assertThat(status.detail()).contains("no online replica");
        assertThat(status.replId()).isEqualTo(REPLID);
        assertThat(status.offset()).isEqualTo(1000);
        assertThat(status.sourceHost()).isNull();
        assertThat(status.slots()).isNull();
    }

    @Test
    void aPausedFlagStoredInBKeepsTheWorkerPausedAfterARestart() {
        target.pausedInB = true;
        ReplicationWorkerImpl worker = worker(0);
        worker.runOnce(); // init reads the flag

        tick(worker, 600);
        tick(worker, 600);

        assertThat(worker.status().state()).isEqualTo(WorkerState.PAUSED);
        assertThat(topology.viewCalls.get()).isZero();
    }

    @Test
    void pauseStoresTheFlagInBStopsSelectingAndIsIdempotent() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);

        worker.pause(false);
        worker.pause(false);

        assertThat(target.pausedInB).isTrue();
        assertThat(target.savePausedCalls.get()).isEqualTo(1);
        assertThat(worker.status().state()).isEqualTo(WorkerState.PAUSED);
        tick(worker, 600);
        assertThat(topology.viewCalls.get()).isZero();
        assertThat(checkpoint.offset()).isEqualTo(1000); // the checkpoint is kept
    }

    @Test
    void pauseIsRefusedWhileTheWorkerStillConnectsToB() {
        ReplicationWorkerImpl worker = worker(0);
        assertThatThrownBy(() -> worker.pause(false)).isInstanceOf(UnavailableException.class);
        assertThat(target.savePausedCalls.get()).isZero();
    }

    @Test
    void pauseFailsWithoutPausingWhenBCannotStoreTheFlag() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);
        target.failSavePaused = true;

        assertThatThrownBy(() -> worker.pause(false)).isInstanceOf(UnavailableException.class);

        assertThat(worker.status().state()).isNotEqualTo(WorkerState.PAUSED);
    }

    @Test
    void resumeContinuesFromTheCheckpointInB() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        worker.pause(false);
        checkpoint.clear(); // memory lost the position; B still has it

        ResumeCheck check = worker.resumeCheck();
        worker.resume();

        assertThat(check.resumable()).isTrue();
        assertThat(target.pausedInB).isFalse();
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(checkpoint.offset()).isEqualTo(1000);
        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_SOURCE);
    }

    @Test
    void resumeWhenNotPausedChangesNothing() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        worker.resume();
        assertThat(target.savePausedCalls.get()).isZero();
        assertThat(worker.resumeCheck().resumable()).isTrue();
    }

    @Test
    void resumeIsRefusedWithoutAUsableCheckpointBecauseItWouldFlushB() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("SYNCING", null, null), servingReplica());
        worker.pause(true);

        assertThatThrownBy(worker::resume).isInstanceOf(ConflictException.class).hasMessageContaining("full-sync");

        assertThat(worker.status().state()).isEqualTo(WorkerState.PAUSED);
        assertThat(target.pausedInB).isTrue();
    }

    @Test
    void resumeIsRefusedWhenNoReplicaCanServeTheCheckpointAnyMore() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), replicaWithShortBacklog());
        worker.pause(false);

        ResumeCheck check = worker.resumeCheck();

        assertThat(check.resumable()).isFalse();
        assertThat(check.reason()).contains("backlog");
        assertThatThrownBy(worker::resume).isInstanceOf(ConflictException.class);
    }

    @Test
    void resumeIsRefusedWhenTheHistoryOfADiverged() {
        topology.infos.put("10.0.0.1", masterInfo(NEW_REPLID, REPLID, "900"));
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        worker.pause(false);

        ResumeCheck check = worker.resumeCheck();

        assertThat(check.resumable()).isFalse();
        assertThat(check.reason()).contains("diverged");
    }

    @Test
    void resumeAcceptsAFailoverWithinTheHistory() {
        topology.infos.put("10.0.0.1", masterInfo(NEW_REPLID, REPLID, "1500"));
        Map<String, String> replica = servingReplica();
        replica.put("master_replid", NEW_REPLID);
        replica.put("master_replid2", REPLID);
        replica.put("second_repl_offset", "1500");
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), replica);
        worker.pause(false);

        assertThat(worker.resumeCheck().resumable()).isTrue();
    }

    @Test
    void resumeAndItsCheckFailAsUnavailableWhenAIsUnreachable() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        worker.pause(false);
        topology.viewFailure = new IOException("sentinel down");

        assertThatThrownBy(worker::resumeCheck).isInstanceOf(UnavailableException.class);
        assertThatThrownBy(worker::resume).isInstanceOf(UnavailableException.class);
        assertThat(worker.status().state()).isEqualTo(WorkerState.PAUSED);
    }

    @Test
    void fullSyncDeletesTheCheckpointAndRunsAgainWithoutPausing() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());

        worker.fullSync();

        assertThat(target.checkpointCleared).isTrue();
        assertThat(checkpoint.isEmpty()).isTrue();
        assertThat(target.pausedInB).isFalse();
        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_SOURCE);
    }

    @Test
    void fullSyncOfAPausedWorkerRunsAgainToo() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("SYNCING", null, null), null);
        worker.pause(true);

        worker.fullSync();

        assertThat(worker.status().state()).isNotEqualTo(WorkerState.PAUSED);
        assertThat(target.pausedInB).isFalse();
    }

    @Test
    void fullSyncThatCannotReachBLeavesThePartitionPaused() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        target.failClear = true;

        assertThatThrownBy(worker::fullSync).isInstanceOf(UnavailableException.class);

        assertThat(worker.status().state()).isEqualTo(WorkerState.PAUSED);
        assertThat(target.pausedInB).isTrue();
    }

    @Test
    void refreshMeasuresTheCheckpointInBAndTheLagToTheMasterOfA() {
        topology.infos.put("10.0.0.1", withReplOffset(masterInfo(REPLID, "0".repeat(40), "-1"), 1600));
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);

        clock.addAndGet(5000);
        worker.refreshStatus();
        WorkerStatus status = worker.status();

        assertThat(status.persistedState()).isEqualTo("OK");
        assertThat(status.persistedOffset()).isEqualTo(1000);
        assertThat(status.masterOffset()).isEqualTo(1600);
        assertThat(status.lagBytes()).isEqualTo(600);
        assertThat(status.asOf()).isNotNull();
        assertThat(status.detail()).isNull();
    }

    @Test
    void refreshKeepsTheLastValuesAndReportsTheErrorWhenBOrAFail() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);
        topology.infos.put("10.0.0.1", withReplOffset(masterInfo(REPLID, "0".repeat(40), "-1"), 1600));
        worker.refreshStatus();

        target.failLoads = 1;
        topology.viewFailure = new IOException("sentinel down");
        worker.refreshStatus();
        WorkerStatus status = worker.status();

        assertThat(status.persistedOffset()).isEqualTo(1000);
        assertThat(status.masterOffset()).isEqualTo(1600);
        assertThat(status.detail()).contains("B: ").contains("A: ");
    }

    @Test
    void refreshBeforeInitializationDoesNothing() {
        ReplicationWorkerImpl worker = worker(0);
        worker.refreshStatus();
        assertThat(target.loadCalls.get()).isZero();
        assertThat(worker.status().asOf()).isNull();
    }

    private static Map<String, String> withReplOffset(Map<String, String> info, long offset) {
        info.put("master_repl_offset", String.valueOf(offset));
        return info;
    }

    // ---------------------------------------------------------------- B refuses or cannot take writes

    private static final Event ANY_EVENT = new Event() {
    };

    private ReplicationWorkerImpl workerWith(EventApplier applier) {
        return new ReplicationWorkerImpl(PARTITION, topology, new SourceSelectorImpl(topology), checkpoint, target,
            applier, 0, 30_000, fatals::add, clock::get);
    }

    /** A session that has read up to offset 1500 although B only holds 1000. */
    private static Configuration sessionAt1500() {
        Configuration cfg = Configuration.defaultSetting();
        cfg.setReplId(REPLID);
        cfg.setReplOffset(1500);
        return cfg;
    }

    @Test
    void aFailedWriteToBIsReportedWithItsCauseAndDoesNotStopTheProcess() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce(); // init

        applier.failure = new RespError("OOM command not allowed when used memory > 'maxmemory'.");
        worker.onEvent(sessionAt1500(), ANY_EVENT);

        assertThat(fatals).isEmpty();
        WorkerStatus status = worker.status();
        assertThat(status.state()).isEqualTo(WorkerState.WAITING_TARGET);
        assertThat(status.detail()).startsWith("cannot write to B: ").contains("OOM command not allowed");
        worker.onEvent(sessionAt1500(), ANY_EVENT);
        assertThat(applier.calls).isEqualTo(1); // the rest of the closing session is ignored
    }

    @Test
    void afterAFailedWriteTheWorkerContinuesFromTheCheckpointInBNotFromTheOffsetItHadRead() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce(); // init
        applier.failure = new RespError("READONLY You can't write against a read only replica.");
        Configuration cfg = sessionAt1500();

        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg);

        assertThat(checkpoint.offset()).isNotEqualTo(1500); // the command at ~1500 was not written
        tick(worker, 100);                                  // too early: B is given a second
        assertThat(target.loadCalls.get()).isEqualTo(1);
        tick(worker, 1000);                                 // reads B again
        assertThat(target.loadCalls.get()).isEqualTo(2);
        assertThat(checkpoint.replId()).isEqualTo(REPLID);
        assertThat(checkpoint.offset()).isEqualTo(1000);
        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_TARGET); // until a write works again
    }

    @Test
    void aStateWithoutAUsableCheckpointInBIsNotKeptFromMemory() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        applier.failure = new RespError("OOM");
        Configuration cfg = sessionAt1500();
        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg);
        target.stored = new StoredCheckpoint("SYNCING", null, null); // B was being rebuilt when the write failed

        tick(worker, 1000);

        assertThat(checkpoint.isEmpty()).isTrue(); // full sync again, not the stale position of memory
    }

    @Test
    void statusSaysWhenBItselfCannotBeReachedForTheRetry() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        applier.failure = new java.net.ConnectException("Connection refused");
        Configuration cfg = sessionAt1500();
        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg);

        target.failLoads = 1;
        tick(worker, 1000);

        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_TARGET);
        assertThat(worker.status().detail()).contains("ConnectException").contains("reconnecting").contains("B down");
    }

    @Test
    void theErrorClearsOnlyOnceAWriteToBSucceeds() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        applier.failure = new RespError("OOM");
        Configuration cfg = sessionAt1500();
        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg);
        tick(worker, 1000);

        applier.failure = null;
        worker.onEvent(cfg, ANY_EVENT); // e.g. a heartbeat that touches nothing in B
        assertThat(worker.status().state()).isEqualTo(WorkerState.WAITING_TARGET);

        applier.writes++;                // a command was written
        worker.onEvent(cfg, ANY_EVENT);
        assertThat(worker.status().state()).isNotEqualTo(WorkerState.WAITING_TARGET);
        assertThat(worker.status().detail()).isNull();
    }

    @Test
    void operatorChangesNameTheWriteErrorOfBInsteadOfSayingItIsStillConnecting() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        applier.failure = new RespError("OOM command not allowed");
        Configuration cfg = sessionAt1500();
        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg); // the worker is back at "reading B" while the cause is still known

        assertThatThrownBy(() -> worker.pause(false)).isInstanceOf(UnavailableException.class)
            .hasMessageContaining("does not take writes").hasMessageContaining("OOM");
        assertThatThrownBy(worker::fullSync).isInstanceOf(UnavailableException.class)
            .hasMessageContaining("OOM");
        assertThat(target.pausedInB).isFalse();
    }

    @Test
    void aLongOutageOfBIsRepeatedInTheLogEveryIntervalNotEverySecond() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        assertThat(worker.remindTargetError()).isFalse(); // B is fine

        applier.failure = new RespError("OOM");
        Configuration cfg = sessionAt1500();
        worker.onEvent(cfg, ANY_EVENT);
        worker.endSession(cfg);

        clock.addAndGet(10_000);
        assertThat(worker.remindTargetError()).isFalse(); // the ERROR itself was just logged
        clock.addAndGet(20_000);
        assertThat(worker.remindTargetError()).isTrue();  // 30 s since the failure
        clock.addAndGet(5_000);
        assertThat(worker.remindTargetError()).isFalse();
        clock.addAndGet(25_000);
        assertThat(worker.remindTargetError()).isTrue();

        applier.failure = null;
        applier.writes++;
        worker.onEvent(cfg, ANY_EVENT);
        clock.addAndGet(60_000);
        assertThat(worker.remindTargetError()).isFalse(); // B works again: nothing to remind
    }

    @Test
    void waitingForAReplicaOfAIsRepeatedInTheLogEveryInterval() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), null);
        assertThat(worker.remindSourceWait()).isFalse(); // not waiting yet

        tick(worker, 600); // listed replica that does not answer: the wait starts
        assertThat(worker.remindSourceWait()).isFalse();
        clock.addAndGet(29_000);
        assertThat(worker.remindSourceWait()).isFalse();
        clock.addAndGet(2_000);
        assertThat(worker.remindSourceWait()).isTrue();   // ~30 s of waiting
        clock.addAndGet(10_000);
        assertThat(worker.remindSourceWait()).isFalse();
        clock.addAndGet(20_000);
        assertThat(worker.remindSourceWait()).isTrue();

        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of()); // the reason changes, the wait goes on
        tick(worker, 600);
        clock.addAndGet(30_000);
        assertThat(worker.remindSourceWait()).isTrue();
    }

    @Test
    void noWaitReminderOnceASourceIsFoundOrWhilePaused() {
        ReplicationWorkerImpl worker = initializedWorker(new StoredCheckpoint("OK", REPLID, 1000L), servingReplica());
        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of());
        tick(worker, 600);
        clock.addAndGet(60_000);
        assertThat(worker.remindSourceWait()).isTrue();

        worker.pause(false);
        clock.addAndGet(60_000);
        assertThat(worker.remindSourceWait()).isFalse(); // paused by the operator: nothing to wait for

        topology.view = new SourceView(new SourceNode("10.0.0.1", 6379), List.of(DEAD_REPLICA));
        worker.resume(); // the replica from the setup can serve the checkpoint
        assertThatThrownBy(() -> tick(worker, 600)).isInstanceOf(DrFatalException.class); // got as far as a session
        clock.addAndGet(60_000);
        assertThat(worker.remindSourceWait()).isFalse(); // a replica was found: the wait is over
    }

    @Test
    void remindersCanBeSwitchedOffAndAreNotLoggedWhilePaused() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl never = new ReplicationWorkerImpl(PARTITION, topology, new SourceSelectorImpl(topology),
            checkpoint, target, applier, 0, 0, fatals::add, clock::get);
        never.runOnce();
        applier.failure = new RespError("OOM");
        never.onEvent(sessionAt1500(), ANY_EVENT);
        clock.addAndGet(3_600_000);
        assertThat(never.remindTargetError()).isFalse();
    }

    @Test
    void anEventThatCannotBeAppliedForOtherReasonsIsStillFatal() {
        target.stored = new StoredCheckpoint("OK", REPLID, 1000L);
        FakeApplier applier = new FakeApplier();
        ReplicationWorkerImpl worker = workerWith(applier);
        worker.runOnce();
        applier.failure = new IllegalStateException("unparsable command");

        worker.onEvent(sessionAt1500(), ANY_EVENT);

        assertThat(fatals).hasSize(1);
        assertThat(fatals.get(0).exitCode()).isEqualTo(DrFatalException.EXIT_FATAL);
        assertThat(worker.status().state()).isNotEqualTo(WorkerState.WAITING_TARGET);
    }

    // ---------------------------------------------------------------- fakes

    private static final class FakeApplier implements EventApplier {
        volatile Exception failure;
        volatile long writes = 0;
        int calls = 0;

        @Override
        public void onEvent(Configuration cfg, Event event) throws Exception {
            calls++;
            if (failure != null) throw failure;
        }

        @Override
        public void resetSession() {
        }

        @Override
        public boolean fullSyncInProgress() {
            return false;
        }

        @Override
        public long applied() {
            return 0;
        }

        @Override
        public long restored() {
            return 0;
        }

        @Override
        public long writes() {
            return writes;
        }
    }

    private static final class FakeTopology implements SourceTopology {
        volatile SourceView view;
        volatile IOException viewFailure;
        final Map<String, Map<String, String>> infos = new HashMap<>();
        final AtomicInteger viewCalls = new AtomicInteger();

        @Override
        public List<Partition> partitions() {
            return List.of(PARTITION);
        }

        @Override
        public SourceView view(Partition partition) throws IOException {
            viewCalls.incrementAndGet();
            if (viewFailure != null) throw viewFailure;
            return view;
        }

        @Override
        public String resolveIp(String host) {
            return host;
        }

        @Override
        public Map<String, String> replicationInfo(String ip, int port) throws IOException {
            Map<String, String> info = infos.get(ip);
            if (info == null) throw new IOException("unreachable " + ip);
            return info;
        }
    }

    private static final class FakeTarget implements TargetWriter {
        volatile StoredCheckpoint stored;
        volatile int failLoads = 0;
        volatile boolean closed = false;
        volatile boolean pausedInB = false;
        volatile boolean failSavePaused = false;
        volatile boolean failClear = false;
        volatile boolean checkpointCleared = false;
        final AtomicInteger savePausedCalls = new AtomicInteger();
        final AtomicInteger initCalls = new AtomicInteger();
        final AtomicInteger loadCalls = new AtomicInteger();

        @Override
        public void init() {
            initCalls.incrementAndGet();
        }

        @Override
        public StoredCheckpoint loadCheckpoint() throws IOException {
            loadCalls.incrementAndGet();
            if (failLoads > 0) {
                failLoads--;
                throw new IOException("B down");
            }
            return stored;
        }

        @Override
        public boolean loadPaused() {
            return pausedInB;
        }

        @Override
        public void savePaused(boolean paused) throws IOException {
            if (failSavePaused) throw new IOException("B down");
            savePausedCalls.incrementAndGet();
            pausedInB = paused;
        }

        @Override
        public void clearCheckpoint() throws IOException {
            if (failClear) throw new IOException("B down");
            checkpointCleared = true;
            stored = null;
        }

        @Override
        public void beginFullSync() {
        }

        @Override
        public void completeFullSync(String replId, long offset) {
        }

        @Override
        public void restore(int db, byte[] key, long absoluteTtlMillis, byte[] dump) {
        }

        @Override
        public void apply(String replId, long offset, int db, byte[] command, byte[][] args) {
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
