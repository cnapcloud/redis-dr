package redis.dr.service.impl;

import org.junit.jupiter.api.Test;
import redis.dr.config.DrProperties;
import redis.dr.exception.ConflictException;
import redis.dr.exception.NotFoundException;
import redis.dr.exception.UnavailableException;
import redis.dr.model.OperatorStatus;
import redis.dr.model.Partition;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerState;
import redis.dr.model.WorkerStatus;
import redis.dr.service.DrEngine;
import redis.dr.service.ReplicationWorker;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Partition selection and the "check everything before changing anything" rule, with mocked workers. */
class OperatorServiceImplTest {
    private final DrEngine engine = mock(DrEngine.class);
    private final DrProperties properties = new DrProperties(DrProperties.Mode.CLUSTER, new DrProperties.Sentinel(0,
        new DrProperties.Site(List.of("a:1"), "m", ""), new DrProperties.Site(List.of("b:1"), "m", "")),
        new DrProperties.Cluster(new DrProperties.Endpoint(List.of("a:1")), new DrProperties.Endpoint(List.of("b:1")), 20),
        new DrProperties.Scheduler(200, 1000, 5000, 5000, 30000), new DrProperties.Replication(10, 30));
    private final OperatorServiceImpl service = new OperatorServiceImpl(engine, properties);

    private static WorkerStatus status(int partition, WorkerState state) {
        return new WorkerStatus(partition, "s" + partition, state, null, null, null, null, null, null, null, null, null,
            null, 0, null, 0, null, null, null);
    }

    private static ReplicationWorker worker(int index, WorkerState state) {
        ReplicationWorker worker = mock(ReplicationWorker.class);
        when(worker.partition()).thenReturn(new Partition(index, "s" + index));
        when(worker.status()).thenReturn(status(index, state));
        when(worker.resumeCheck()).thenReturn(new ResumeCheck(true, "ok"));
        return worker;
    }

    @Test
    void statusIsStartingWithoutWorkersAndRunningWithThem() {
        when(engine.workers()).thenReturn(List.of());
        OperatorStatus starting = service.status();
        assertThat(starting.engine()).isEqualTo("STARTING");
        assertThat(starting.mode()).isEqualTo("cluster");
        assertThat(starting.partitions()).isEmpty();

        ReplicationWorker streaming = worker(0, WorkerState.STREAMING);
        when(engine.workers()).thenReturn(List.of(streaming));
        assertThat(service.status().engine()).isEqualTo("RUNNING");
        assertThat(service.status().partitions()).hasSize(1);
    }

    @Test
    void actionsAreUnavailableUntilTheEngineStarted() {
        when(engine.workers()).thenReturn(List.of());
        assertThatThrownBy(() -> service.pause(null, false)).isInstanceOf(UnavailableException.class);
        assertThatThrownBy(() -> service.resume(0)).isInstanceOf(UnavailableException.class);
        assertThatThrownBy(() -> service.fullSync(null)).isInstanceOf(UnavailableException.class);
        assertThatThrownBy(() -> service.partition(0)).isInstanceOf(UnavailableException.class);
    }

    @Test
    void anUnknownPartitionIsNotFound() {
        ReplicationWorker streaming = worker(0, WorkerState.STREAMING);
        when(engine.workers()).thenReturn(List.of(streaming));
        assertThatThrownBy(() -> service.partition(4)).isInstanceOf(NotFoundException.class).hasMessageContaining("[0]");
        assertThatThrownBy(() -> service.pause(4, false)).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> service.resumeCheck(4)).isInstanceOf(NotFoundException.class);
    }

    @Test
    void pauseWithoutPartitionPausesAllAndOnePartitionOnlyThatOne() {
        ReplicationWorker w0 = worker(0, WorkerState.STREAMING), w1 = worker(1, WorkerState.STREAMING);
        when(engine.workers()).thenReturn(List.of(w0, w1));

        service.pause(1, false);
        verify(w1).pause(false);
        verify(w0, never()).pause(false);

        service.pause(null, true);
        verify(w0).pause(true);
        verify(w1).pause(true);
    }

    @Test
    void pauseChecksAllPartitionsForAFullSyncBeforePausingAny() {
        ReplicationWorker w0 = worker(0, WorkerState.STREAMING), w1 = worker(1, WorkerState.FULL_SYNC);
        when(engine.workers()).thenReturn(List.of(w0, w1));

        assertThatThrownBy(() -> service.pause(null, false)).isInstanceOf(ConflictException.class)
            .hasMessageContaining("[1]");

        verify(w0, never()).pause(false);
        verify(w1, never()).pause(false);
        service.pause(null, true); // force goes through
        verify(w1).pause(true);
    }

    @Test
    void resumeChangesNothingWhenOnePartitionCannotContinue() {
        ReplicationWorker w0 = worker(0, WorkerState.PAUSED), w1 = worker(1, WorkerState.PAUSED);
        when(w1.resumeCheck()).thenReturn(new ResumeCheck(false, "no usable checkpoint"));
        when(engine.workers()).thenReturn(List.of(w0, w1));

        assertThatThrownBy(() -> service.resume(null)).isInstanceOf(ConflictException.class)
            .hasMessageContaining("nothing was changed").hasMessageContaining("partition 1: no usable checkpoint");

        verify(w0, never()).resume();
        verify(w1, never()).resume();
    }

    @Test
    void resumeAndFullSyncReachTheSelectedWorkers() {
        ReplicationWorker w0 = worker(0, WorkerState.PAUSED), w1 = worker(1, WorkerState.PAUSED);
        when(engine.workers()).thenReturn(List.of(w0, w1));

        service.resume(null);
        verify(w0).resume();
        verify(w1).resume();

        service.fullSync(0);
        verify(w0).fullSync();
        verify(w1, never()).fullSync();
    }
}
