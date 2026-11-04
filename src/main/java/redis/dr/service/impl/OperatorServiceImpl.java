package redis.dr.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.dr.config.DrProperties;
import redis.dr.exception.ConflictException;
import redis.dr.exception.NotFoundException;
import redis.dr.exception.UnavailableException;
import redis.dr.model.OperatorStatus;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerState;
import redis.dr.model.WorkerStatus;
import redis.dr.service.DrEngine;
import redis.dr.service.OperatorService;
import redis.dr.service.ReplicationWorker;

import java.util.ArrayList;
import java.util.List;

public class OperatorServiceImpl implements OperatorService {
    private static final Logger log = LoggerFactory.getLogger(OperatorServiceImpl.class);

    private final DrEngine engine;
    private final DrProperties.Mode mode;

    public OperatorServiceImpl(DrEngine engine, DrProperties properties) {
        this.engine = engine;
        this.mode = properties.mode();
    }

    @Override
    public OperatorStatus status() {
        List<WorkerStatus> partitions = engine.workers().stream().map(ReplicationWorker::status).toList();
        return new OperatorStatus(mode.name().toLowerCase(), partitions.isEmpty() ? "STARTING" : "RUNNING", partitions);
    }

    @Override
    public WorkerStatus partition(int index) {
        return worker(index).status();
    }

    @Override
    public OperatorStatus pause(Integer partition, boolean force) {
        List<ReplicationWorker> targets = select(partition);
        if (!force) {
            List<Integer> loading = targets.stream().map(ReplicationWorker::status)
                .filter(s -> s.state() == WorkerState.FULL_SYNC).map(WorkerStatus::partition).toList();
            if (!loading.isEmpty()) {
                throw new ConflictException("a full sync is in progress in partition " + loading + ": B was flushed and the "
                    + "snapshot cannot be continued, so resume would start it over. Pause with force=true if you still want to");
            }
        }
        log.warn("operator: pause partitions={} force={}", indexes(targets), force);
        targets.forEach(w -> w.pause(force));
        return status();
    }

    @Override
    public OperatorStatus resume(Integer partition) {
        List<ReplicationWorker> targets = select(partition);
        List<String> blocked = new ArrayList<>();
        for (ReplicationWorker worker : targets) {
            ResumeCheck check = worker.resumeCheck();
            if (!check.resumable()) blocked.add("partition " + worker.partition().index() + ": " + check.reason());
        }
        if (!blocked.isEmpty()) throw new ConflictException("cannot resume, nothing was changed: " + String.join("; ", blocked));
        log.info("operator: resume partitions={}", indexes(targets));
        targets.forEach(ReplicationWorker::resume);
        return status();
    }

    @Override
    public ResumeCheck resumeCheck(int partition) {
        return worker(partition).resumeCheck();
    }

    @Override
    public OperatorStatus fullSync(Integer partition) {
        List<ReplicationWorker> targets = select(partition);
        log.warn("operator: full sync partitions={} (B will be flushed)", indexes(targets));
        targets.forEach(ReplicationWorker::fullSync);
        return status();
    }

    private List<ReplicationWorker> workers() {
        List<ReplicationWorker> workers = engine.workers();
        if (workers.isEmpty()) {
            throw new UnavailableException("the engine is starting: waiting until A (and B in cluster mode) answer");
        }
        return workers;
    }

    private ReplicationWorker worker(int index) {
        return workers().stream().filter(w -> w.partition().index() == index).findFirst()
            .orElseThrow(() -> new NotFoundException("no partition " + index + " (partitions: " + indexes(workers()) + ")"));
    }

    private List<ReplicationWorker> select(Integer partition) {
        return partition == null ? workers() : List.of(worker(partition));
    }

    private static List<Integer> indexes(List<ReplicationWorker> workers) {
        return workers.stream().map(w -> w.partition().index()).toList();
    }
}
