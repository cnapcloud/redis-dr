package redis.dr.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ErrorHandler;
import redis.dr.config.DrProperties;
import redis.dr.exception.DrFatalException;
import redis.dr.model.Checkpoint;
import redis.dr.model.Partition;
import redis.dr.service.DrEngine;
import redis.dr.service.EventApplier;
import redis.dr.service.LayoutValidator;
import redis.dr.service.ReplicationWorker;
import redis.dr.service.SourceSelector;
import redis.dr.service.SourceTopology;
import redis.dr.service.TargetWriter;
import redis.dr.service.TargetWriterFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts with the application: waits until A (and B, in cluster mode) answer, checks the layout, then schedules one
 * {@link ReplicationWorker} per partition on a scheduler of its own (a session keeps a thread busy, so the workers
 * must not share a pool with the periodic jobs). Stops everything on shutdown.
 */
public class DrEngineImpl implements DrEngine, SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(DrEngineImpl.class);

    private static final long BOOTSTRAP_RETRY_MS = 1000;

    private final DrProperties properties;
    private final SourceTopology topology;
    private final SourceSelector selector;
    private final TargetWriterFactory targetWriterFactory;
    private final LayoutValidator layoutValidator; // null outside cluster mode
    private final TaskScheduler scheduler;
    private final ErrorHandler errorHandler;
    private final ConfigurableApplicationContext context;

    private volatile boolean running = false;
    private volatile List<ReplicationWorker> workers = List.of();
    private ThreadPoolTaskScheduler workerScheduler;
    private final List<ScheduledFuture<?>> futures = new CopyOnWriteArrayList<>();
    private final AtomicBoolean failing = new AtomicBoolean(false);
    private String lastBootstrapError = "";

    public DrEngineImpl(DrProperties properties, SourceTopology topology, SourceSelector selector,
                        TargetWriterFactory targetWriterFactory, LayoutValidator layoutValidator,
                        TaskScheduler scheduler, ErrorHandler errorHandler, ConfigurableApplicationContext context) {
        this.properties = properties;
        this.topology = topology;
        this.selector = selector;
        this.targetWriterFactory = targetWriterFactory;
        this.layoutValidator = layoutValidator;
        this.scheduler = scheduler;
        this.errorHandler = errorHandler;
        this.context = context;
    }

    @Override
    public List<ReplicationWorker> workers() {
        return workers;
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public void start() {
        running = true;
        scheduler.schedule(this::bootstrap, Instant.now());
    }

    @Override
    public synchronized void stop() {
        running = false;
        futures.forEach(f -> f.cancel(false));
        futures.clear();
        workers.forEach(ReplicationWorker::stop);
        if (workerScheduler != null) workerScheduler.shutdown();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Checks the clusters and starts the workers; retried every second while A or B cannot be reached. */
    private void bootstrap() {
        if (!running) return;
        List<Partition> partitions;
        try {
            if (layoutValidator != null) {
                String problem = layoutValidator.problem();
                if (problem != null) {
                    fail(new DrFatalException(problem + " -> not starting", DrFatalException.EXIT_LAYOUT));
                    return;
                }
            }
            partitions = topology.partitions();
        } catch (Exception e) {
            String error = e.toString();
            if (!error.equals(lastBootstrapError)) log.warn("cluster not reachable, retrying: cause={}", error);
            lastBootstrapError = error;
            scheduler.schedule(this::bootstrap, Instant.now().plusMillis(BOOTSTRAP_RETRY_MS));
            return;
        }
        if (layoutValidator != null) {
            log.info("layout ok: shards={} names={}", partitions.size(), partitions.stream().map(Partition::name).toList());
        }
        startWorkers(partitions);
    }

    /** The read timeout of the replication sockets, replacing an invalid configured one (with a warning). */
    private int effectiveReadTimeoutSeconds() {
        DrProperties.Replication replication = properties.replication();
        int effective = replication.effectiveReadTimeoutSeconds();
        if (!replication.readTimeoutValid()) {
            log.warn("redis-dr.replication.read-timeout-seconds={} must be more than twice "
                    + "redis-dr.replication.source-ping-period-seconds={}, using {} instead",
                replication.readTimeoutSeconds(), replication.sourcePingPeriodSeconds(), effective);
        }
        log.info("replication read timeout: {}s (source ping period {}s)", effective,
            replication.sourcePingPeriodSeconds());
        return effective;
    }

    private synchronized void startWorkers(List<Partition> partitions) {
        if (!running) return;
        workerScheduler = new ThreadPoolTaskScheduler();
        workerScheduler.setPoolSize(partitions.size());
        workerScheduler.setThreadNamePrefix("dr-worker-");
        workerScheduler.setErrorHandler(errorHandler);
        workerScheduler.initialize();

        int readTimeoutMs = effectiveReadTimeoutSeconds() * 1000;
        List<ReplicationWorker> created = new ArrayList<>();
        for (Partition partition : partitions) {
            Checkpoint checkpoint = new Checkpoint();
            TargetWriter target = targetWriterFactory.create(partition);
            EventApplier applier = new EventApplierImpl(checkpoint, target);
            created.add(new ReplicationWorkerImpl(partition, topology, selector, checkpoint, target, applier,
                properties.fullsyncFallbackSeconds(), properties.scheduler().waitLogMs(), readTimeoutMs, this::fail));
        }
        workers = List.copyOf(created);
        Duration delay = Duration.ofMillis(properties.scheduler().workerRetryMs());
        for (ReplicationWorker worker : created) {
            futures.add(workerScheduler.scheduleWithFixedDelay(worker::runOnce, delay));
        }
    }

    // ---------------------------------------------------------------- failure

    @Override
    public void fail(DrFatalException cause) {
        if (!failing.compareAndSet(false, true)) return;
        log.error("fatal, shutting down: {}", cause.getMessage(), cause.getCause());
        // closing the context stops this engine; do it off the calling thread, which may be one of the workers
        Thread exit = new Thread(() -> System.exit(SpringApplication.exit(context, cause::exitCode)), "dr-exit");
        exit.start();
    }
}
