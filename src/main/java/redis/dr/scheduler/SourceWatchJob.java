package redis.dr.scheduler;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.dr.service.DrEngine;
import redis.dr.service.ReplicationWorker;

/** Moves a worker away from its source replica as soon as that replica is promoted to master. */
@Component
@Profile("!tool")
public class SourceWatchJob {
    private final DrEngine engine;

    public SourceWatchJob(DrEngine engine) {
        this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${redis-dr.scheduler.source-watch-ms:1000}",
        initialDelayString = "${redis-dr.scheduler.source-watch-ms:1000}")
    public void run() {
        engine.workers().forEach(ReplicationWorker::checkSource);
    }
}
