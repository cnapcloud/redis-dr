package redis.dr.scheduler;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.dr.service.DrEngine;
import redis.dr.service.ReplicationWorker;

/** Periodic refresh of the operator status, and the stat line of every worker that has a running session. */
@Component
@Profile("!tool")
public class StatReportJob {
    private final DrEngine engine;

    public StatReportJob(DrEngine engine) {
        this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${redis-dr.scheduler.stat-report-ms:5000}",
        initialDelayString = "${redis-dr.scheduler.stat-report-ms:5000}")
    public void run() {
        engine.workers().forEach(worker -> {
            worker.refreshStatus();
            worker.reportStat();
        });
    }
}
