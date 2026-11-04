package redis.dr.scheduler;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import redis.dr.exception.DrFatalException;
import redis.dr.service.DrEngine;
import redis.dr.service.LayoutValidator;

/**
 * Cluster mode: stops the engine when A and B stop having the same shard count and slot ranges (including a
 * resharding of A), without any automatic repair.
 */
@Component
@Profile("!tool")
@ConditionalOnProperty(prefix = "redis-dr", name = "mode", havingValue = "cluster")
public class LayoutCheckJob {
    private final LayoutValidator validator;
    private final DrEngine engine;

    public LayoutCheckJob(LayoutValidator validator, DrEngine engine) {
        this.validator = validator;
        this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${redis-dr.scheduler.layout-check-ms:5000}",
        initialDelayString = "${redis-dr.scheduler.layout-check-ms:5000}")
    public void run() {
        try {
            String problem = validator.problem();
            if (problem != null) {
                engine.fail(new DrFatalException(problem + " -> stopping, no automatic repair",
                    DrFatalException.EXIT_LAYOUT));
            }
        } catch (Exception ignored) {
            // a cluster being unreachable is handled by the workers; only a visible mismatch stops the engine
        }
    }
}
