package redis.dr.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.ErrorHandler;
import redis.dr.exception.DrFatalException;
import redis.dr.service.DrEngine;

/**
 * Error handler of all schedulers. Spring logs the errors of periodic tasks and keeps running them, so a fatal error
 * (which used to be {@code System.exit}) must be handed to the engine explicitly.
 */
@Component
public class SchedulerErrorHandler implements ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(SchedulerErrorHandler.class);

    private final ObjectProvider<DrEngine> engine; // lazy: the engine itself needs a scheduler

    public SchedulerErrorHandler(ObjectProvider<DrEngine> engine) {
        this.engine = engine;
    }

    @Override
    public void handleError(Throwable t) {
        if (t instanceof DrFatalException fatal) {
            DrEngine e = engine.getIfAvailable();
            if (e != null) {
                e.fail(fatal);
                return;
            }
        }
        log.error("scheduled task failed", t);
    }
}
