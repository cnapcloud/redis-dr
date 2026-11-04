package redis.dr.log;

import org.slf4j.MDC;
import redis.dr.model.Partition;

/**
 * Puts the partition's log prefix ("[shard N] ", empty in Sentinel mode) into the MDC for the current thread.
 * Use in try-with-resources at every entry point that runs on a pool or library thread.
 */
public final class LogContext implements AutoCloseable {
    private static final String KEY = "partition";

    private final String previous;

    private LogContext(String previous) {
        this.previous = previous;
    }

    public static LogContext partition(Partition partition) {
        String previous = MDC.get(KEY);
        MDC.put(KEY, partition.label());
        return new LogContext(previous);
    }

    @Override
    public void close() {
        if (previous == null) MDC.remove(KEY);
        else MDC.put(KEY, previous);
    }
}
