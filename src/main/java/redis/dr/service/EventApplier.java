package redis.dr.service;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.event.Event;

/** Applies the events of one replication session (snapshot keys and streamed commands) to B. */
public interface EventApplier {
    void onEvent(Configuration cfg, Event event) throws Exception;

    /** Forgets the state of the previous session (selected db, full sync in progress). */
    void resetSession();

    /** True between the start of a FULLRESYNC and the end of its snapshot. */
    boolean fullSyncInProgress();

    long applied();

    /** Number of successful writes to B (commands, restored keys, full sync markers); never decreases. */
    long writes();

    long restored();
}
