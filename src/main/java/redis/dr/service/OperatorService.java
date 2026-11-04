package redis.dr.service;

import redis.dr.model.OperatorStatus;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerStatus;

/**
 * What the operator can see and do: progress of the sync, pause / resume, forced full sync. A {@code partition} of
 * null means every partition. Actions are checked for all partitions before any of them is changed.
 */
public interface OperatorService {
    OperatorStatus status();

    /** @throws redis.dr.exception.NotFoundException no such partition */
    WorkerStatus partition(int index);

    /** @throws redis.dr.exception.ConflictException a full sync is in progress and {@code force} is false */
    OperatorStatus pause(Integer partition, boolean force);

    /** @throws redis.dr.exception.ConflictException a partition cannot continue from its checkpoint */
    OperatorStatus resume(Integer partition);

    ResumeCheck resumeCheck(int partition);

    /** Deletes the checkpoint of the partitions in B and restarts them: B is flushed when the snapshot starts. */
    OperatorStatus fullSync(Integer partition);
}
