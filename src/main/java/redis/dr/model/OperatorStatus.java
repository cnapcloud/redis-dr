package redis.dr.model;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/** Status of the whole engine: its mode and every partition. */
@Schema(description = "Status of the engine and of all its partitions")
public record OperatorStatus(
    @Schema(description = "redis-dr.mode", allowableValues = {"sentinel", "cluster"}) String mode,
    @Schema(description = "STARTING while A (and B in cluster mode) are not reachable and no worker runs yet, then RUNNING",
        allowableValues = {"STARTING", "RUNNING"})
    String engine,
    @Schema(description = "One entry per partition, in partition order") List<WorkerStatus> partitions) {
}
