package redis.dr.model;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Progress of one partition. Built from memory and from values refreshed every {@code stat-report-ms}. */
@Schema(description = "Replication status of one partition (the whole deployment in Sentinel mode, one shard in Cluster mode)")
public record WorkerStatus(
    @Schema(description = "Partition index (shard order by first slot; 0 in Sentinel mode)", example = "0")
    int partition,
    @Schema(description = "Slot ranges of the shard; null in Sentinel mode", example = "0-5460", nullable = true)
    String slots,
    @Schema(description = "Current state") WorkerState state,
    @Schema(description = "Why the worker is waiting or the last error that is being retried", nullable = true)
    String detail,
    @Schema(description = "Replica of A that is read; null when there is no session", nullable = true)
    String sourceHost,
    @Schema(description = "resume = continue from the checkpoint, full_sync = rebuild B; null when there is no session",
        allowableValues = {"resume", "full_sync"}, nullable = true)
    String sessionMode,
    @Schema(description = "Start of the current session", nullable = true) Instant sessionStartedAt,
    @Schema(description = "Replication id of the position", nullable = true) String replId,
    @Schema(description = "Replication offset read from A (live during a session, else the checkpoint in memory)",
        nullable = true)
    Long offset,
    @Schema(description = "State of the checkpoint stored in B (OK or SYNCING); null when none", nullable = true)
    String persistedState,
    @Schema(description = "Offset of the checkpoint stored in B", nullable = true) Long persistedOffset,
    @Schema(description = "master_repl_offset of the master of A", nullable = true) Long masterOffset,
    @Schema(description = "masterOffset - offset in bytes; null during a full sync or when unknown", nullable = true)
    Long lagBytes,
    @Schema(description = "Commands applied to B since the process started") long applied,
    @Schema(description = "Commands applied per second between the last two refreshes", nullable = true)
    Double appliedPerSecond,
    @Schema(description = "Keys restored by the current or last full sync") long restored,
    @Schema(description = "Keys of the source replica when the full sync started (an estimate: TTLs and writes change it)",
        nullable = true)
    Long expectedKeys,
    @Schema(description = "restored / expectedKeys, capped at 100. Approximate", nullable = true)
    Integer approxPercent,
    @Schema(description = "Time the cached values (persisted*, masterOffset, lag, rate) were measured", nullable = true)
    Instant asOf) {
}
