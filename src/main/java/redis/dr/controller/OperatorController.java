package redis.dr.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import redis.dr.model.OperatorStatus;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerStatus;
import redis.dr.service.OperatorService;

/** Operator API: progress of the sync, pause / resume, forced full sync. Not part of the "tool" profile. */
@RestController
@RequestMapping("/api/v1")
@Profile("!tool")
@Tag(name = "Operator", description = "Progress of the sync and control of the replication. "
    + "`partition` is the partition index (0 in Sentinel mode, the shard order in Cluster mode); "
    + "leave it out to address every partition.")
@ApiResponse(responseCode = "400", description = "Invalid parameter or body",
    content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
@ApiResponse(responseCode = "503", description = "The engine is starting, or A / B cannot be reached",
    content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
public class OperatorController {
    private static final Logger log = LoggerFactory.getLogger(OperatorController.class);

    private final OperatorService service;

    public OperatorController(OperatorService service) {
        this.service = service;
    }

    @GetMapping("/status")
    @Operation(summary = "Status of every partition",
        description = "Served from memory and from values refreshed every `redis-dr.scheduler.stat-report-ms` "
            + "(see `asOf`); it never calls A or B. While the engine is starting `partitions` is empty.")
    @ApiResponse(responseCode = "200", description = "Engine mode and every partition",
        content = @Content(schema = @Schema(implementation = OperatorStatus.class)))
    public OperatorStatus status() {
        return service.status();
    }

    @GetMapping("/partitions/{index}")
    @Operation(summary = "Status of one partition")
    @ApiResponse(responseCode = "200", description = "Status of the partition",
        content = @Content(schema = @Schema(implementation = WorkerStatus.class)))
    @ApiResponse(responseCode = "404", description = "No such partition",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    public WorkerStatus partition(
        @Parameter(description = "Partition index", example = "0") @PathVariable @Min(0) int index) {
        return service.partition(index);
    }

    @GetMapping("/partitions/{index}/resume-check")
    @Operation(summary = "Would resume continue from the checkpoint?",
        description = "Changes nothing. Reads the checkpoint in B and the topology of A. "
            + "`resumable=false` means resume would need a full sync, which flushes B.")
    @ApiResponse(responseCode = "200", description = "Whether resume would continue from the checkpoint",
        content = @Content(schema = @Schema(implementation = ResumeCheck.class)))
    @ApiResponse(responseCode = "404", description = "No such partition",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    public ResumeCheck resumeCheck(
        @Parameter(description = "Partition index", example = "0") @PathVariable @Min(0) int index) {
        return service.resumeCheck(index);
    }

    @PostMapping("/pause")
    @Operation(summary = "Pause the replication",
        description = "Ends the session and keeps the checkpoint. The pause is stored in B, so a restarted redis-dr "
            + "stays paused. Idempotent. While paused the lag grows, and a pause longer than the replication backlog "
            + "of A makes the checkpoint unusable (resume is then refused).")
    @ApiResponse(responseCode = "200", description = "Status after the pause",
        content = @Content(schema = @Schema(implementation = OperatorStatus.class)))
    @ApiResponse(responseCode = "404", description = "No such partition",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "A full sync is loading and `force` is false",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    public OperatorStatus pause(
        @Parameter(description = "Partition index; all partitions when omitted") @RequestParam(required = false) @Min(0) Integer partition,
        @Parameter(description = "Also pause while a full sync is loading. The snapshot is lost: B stays flushed "
            + "until a full sync is done") @RequestParam(defaultValue = "false") boolean force,
        HttpServletRequest request) {
        audit(request, "pause", partition);
        return service.pause(partition, force);
    }

    @PostMapping("/resume")
    @Operation(summary = "Resume the replication from the checkpoint",
        description = "Never starts a full sync by itself: when the checkpoint cannot be continued the request is "
            + "refused with 409 and nothing is changed (use `full-sync` if that is intended). Idempotent.")
    @ApiResponse(responseCode = "200", description = "Status after the resume",
        content = @Content(schema = @Schema(implementation = OperatorStatus.class)))
    @ApiResponse(responseCode = "404", description = "No such partition",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "A partition cannot continue from its checkpoint",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    public OperatorStatus resume(
        @Parameter(description = "Partition index; all partitions when omitted") @RequestParam(required = false) @Min(0) Integer partition,
        HttpServletRequest request) {
        audit(request, "resume", partition);
        return service.resume(partition);
    }

    @PostMapping("/full-sync")
    @Operation(summary = "Delete the checkpoint and sync everything again",
        description = "**Flushes B** (only the addressed shard in Cluster mode) when the snapshot starts, and the DR "
            + "of that range is unavailable until the snapshot is loaded. The partition is paused, its checkpoint in B "
            + "deleted, and it runs again. This replaces the manual procedure of deleting the checkpoint key. "
            + "The body must confirm it.")
    @ApiResponse(responseCode = "200", description = "Status after the full sync was requested",
        content = @Content(schema = @Schema(implementation = OperatorStatus.class)))
    @ApiResponse(responseCode = "404", description = "No such partition",
        content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    public OperatorStatus fullSync(
        @Parameter(description = "Partition index; all partitions when omitted") @RequestParam(required = false) @Min(0) Integer partition,
        @RequestBody @Valid FullSyncRequest body, HttpServletRequest request) {
        audit(request, "full-sync", partition);
        return service.fullSync(partition);
    }

    /** There is no authentication: at least leave a trace of who asked for a change. */
    private static void audit(HttpServletRequest request, String action, Integer partition) {
        log.warn("operator request: action={} partition={} from={}", action, partition == null ? "all" : partition,
            request.getRemoteAddr());
    }
}
