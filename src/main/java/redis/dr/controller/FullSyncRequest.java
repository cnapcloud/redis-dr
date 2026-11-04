package redis.dr.controller;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Confirmation that the operator knows a full sync flushes B. */
@Schema(description = "A full sync deletes the checkpoint and flushes the data of B; confirm it explicitly")
public record FullSyncRequest(
    @Schema(description = "Must be exactly FLUSH_B", example = "FLUSH_B", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "is required")
    @Pattern(regexp = "FLUSH_B", message = "must be exactly FLUSH_B")
    String confirm) {
}
