package redis.dr.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import redis.dr.exception.ConflictException;
import redis.dr.exception.NotFoundException;
import redis.dr.exception.UnavailableException;
import redis.dr.model.OperatorStatus;
import redis.dr.model.ResumeCheck;
import redis.dr.model.WorkerState;
import redis.dr.model.WorkerStatus;
import redis.dr.service.OperatorService;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** HTTP contract of the operator API: status codes, validation errors, JSON shape. The service is mocked. */
@WebMvcTest(OperatorController.class)
class OperatorControllerTest {
    @Autowired
    private MockMvc mvc;
    @MockitoBean
    private OperatorService service;

    private static WorkerStatus streaming(int partition) {
        return new WorkerStatus(partition, "0-5460", WorkerState.STREAMING, null, "10.0.0.5", "resume",
            Instant.parse("2026-11-04T01:02:03Z"), "a".repeat(40), 1200L, "OK", 1190L, 1500L, 300L, 42, 12.5, 0, null,
            null, Instant.parse("2026-11-04T01:02:05Z"));
    }

    private static OperatorStatus running(WorkerStatus... partitions) {
        return new OperatorStatus("cluster", "RUNNING", List.of(partitions));
    }

    // ---------------------------------------------------------------- status

    @Test
    void statusListsThePartitions() throws Exception {
        when(service.status()).thenReturn(running(streaming(0), streaming(1)));

        mvc.perform(get("/api/v1/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.mode").value("cluster"))
            .andExpect(jsonPath("$.engine").value("RUNNING"))
            .andExpect(jsonPath("$.partitions.length()").value(2))
            .andExpect(jsonPath("$.partitions[0].state").value("STREAMING"))
            .andExpect(jsonPath("$.partitions[0].slots").value("0-5460"))
            .andExpect(jsonPath("$.partitions[0].lagBytes").value(300))
            .andExpect(jsonPath("$.partitions[0].sessionStartedAt").value("2026-11-04T01:02:03Z"))
            .andExpect(jsonPath("$.partitions[0].appliedPerSecond").value(12.5));
    }

    @Test
    void partitionStatusAndNotFound() throws Exception {
        when(service.partition(0)).thenReturn(streaming(0));
        when(service.partition(7)).thenThrow(new NotFoundException("no partition 7"));

        mvc.perform(get("/api/v1/partitions/0")).andExpect(status().isOk()).andExpect(jsonPath("$.partition").value(0));
        mvc.perform(get("/api/v1/partitions/7")).andExpect(status().isNotFound())
            .andExpect(jsonPath("$.detail").value("no partition 7"));
    }

    @Test
    void aNegativeOrNonNumericPartitionIsABadRequest() throws Exception {
        mvc.perform(get("/api/v1/partitions/-1")).andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors[0].field").value("index"));
        mvc.perform(get("/api/v1/partitions/abc")).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void resumeCheckIsServed() throws Exception {
        when(service.resumeCheck(1)).thenReturn(new ResumeCheck(false, "no usable checkpoint"));

        mvc.perform(get("/api/v1/partitions/1/resume-check")).andExpect(status().isOk())
            .andExpect(jsonPath("$.resumable").value(false))
            .andExpect(jsonPath("$.reason").value("no usable checkpoint"));
    }

    // ---------------------------------------------------------------- pause / resume

    @Test
    void pauseWithoutParametersAddressesAllPartitionsWithoutForce() throws Exception {
        when(service.pause(null, false)).thenReturn(running(streaming(0)));

        mvc.perform(post("/api/v1/pause")).andExpect(status().isOk()).andExpect(jsonPath("$.partitions.length()").value(1));

        verify(service).pause(isNull(), anyBoolean());
        verify(service).pause(null, false);
    }

    @Test
    void pauseOfOnePartitionWithForce() throws Exception {
        when(service.pause(2, true)).thenReturn(running(streaming(2)));

        mvc.perform(post("/api/v1/pause").param("partition", "2").param("force", "true")).andExpect(status().isOk());

        verify(service).pause(2, true);
    }

    @Test
    void pauseValidatesItsParameters() throws Exception {
        mvc.perform(post("/api/v1/pause").param("partition", "-3")).andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors[0].field").value("partition"));
        mvc.perform(post("/api/v1/pause").param("force", "maybe")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/v1/pause").param("partition", "x")).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void pauseDuringAFullSyncIsAConflict() throws Exception {
        when(service.pause(null, false)).thenThrow(new ConflictException("a full sync is in progress"));

        mvc.perform(post("/api/v1/pause")).andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value(409))
            .andExpect(jsonPath("$.detail").value("a full sync is in progress"));
    }

    @Test
    void resumeMapsServiceErrors() throws Exception {
        when(service.resume(0)).thenThrow(new ConflictException("no usable checkpoint"));
        when(service.resume(1)).thenThrow(new UnavailableException("B is down"));
        when(service.resume(null)).thenReturn(running(streaming(0)));

        mvc.perform(post("/api/v1/resume").param("partition", "0")).andExpect(status().isConflict());
        mvc.perform(post("/api/v1/resume").param("partition", "1")).andExpect(status().isServiceUnavailable())
            .andExpect(jsonPath("$.detail").value("B is down"));
        mvc.perform(post("/api/v1/resume")).andExpect(status().isOk());
    }

    // ---------------------------------------------------------------- full sync

    private static final String CONFIRM = "{\"confirm\":\"FLUSH_B\"}";

    @Test
    void fullSyncNeedsTheConfirmation() throws Exception {
        mvc.perform(post("/api/v1/full-sync")).andExpect(status().isBadRequest()); // no body
        mvc.perform(post("/api/v1/full-sync").contentType(MediaType.APPLICATION_JSON).content("{}"))
            .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors[0].field").value("confirm"));
        mvc.perform(post("/api/v1/full-sync").contentType(MediaType.APPLICATION_JSON).content("{\"confirm\":\"yes\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors[0].message").value("must be exactly FLUSH_B"));
        mvc.perform(post("/api/v1/full-sync").contentType(MediaType.APPLICATION_JSON).content("{not json"))
            .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void fullSyncValidatesThePartitionToo() throws Exception {
        mvc.perform(post("/api/v1/full-sync").param("partition", "-1").contentType(MediaType.APPLICATION_JSON)
                .content(CONFIRM))
            .andExpect(status().isBadRequest());
        verify(service, never()).fullSync(anyInt());
    }

    @Test
    void confirmedFullSyncIsForwarded() throws Exception {
        when(service.fullSync(1)).thenReturn(running(streaming(1)));

        mvc.perform(post("/api/v1/full-sync").param("partition", "1").contentType(MediaType.APPLICATION_JSON)
                .content(CONFIRM))
            .andExpect(status().isOk());

        verify(service).fullSync(1);
    }
}
