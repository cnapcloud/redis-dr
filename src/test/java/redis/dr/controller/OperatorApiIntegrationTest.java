package redis.dr.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import redis.dr.service.DrEngine;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The whole application with the real engine against unreachable Redis addresses (nothing listens on 127.0.0.1:1):
 * the API is wired to the engine, a worker that cannot reach B is INITIALIZING and refuses changes with 503, and the
 * OpenAPI document describes every endpoint.
 */
@SpringBootTest(properties = {"redis-dr.mode=sentinel", "redis-dr.sentinel.source.nodes=127.0.0.1:1",
    "redis-dr.sentinel.target.nodes=127.0.0.1:1"})
@AutoConfigureMockMvc
class OperatorApiIntegrationTest {
    @Autowired
    private MockMvc mvc;
    @Autowired
    private DrEngine engine;

    @Test
    void aWorkerThatCannotReachBIsInitializingAndRefusesChanges() throws Exception {
        await().atMost(Duration.ofSeconds(10)).until(() -> engine.workers().size() == 1);

        mvc.perform(get("/api/v1/status")).andExpect(status().isOk())
            .andExpect(jsonPath("$.mode").value("sentinel"))
            .andExpect(jsonPath("$.engine").value("RUNNING"))
            .andExpect(jsonPath("$.partitions[0].state").value("INITIALIZING"))
            .andExpect(jsonPath("$.partitions[0].detail").isNotEmpty());
        mvc.perform(post("/api/v1/pause")).andExpect(status().isServiceUnavailable());
        mvc.perform(post("/api/v1/resume").param("partition", "0")).andExpect(status().isServiceUnavailable());
        mvc.perform(post("/api/v1/full-sync").contentType(MediaType.APPLICATION_JSON).content("{\"confirm\":\"FLUSH_B\"}"))
            .andExpect(status().isServiceUnavailable());
        mvc.perform(get("/api/v1/partitions/5")).andExpect(status().isNotFound());
    }

    @Test
    void openApiDescribesTheOperatorEndpoints() throws Exception {
        String api = mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse().getContentAsString();

        assertThat(api).contains("/api/v1/status", "/api/v1/partitions/{index}", "/api/v1/partitions/{index}/resume-check",
            "/api/v1/pause", "/api/v1/resume", "/api/v1/full-sync", "FLUSH_B", "WAITING_SOURCE", "operator API");
    }

    @Test
    void swaggerUiIsServed() throws Exception {
        mvc.perform(get("/swagger-ui/index.html")).andExpect(status().isOk());
    }
}
