package redis.dr.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Swagger UI at {@code /swagger-ui.html}, the OpenAPI document at {@code /v3/api-docs}. */
@Configuration
@Profile("!tool")
public class OpenApiConfiguration {
    @Bean
    public OpenAPI operatorApi() {
        return new OpenAPI().info(new Info()
            .title("redis-dr operator API")
            .version("v1")
            .description("Progress of the sync from Site A to Site B, pause / resume and forced full sync.\n\n"
                + "**There is no authentication.** The server listens on 127.0.0.1 only (`server.address`); "
                + "reach it with `kubectl port-forward` / `kubectl exec`, and think twice before binding it "
                + "to another address: `full-sync` flushes Site B."));
    }
}
