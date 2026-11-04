package redis.dr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * DR sync from Redis site A (active) to site B (standby), for Redis Sentinel or Redis Cluster
 * ({@code redis-dr.mode} in application.yaml). See {@link redis.dr.service.DrEngine}.
 */
@SpringBootApplication
public class RedisDrApplication {
    public static void main(String[] args) {
        SpringApplication.run(RedisDrApplication.class, args);
    }
}
