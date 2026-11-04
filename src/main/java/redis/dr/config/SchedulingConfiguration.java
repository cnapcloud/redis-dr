package redis.dr.config;

import org.springframework.boot.task.ThreadPoolTaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import redis.dr.scheduler.SchedulerErrorHandler;

/**
 * Enables {@code @Scheduled} jobs on Spring Boot's {@code taskScheduler} (pool size: {@code spring.task.scheduling.pool.size})
 * and routes their errors to {@link SchedulerErrorHandler}.
 */
@Configuration
@EnableScheduling
public class SchedulingConfiguration {
    @Bean
    public ThreadPoolTaskSchedulerCustomizer taskSchedulerCustomizer(SchedulerErrorHandler errorHandler) {
        return scheduler -> scheduler.setErrorHandler(errorHandler);
    }
}
