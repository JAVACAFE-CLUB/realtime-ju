package com.realtime.refine.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class ExecutorConfig {

    @Bean(name = "refineTaskExecutor")
    public Executor refineTaskExecutor(
            @Value("${task-executor.core-pool-size:12}") int coreSize,
            @Value("${task-executor.max-pool-size:24}") int maxSize,
            @Value("${task-executor.queue-capacity:10000}") int queueCapacity,
            @Value("${task-executor.thread-name-prefix:refine-worker-}") String prefix
    ) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(prefix);
        executor.setWaitForTasksToCompleteOnShutdown(true);

        // Rejected 정책: CallerRunsPolicy로 변경 (큐 초과 시 호출 스레드에서 실행)
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());

        executor.initialize();
        return executor;
    }
}



