package com.realtime.index.infrastructure.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class ExecutorConfig {

    @Value("${task-executor.core-pool-size:10}")
    private int corePoolSize;

    @Value("${task-executor.max-pool-size:20}")
    private int maxPoolSize;

    @Value("${task-executor.queue-capacity:500}")
    private int queueCapacity;

    @Value("${task-executor.thread-name-prefix:index-executor-}")
    private String threadNamePrefix;

    @Bean(name = "indexTaskExecutor")
    public Executor indexTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }
}
