package com.realtime.refine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.realtime.refine", "com.realtime.common"})
@EntityScan(basePackages = {"com.realtime.refine.domain", "com.realtime.common.domain"})
@EnableJpaRepositories(basePackages = {"com.realtime.refine.infrastructure.persistence.jpa", "com.realtime.common.infrastructure.persistence.jpa"})
@EnableRetry
@EnableScheduling
public class RefineApplication {

    public static void main(String[] args) {
        SpringApplication.run(RefineApplication.class, args);
    }

}
