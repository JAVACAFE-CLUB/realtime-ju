package com.realtime.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"com.realtime.collector", "com.realtime.common"})
@EntityScan(basePackages = {"com.realtime.collector.domain", "com.realtime.common.domain"})
@EnableJpaRepositories(basePackages = {"com.realtime.collector.infrastructure.persistence.jpa", "com.realtime.common.infrastructure.persistence.jpa"})
public class CollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(CollectorApplication.class, args);
    }

}
