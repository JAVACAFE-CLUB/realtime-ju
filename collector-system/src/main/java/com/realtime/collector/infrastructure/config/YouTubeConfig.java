package com.realtime.collector.infrastructure.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class YouTubeConfig {

    @Value("${youtube.api.key:}")
    private String key;
}
