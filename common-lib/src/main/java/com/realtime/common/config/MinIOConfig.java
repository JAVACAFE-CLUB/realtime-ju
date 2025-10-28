package com.realtime.common.config;

import io.minio.MinioClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MinIO 객체 저장소 설정 + 라이프사이클 정책
 */
@Configuration
@Slf4j
public class MinIOConfig {

    @Value("${minio.endpoint:http://localhost:9000}")
    private String endpoint;

    @Value("${minio.access-key:minioadmin}")
    private String accessKey;

    @Value("${minio.secret-key:minioadmin123}")
    private String secretKey;

    @Bean
    public MinioClient minioClient() {
        log.info("🔥 Initializing MinIO Client - Endpoint: {}", endpoint);

        return MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }
}