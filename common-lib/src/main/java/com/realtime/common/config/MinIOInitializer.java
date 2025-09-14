package com.realtime.common.config;

import com.realtime.common.constants.MinIOBuckets;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.boot.context.event.ApplicationReadyEvent;

/**
 * MinIO 버킷/라이프사이클 비차단 초기화기.
 * - 앱 부팅 완료 후 백그라운드에서 재시도로 안전하게 초기화
 * - 운영/개발 공통 사용. 실패해도 애플리케이션 부팅을 막지 않음
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(value = "minio.init.enabled", havingValue = "true", matchIfMissing = true)
public class MinIOInitializer {

    private final MinioClient minioClient;

    @Value("${minio.init.max-attempts:20}")
    private int maxAttempts;

    @Value("${minio.init.initial-delay-ms:2000}")
    private long initialDelayMs;

    @Value("${minio.init.backoff-ms:2000}")
    private long backoffMs;

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("🪣 MinIO initialization scheduled (non-blocking)");
        CompletableFuture.runAsync(this::initializeWithRetry);
    }

    private void initializeWithRetry() {
        sleep(initialDelayMs);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                initializeBuckets();
                log.info("🎉 MinIO initialization completed (attempt {})", attempt);
                return;
            } catch (Exception e) {
                long delay = backoffMs * attempt;
                log.warn("MinIO initialization failed (attempt {}/{}). Retrying in {} ms. Cause: {}",
                        attempt, maxAttempts, delay, e.getMessage());
                sleep(delay);
            }
        }

        log.error("❌ MinIO initialization failed after {} attempts. Please check MinIO service.", maxAttempts);
    }

    private void initializeBuckets() throws Exception {
        String[] buckets = MinIOBuckets.getAllBuckets();
        for (String bucketName : buckets) {
            ensureBucket(bucketName);
        }
    }

    private void ensureBucket(String bucketName) throws Exception {
        boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(bucketName).build()
        );
        if (!exists) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            log.info("✅ Created MinIO bucket: {}", bucketName);
        } else {
            log.debug("✅ MinIO bucket already exists: {}", bucketName);
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}


