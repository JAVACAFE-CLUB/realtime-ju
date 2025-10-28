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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * MinIO 버킷 초기화
 * <p>- 애플리케이션 기동 완료 후 별도 스레드에서 버킷 존재를 보장합니다.
 * <p>- 초기화 실패가 발생해도 애플리케이션 부팅을 막지 않으며, 최대 {@code max-attempts} 회까지 재시도합니다.
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
