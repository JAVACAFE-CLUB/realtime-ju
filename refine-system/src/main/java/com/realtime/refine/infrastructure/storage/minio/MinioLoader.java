package com.realtime.refine.infrastructure.storage.minio;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import java.io.InputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MinioLoader {

    private final MinioClient minioClient;

    // [재시도 정책] 네트워크 오류에 대해 최대 3회, 지연 증가 백오프
    @Retryable(
            retryFor = { Exception.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0, maxDelay = 400)
    )
    public InputStream loadStream(String bucket, String objectKey) {
        try {
            log.debug("📦 MinIO 객체 로드 시도 - bucket={}, key={}", bucket, objectKey);
            InputStream stream = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectKey)
                    .build());
            log.debug("✅ MinIO 객체 로드 성공 - bucket={}, key={}", bucket, objectKey);
            return stream;
        } catch (Exception e) {
            // [에러 처리] 상세 경로 포함하여 예외 래핑
            log.error("❌ MinIO 객체 로드 실패 - bucket={}, key={}, error={}", bucket, objectKey, e.getMessage(), e);
            throw new RuntimeException("MinIO load failed: " + bucket + "/" + objectKey, e);
        }
    }

    public long sizeOf(String bucket, String objectKey) {
        try {
            StatObjectResponse stat = minioClient.statObject(StatObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectKey)
                    .build());
            long size = stat.size();
            log.debug("📏 MinIO 객체 크기 조회 - bucket={}, key={}, size={}bytes", bucket, objectKey, size);
            return size;
        } catch (Exception e) {
            // [경고] 사이즈 조회 실패는 비치명적 → 0 반환하여 파이프라인 유지
            log.warn("⚠️ MinIO 객체 크기 조회 실패 - bucket={}, key={}, error={}, 0 반환", bucket, objectKey, e.getMessage());
            return 0L;
        }
    }
}



