package com.realtime.collector.application.sns.youtube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.sns.youtube.dto.YouTubeVideoListResponse;
import com.realtime.collector.application.sns.youtube.util.YouTubeErrorMapper;
import com.realtime.collector.application.util.CollectorEventAsyncInvoker;
import com.realtime.collector.application.util.RetryUtils;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.collector.exception.YouTubeApiException;
import com.realtime.collector.exception.YouTubeDataCollectionException;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.BusinessException;
import com.realtime.common.exception.DatabaseStorageException;
import com.realtime.common.exception.ErrorCode;
import com.realtime.common.exception.MinioStorageException;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

@Service
@Slf4j
@RequiredArgsConstructor
public class YouTubeCollector {

    private static final String YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos";
    private static final DateTimeFormatter MINIO_PATH_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventAsyncInvoker eventAsyncInvoker;

    @Value("${collector.youtube.api.key:}")
    private String youtubeApiKey;

    @Value("${collector.youtube.retry.max-attempts:2}")
    private int maxRetryAttempts;

    @Value("${collector.youtube.retry.base-backoff-ms:200}")
    private long baseBackoffMs;

    @Value("${collector.youtube.batch-size:1000}")
    private int batchSize;


    @Async("youtubeTaskExecutor")
    @Transactional
    public CompletableFuture<Void> collectAndProcessYouTubeData() {
        String collectionId = CollectionIdGenerator.generateId("YT");

        try {
            // 1) API 호출
            YouTubeVideoListResponse response = fetchMostPopularVideos();

            // 2) 데이터 저장 (MinIO + MySQL)
            String minioUrl = storeToMinio(response, collectionId);
            List<ContentMetadata> metadataList = createMetadataList(response, collectionId, minioUrl);
            saveMetadataToDatabase(metadataList);

            // 3) 성공 이벤트 발행
            eventAsyncInvoker.publishSuccess(
                    ContentSource.SNS_YOUTUBE.name(),
                    KafkaTopics.RAW_SNS_YOUTUBE,
                    collectionId,
                    minioUrl,
                    response.items().size());

            log.info("✅ YouTube 데이터 수집 완료 - CollectionId: {}, Videos: {}",
                    collectionId, response.items().size());

            return CompletableFuture.completedFuture(null);

        } catch (Exception e) {
            log.error("❌ YouTube 데이터 수집 실패 - CollectionId: {}", collectionId, e);
            eventAsyncInvoker.publishError(
                    ContentSource.SNS_YOUTUBE.name(),
                    KafkaTopics.RAW_SNS_YOUTUBE_DLQ,
                    collectionId,
                    "",
                    determineErrorCode(e),
                    e.getMessage(),
                    RetryUtils.isRetriable(e));
            throw new YouTubeDataCollectionException("데이터 수집 실패", e);
        }
    }

    private YouTubeVideoListResponse fetchMostPopularVideos() {
        Exception lastException = null;
        int maxAttempts = maxRetryAttempts;
        long configuredBaseBackoffMs = this.baseBackoffMs;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                String responseBody = getYouTubeApiResponse();
                return objectMapper.readValue(responseBody, YouTubeVideoListResponse.class);

            } catch (Exception e) {
                if (!RetryUtils.isRetriable(e)) {
                    throw new YouTubeApiException("YouTube API 호출 실패", e);
                }
                lastException = e;
                if (attempt < maxAttempts) {
                    log.debug("🔁 YouTube API 재시도 예정 - attempt={}/{} cause={}", attempt, maxAttempts, e.toString());
                    try {
                        RetryUtils.sleepWithBackoff(configuredBaseBackoffMs, attempt);
                    } catch (RuntimeException ex) {
                        throw new YouTubeApiException("API 호출 중 인터럽트 발생", ex);
                    }
                }
            }
        }

        throw new YouTubeApiException("YouTube API 호출 실패 - 최대 재시도 횟수 초과", lastException);
    }

    private String getYouTubeApiResponse() {
        URI uri = UriComponentsBuilder.fromHttpUrl(YOUTUBE_API_URL)
                .queryParam("part", "snippet,statistics,contentDetails")
                .queryParam("chart", "mostPopular")
                .queryParam("maxResults", 50)
                .queryParam("regionCode", "KR")
                .queryParam("hl", "ko")
                .queryParam("key", youtubeApiKey)
                .build()
                .toUri();

        return webClient.get()
                .uri(uri)
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), clientResponse ->
                        clientResponse.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .map(body -> YouTubeErrorMapper.toException(
                                        objectMapper,
                                        clientResponse.statusCode().value(),
                                        clientResponse.statusCode().toString(),
                                        body)))
                .bodyToMono(String.class)
                .block();
    }

    private String storeToMinio(YouTubeVideoListResponse response, String collectionId) {
        try {
            String jsonData = objectMapper.writeValueAsString(response);
            byte[] bytes = jsonData.getBytes(StandardCharsets.UTF_8);
            String fileName = String.format("%s/%s.json",
                    LocalDateTime.now().format(MINIO_PATH_FORMATTER),
                    collectionId);

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_SNS_YOUTUBE)
                    .object(fileName)
                    .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                    .contentType("application/json; charset=utf-8")
                    .userMetadata(createMinioMetadata(collectionId, response.items().size()))
                    .build());

            String minioUrl = String.format("minio://%s/%s", MinIOBuckets.RAW_SNS_YOUTUBE, fileName);
            log.debug("📦 MinIO 저장 완료: {}", minioUrl);
            return minioUrl;

        } catch (Exception e) {
            throw new MinioStorageException("MinIO 저장 실패", e);
        }
    }

    private Map<String, String> createMinioMetadata(String collectionId, int videoCount) {
        return Map.of(
                "collection-id", collectionId,
                "video-count", String.valueOf(videoCount),
                "collection-time", Instant.now().toString()
        );
    }

    private List<ContentMetadata> createMetadataList(YouTubeVideoListResponse response,
                                                     String collectionId, String rawUri) {
        LocalDateTime collectedAt = LocalDateTime.now();

        return response.items().stream()
                .map(item -> ContentMetadata.builder()
                        .source(ContentSource.SNS_YOUTUBE.getCode())
                        .externalId(item.id())
                        .title(item.snippet().title())
                        .rawUri(rawUri)
                        .collectionId(collectionId)
                        .collectedAt(collectedAt)
                        .build())
                .collect(Collectors.toList());
    }

    private void saveMetadataToDatabase(List<ContentMetadata> metadataList) {
        try {
            int configuredBatchSize = batchSize;
            for (int i = 0; i < metadataList.size(); i += configuredBatchSize) {
                int endIndex = Math.min(i + configuredBatchSize, metadataList.size());
                List<ContentMetadata> batch = metadataList.subList(i, endIndex);
                contentMetadataRepository.saveAll(batch);
                log.debug("🗂️ 메타데이터 배치 저장 완료: {} records", batch.size());
            }

            log.info("🧾 전체 메타데이터 저장 완료: {} records", metadataList.size());

        } catch (Exception e) {
            throw new DatabaseStorageException("데이터베이스 저장 실패", e);
        }
    }

    private String determineErrorCode(Exception exception) {
        if (exception instanceof BusinessException businessException) {
            return businessException.getErrorCode().name();
        }
        if (exception instanceof YouTubeApiException) {
            return ErrorCode.EXTERNAL_API_ERROR.name();
        }
        return ErrorCode.INTERNAL_SERVER_ERROR.name();
    }
}
