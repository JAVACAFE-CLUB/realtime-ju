package com.realtime.collector.application.sns.youtube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.sns.youtube.dto.VideoRecord;
import com.realtime.collector.application.sns.youtube.dto.YouTubeVideoListResponse;
import com.realtime.collector.application.sns.youtube.dto.YouTubeVideoListResponse.YouTubeVideo;
import com.realtime.collector.application.sns.youtube.util.YouTubeErrorMapper;
import com.realtime.collector.application.util.CollectorEventAsyncInvoker;
import com.realtime.collector.application.util.RetryUtils;
import com.realtime.common.domain.content.ContentMetadata;
import com.realtime.common.domain.content.ContentMetadataRepository;
import com.realtime.collector.exception.YouTubeApiException;
import com.realtime.collector.exception.YouTubeDataCollectionException;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.DateTimeFormats;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.BusinessException;
import com.realtime.common.exception.ErrorCode;
import com.realtime.common.exception.MinioStorageException;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

@Service
@Slf4j
public class YouTubeCollector {

    private static final String YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos";

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventAsyncInvoker eventAsyncInvoker;
    private final TaskExecutor videoExecutor;

    @Value("${collector.youtube.api.key:}")
    private String youtubeApiKey;

    @Value("${collector.youtube.retry.max-attempts:2}")
    private int maxRetryAttempts;

    @Value("${collector.youtube.retry.base-backoff-ms:200}")
    private long baseBackoffMs;

    public YouTubeCollector(
            WebClient webClient,
            ObjectMapper objectMapper,
            MinioClient minioClient,
            ContentMetadataRepository contentMetadataRepository,
            CollectorEventAsyncInvoker eventAsyncInvoker,
            @Qualifier("youtubeVideoExecutor") TaskExecutor youtubeVideoExecutor
    ) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.minioClient = minioClient;
        this.contentMetadataRepository = contentMetadataRepository;
        this.eventAsyncInvoker = eventAsyncInvoker;
        this.videoExecutor = youtubeVideoExecutor;
    }


    // ============================================================
    // 메인 진입점
    // ============================================================

    @Async("youtubeTaskExecutor")
    public CompletableFuture<Void> collectAndProcessYouTubeData() {
        String collectionId = CollectionIdGenerator.generateId("YT");
        log.info("🚀 YouTube 수집 시작 - collectionId={}", collectionId);

        try {
            // Step 1: API 호출하여 비디오 목록 가져오기
            YouTubeVideoListResponse response = fetchMostPopularVideos();
            if (response.items() == null || response.items().isEmpty()) {
                log.warn("⚠️ 수집된 비디오 없음 - collectionId={}", collectionId);
                return CompletableFuture.completedFuture(null);
            }

            // Step 2: 병렬 처리 시작
            startParallelProcessing(response.items(), collectionId);

            log.info("✅ YouTube 수집 요청 완료 - collectionId={}, videos={}",
                    collectionId, response.items().size());
            return CompletableFuture.completedFuture(null);

        } catch (Exception e) {
            log.error("❌ YouTube 수집 실패 - collectionId={}", collectionId, e);
            publishErrorEvent(collectionId, "", e, "BATCH_PROCESSING_ERROR");
            return CompletableFuture.failedFuture(new YouTubeDataCollectionException("YouTube 데이터 수집 실패", e));
        }
    }

    // ============================================================
    // Step 1: API 호출
    // ============================================================

    // TODO: 백오프 어노테이션이 있다. (스프링 리트라이)
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

    // ============================================================
    // Step 2: 병렬 처리
    // ============================================================

    private void startParallelProcessing(List<YouTubeVideo> videos, String collectionId) {
        log.info("🔄 병렬 처리 시작 - collectionId={}, videos={}", collectionId, videos.size());

        for (YouTubeVideo video : videos) {
            String videoId = video.id();
            processVideoAsync(video, videoId, collectionId);
        }
    }

    private void processVideoAsync(YouTubeVideo video, String videoId, String collectionId) {
        CompletableFuture
                .supplyAsync(() -> processVideo(video, collectionId), videoExecutor)
                .thenAccept(videoRecord -> handleVideoResult(videoRecord, videoId))
                .exceptionally(ex -> {
                    publishErrorEvent(videoId, "", new Exception(ex.getMessage()), "VIDEO_PROCESSING_FAILED");
                    return null;
                });
    }

    private VideoRecord processVideo(YouTubeVideo video, String collectionId) {
        try {
            // MinIO에 개별 비디오 JSON 저장
            String objectKey = saveVideoToMinio(video, collectionId);

            // 성공 레코드 반환
            return VideoRecord.success(video, objectKey);

        } catch (Exception e) {
            log.warn("❌ 비디오 처리 실패 - videoId={}", video.id(), e);
            return VideoRecord.failed(video, 500, e.getMessage());
        }
    }

    // ============================================================
    // Step 3: MinIO 저장
    // ============================================================

    private String saveVideoToMinio(YouTubeVideo video, String collectionId) {
        try {
            String videoJson = objectMapper.writeValueAsString(video);
            byte[] bytes = videoJson.getBytes(StandardCharsets.UTF_8);
            String objectKey = buildVideoObjectKey(collectionId, video.id());

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_SNS_YOUTUBE)
                    .object(objectKey)
                    .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                    .contentType("application/json; charset=utf-8")
                    .userMetadata(buildVideoMetadata(collectionId, video.id()))
                    .build());

            return buildMinioUri(objectKey);

        } catch (Exception e) {
            throw new MinioStorageException("MinIO 비디오 저장 실패", e);
        }
    }

    // ============================================================
    // Step 4: 결과 처리
    // ============================================================

    private void handleVideoResult(VideoRecord videoRecord, String videoId) {
        if (videoRecord.status() == 200) {
            // 1. DB에 메타데이터 저장
            saveVideoMetadata(videoRecord);

            // 2. Kafka 성공 이벤트 발행
            publishSuccessEvent(videoId, videoRecord.jsonObjectKey());

            log.debug("✅ 비디오 처리 성공 - videoId={}", videoId);
        } else {
            // 비디오 단위 실패는 videoId를 Kafka 키로 사용
            publishErrorEvent(videoId, "", new Exception(videoRecord.errorMessage()), "VIDEO_PROCESSING_FAILED");

            log.warn("⚠️ 비디오 처리 실패 - videoId={}, status={}, error={}",
                    videoId, videoRecord.status(), videoRecord.errorMessage());
        }
    }

    private void saveVideoMetadata(VideoRecord record) {
        YouTubeVideo video = record.video();

        ContentMetadata metadata = ContentMetadata.builder()
                .source(ContentSource.SNS_YOUTUBE.getCode())
                .externalId(video.id())
                .title(video.snippet().title())
                .rawUri(record.jsonObjectKey())
                .collectionId(extractCollectionIdFromUri(record.jsonObjectKey()))
                .collectedAt(LocalDateTime.now())
                .build();

        contentMetadataRepository.save(metadata);
    }

    // ============================================================
    // Kafka 이벤트 발행
    // ============================================================

    private void publishSuccessEvent(String videoId, String objectKey) {
        eventAsyncInvoker.publishSuccess(
                ContentSource.SNS_YOUTUBE.name(),
                KafkaTopics.RAW_SNS_YOUTUBE,
                videoId,  // Kafka 키
                objectKey, // MinIO URI
                1
        );
    }

    private void publishErrorEvent(String kafkaKey, String rawDataUrl, Exception e, String errorCode) {
        eventAsyncInvoker.publishError(
                ContentSource.SNS_YOUTUBE.name(),
                KafkaTopics.RAW_SNS_YOUTUBE_DLQ,
                kafkaKey,
                rawDataUrl,
                errorCode,
                e.getMessage(),
                RetryUtils.isRetriable(e)
        );
    }

    // ============================================================
    // 유틸리티 메서드
    // ============================================================

    private String buildVideoObjectKey(String collectionId, String videoId) {
        return String.format("%s/%s.json", buildBasePath(collectionId), videoId);
    }

    private String buildBasePath(String collectionId) {
        String datePath = LocalDateTime.now().format(DateTimeFormats.STORAGE_PATH_DATE);
        return String.format("%s/%s", datePath, collectionId);
    }

    private String buildMinioUri(String objectKey) {
        return String.format("minio://%s/%s", MinIOBuckets.RAW_SNS_YOUTUBE, objectKey);
    }

    private Map<String, String> buildVideoMetadata(String collectionId, String videoId) {
        return Map.of(
                "collection-id", collectionId,
                "video-id", videoId,
                "collected-at", Instant.now().toString()
        );
    }

    private String extractCollectionIdFromUri(String minioUri) {
        // minio://raw-sns-youtube/2025/11/27/YT_xxx/videoId.json -> YT_xxx
        String[] parts = minioUri.split("/");
        return parts.length >= 6 ? parts[5] : "";
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
