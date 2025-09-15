package com.realtime.collector.application.sns.youtube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.sns.youtube.dto.YouTubeVideoListResponse;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.collector.exception.YouTubeApiException;
import com.realtime.collector.exception.YouTubeDataCollectionException;
import com.realtime.collector.infrastructure.config.YouTubeConfig;
import com.realtime.collector.infrastructure.messaging.CollectorEventPublisher;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.ErrorCode;
import com.realtime.common.exception.BusinessException;
import com.realtime.common.exception.DatabaseStorageException;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

@Service
@Slf4j
@RequiredArgsConstructor
public class YouTubeCollector {

    private static final String YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos";
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long BASE_BACKOFF_MS = 200L;
    private static final int BATCH_SIZE = 1000;
    private static final Pattern HTTP_STATUS_PATTERN = Pattern.compile("HTTP\\s+(\\d{3})\\s+");

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventPublisher collectorEventPublisher;
    private final YouTubeConfig youTubeConfig;

    @Async("youtubeTaskExecutor")
    @Transactional
    public CompletableFuture<Void> collectAndProcessYouTubeData() {
        String collectionId = CollectionIdGenerator.generateId("YT");

        try {
            // 1. API 호출
            YouTubeVideoListResponse response = fetchMostPopularVideos(collectionId);

            // 2. 데이터 저장 (MinIO + MySQL)
            String minioUrl = storeToMinio(response, collectionId);
            List<ContentMetadata> metadataList = createMetadataList(response, collectionId, minioUrl);
            saveMetadataToDatabase(metadataList);

            // 3. 성공 이벤트 발행
            publishSuccessEvent(collectionId, minioUrl, response.items().size());

            log.info("YouTube 데이터 수집 완료 - CollectionId: {}, Videos: {}",
                    collectionId, response.items().size());

            return CompletableFuture.completedFuture(null);

        } catch (Exception e) {
            log.error("YouTube 데이터 수집 실패 - CollectionId: {}", collectionId, e);
            publishErrorEvent(collectionId, "", e);
            throw new YouTubeDataCollectionException("데이터 수집 실패", e);
        }
    }

    private YouTubeVideoListResponse fetchMostPopularVideos(String collectionId) {
        URI uri = buildApiUri();

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                String responseBody = webClient.get()
                        .uri(uri)
                        .retrieve()
                        .onStatus(status -> !status.is2xxSuccessful(),
                                clientResponse -> clientResponse.bodyToMono(String.class)
                                        .defaultIfEmpty("")
                                        .map(body -> new YouTubeApiException(
                                                String.format("YouTube API 호출 실패 - HTTP %d: %s",
                                                        clientResponse.statusCode().value(), body))))
                        .bodyToMono(String.class)
                        .block();

                return objectMapper.readValue(responseBody, YouTubeVideoListResponse.class);

            } catch (Exception e) {
                if (!isRetriableError(e) || attempt == MAX_RETRY_ATTEMPTS) {
                    throw new YouTubeApiException("YouTube API 호출 실패", e);
                }
                sleepWithBackoff(attempt);
            }
        }

        throw new YouTubeApiException("YouTube API 호출 실패 - 최대 재시도 횟수 초과");
    }

    private URI buildApiUri() {
        return UriComponentsBuilder.fromHttpUrl(YOUTUBE_API_URL)
                .queryParam("part", "snippet,statistics,contentDetails")
                .queryParam("chart", "mostPopular")
                .queryParam("maxResults", 50)
                .queryParam("regionCode", "KR")
                .queryParam("hl", "ko")
                .queryParam("key", youTubeConfig.getKey())
                .build()
                .toUri();
    }

    private boolean isRetriableError(Exception exception) {
        String message = exception.getMessage();
        if (message == null) {
            return true;
        }

        Matcher matcher = HTTP_STATUS_PATTERN.matcher(message);
        if (matcher.find()) {
            int statusCode = Integer.parseInt(matcher.group(1));
            return statusCode == 429 || statusCode >= 500;
        }
        return true; // 네트워크 오류 등은 재시도
    }

    private void sleepWithBackoff(int attempt) {
        try {
            long backoff = (long) (BASE_BACKOFF_MS * Math.pow(2, attempt - 1));
            long jitter = ThreadLocalRandom.current().nextLong(50, 150);
            Thread.sleep(backoff + jitter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new YouTubeApiException("API 호출 중 인터럽트 발생", e);
        }
    }

    private String storeToMinio(YouTubeVideoListResponse response, String collectionId) {
        try {
            String jsonData = objectMapper.writeValueAsString(response);
            byte[] bytes = jsonData.getBytes(StandardCharsets.UTF_8);
            String fileName = createMinioFileName(collectionId);

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_SNS_YOUTUBE)
                    .object(fileName)
                    .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                    .contentType("application/json; charset=utf-8")
                    .userMetadata(createMinioMetadata(collectionId, response.items().size()))
                    .build());

            String minioUrl = String.format("minio://%s/%s", MinIOBuckets.RAW_SNS_YOUTUBE, fileName);
            log.debug("MinIO 저장 완료: {}", minioUrl);
            return minioUrl;

        } catch (Exception e) {
            throw new MinioStorageException("MinIO 저장 실패", e);
        }
    }

    private String createMinioFileName(String collectionId) {
        return String.format("%s/%s.json",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")),
                collectionId);
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
                        .source(ContentSource.SNS_YOUTUBE.code())
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
            // 배치 크기로 분할 저장
            for (int i = 0; i < metadataList.size(); i += BATCH_SIZE) {
                int endIndex = Math.min(i + BATCH_SIZE, metadataList.size());
                List<ContentMetadata> batch = metadataList.subList(i, endIndex);
                contentMetadataRepository.saveAll(batch);
                log.debug("메타데이터 배치 저장 완료: {} records", batch.size());
            }

            log.info("전체 메타데이터 저장 완료: {} records", metadataList.size());

        } catch (Exception e) {
            throw new DatabaseStorageException("데이터베이스 저장 실패", e);
        }
    }

    @Async("kafkaTaskExecutor")
    protected void publishSuccessEvent(String collectionId, String minioUrl, int videoCount) {
        collectorEventPublisher.publishCollected(
                        ContentSource.SNS_YOUTUBE.name(),
                        KafkaTopics.RAW_SNS_YOUTUBE,
                        collectionId,
                        minioUrl,
                        videoCount)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("성공 이벤트 발행 완료 - CollectionId: {}", collectionId);
                    } else {
                        log.error("성공 이벤트 발행 실패 - CollectionId: {}", collectionId, ex);
                    }
                });
    }

    @Async("kafkaTaskExecutor")
    protected void publishErrorEvent(String collectionId, String rawDataUrl, Exception exception) {
        String errorCode = determineErrorCode(exception);
        boolean retriable = isRetriableError(exception);
        String topic = retriable ? KafkaTopics.RAW_SNS_YOUTUBE_RETRY : KafkaTopics.RAW_SNS_YOUTUBE_DLQ;

        collectorEventPublisher.publishCollectError(
                        ContentSource.SNS_YOUTUBE.name(),
                        topic,
                        collectionId,
                        rawDataUrl,
                        errorCode,
                        exception.getMessage(),
                        retriable)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.warn("오류 이벤트 발행 완료 - CollectionId: {}", collectionId);
                    } else {
                        log.error("오류 이벤트 발행 실패 - CollectionId: {}", collectionId, ex);
                    }
                });
    }

    private String determineErrorCode(Exception exception) {
        if (exception instanceof BusinessException) {
            return ((BusinessException) exception).getErrorCode().name();
        }
        if (exception instanceof YouTubeApiException) {
            return ErrorCode.EXTERNAL_API_ERROR.name();
        }
        return ErrorCode.INTERNAL_SERVER_ERROR.name();
    }
}