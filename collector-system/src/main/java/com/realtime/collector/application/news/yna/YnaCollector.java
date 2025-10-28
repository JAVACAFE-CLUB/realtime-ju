package com.realtime.collector.application.news.yna;

import com.realtime.collector.application.news.yna.config.YnaConfig;
import com.realtime.collector.application.news.yna.dto.ArticleRecord;
import com.realtime.collector.application.news.yna.dto.RssItem;
import com.realtime.collector.application.news.yna.util.YnaRssParser;
import com.realtime.collector.application.util.CollectorEventAsyncInvoker;
import com.realtime.collector.application.util.RetryUtils;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.collector.exception.RetriableException;
import com.realtime.collector.exception.YnaDataCollectionException;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.DateTimeFormats;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.exception.MinioStorageException;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
@Slf4j
public class YnaCollector {

    private final WebClient webClient;
    private final MinioClient minioClient;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventAsyncInvoker eventPublisher;
    private final YnaConfig config;
    private final TaskExecutor articleExecutor;

    public YnaCollector(
            WebClient webClient,
            MinioClient minioClient,
            ContentMetadataRepository contentMetadataRepository,
            CollectorEventAsyncInvoker eventAsyncInvoker,
            YnaConfig config,
            @Qualifier("ynaArticleExecutor") TaskExecutor ynaArticleExecutor
    ) {
        this.webClient = webClient;
        this.minioClient = minioClient;
        this.contentMetadataRepository = contentMetadataRepository;
        this.eventPublisher = eventAsyncInvoker;
        this.config = config;
        this.articleExecutor = ynaArticleExecutor;
    }

    // ============================================================
    // 메인 진입점
    // ============================================================

    @Async("ynaTaskExecutor")
    public CompletableFuture<Void> collectAndProcessYnaData() {
        String batchId = CollectionIdGenerator.generateId("YNA");
        log.info("🚀 YNA 수집 시작 - batchId={}", batchId);

        try {
            // Step 1: RSS 피드 수집
            List<RssItem> rssItems = collectAllRssFeeds(batchId);
            if (rssItems.isEmpty()) {
                log.warn("⚠️ 수집된 RSS 아이템 없음 - batchId={}", batchId);
                return CompletableFuture.completedFuture(null);
            }

            // Step 2: 중복 제거
            List<RssItem> uniqueItems = deduplicateItems(rssItems);

            // Step 3: 병렬 크롤링 시작
            startParallelCrawling(uniqueItems, batchId);

            log.info("✅ YNA 수집 요청 완료 - batchId={}", batchId);
            return CompletableFuture.completedFuture(null);

        } catch (Exception e) {
            log.error("❌ YNA 수집 실패 - batchId={}", batchId, e);
            // 배치 단위 실패는 기존대로 배치 ID를 키로 사용
            publishErrorEvent(batchId, "", e, "BATCH_PROCESSING_ERROR");
            return CompletableFuture.failedFuture(new YnaDataCollectionException("YNA 데이터 수집 실패", e));
        }
    }

    // ============================================================
    // Step 1: RSS 피드 수집
    // ============================================================

    private List<RssItem> collectAllRssFeeds(String batchId) {
        List<RssItem> allItems = new ArrayList<>();
        List<String> feedUrls = config.getFeeds();

        log.info("📡 RSS 피드 수집 시작 - batchId={}, feeds={}", batchId, feedUrls.size());

        for (String feedUrl : feedUrls) {
            try {
                List<RssItem> items = collectSingleFeed(feedUrl, batchId);
                allItems.addAll(items);
                log.info("✅ RSS 피드 수집 성공 - url={}, items={}", feedUrl, items.size());

            } catch (Exception e) {
                log.error("❌ RSS 피드 수집 실패 - url={}", feedUrl, e);
                publishRssFeedError(feedUrl, batchId, e);
                // 다음 피드 계속 처리
            }
        }

        log.info("📦 전체 RSS 수집 완료 - batchId={}, totalItems={}", batchId, allItems.size());
        return allItems;
    }

    private List<RssItem> collectSingleFeed(String feedUrl, String batchId) {
        // 1. RSS XML 다운로드
        String rssXml = downloadRssXml(feedUrl);

        // 2. MinIO에 원본 저장
        saveRssXmlToStorage(batchId, feedUrl, rssXml);

        // 3. XML 파싱
        return parseRssXml(rssXml);
    }

    private String downloadRssXml(String feedUrl) {
        log.debug("⬇️ RSS XML 다운로드 - url={}", feedUrl);

        return webClient.get()
                .uri(feedUrl)
                .header(HttpHeaders.USER_AGENT, config.getUserAgent())
                .header(HttpHeaders.ACCEPT_LANGUAGE, config.getAcceptLanguage())
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    private void saveRssXmlToStorage(String batchId, String feedUrl, String rssXml) {
        try {
            String objectKey = buildRssObjectKey(batchId);
            byte[] xmlBytes = rssXml.getBytes(StandardCharsets.UTF_8);

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_NEWS_YNA)
                    .object(objectKey)
                    .stream(new ByteArrayInputStream(xmlBytes), xmlBytes.length, -1)
                    .contentType("application/xml; charset=utf-8")
                    .userMetadata(buildRssMetadata(batchId, feedUrl))
                    .build());

            log.debug("💾 RSS XML 저장 완료 - key={}", objectKey);

        } catch (Exception e) {
            log.warn("⚠️ RSS XML 저장 실패 - batchId={}, url={}", batchId, feedUrl, e);
            // RSS 저장 실패는 치명적이지 않으므로 계속 진행
        }
    }

    private List<RssItem> parseRssXml(String rssXml) {
        return YnaRssParser.parse(rssXml);
    }

    // ============================================================
    // Step 2: 중복 제거
    // ============================================================

    private List<RssItem> deduplicateItems(List<RssItem> items) {
        Map<String, RssItem> uniqueMap = items.stream()
                .collect(Collectors.toMap(
                        RssItem::articleId,
                        item -> item,
                        (existing, duplicate) -> existing
                ));

        return new ArrayList<>(uniqueMap.values());
    }

    // ============================================================
    // Step 3: 병렬 크롤링
    // ============================================================

    private void startParallelCrawling(List<RssItem> items, String batchId) {
        Semaphore rateLimiter = new Semaphore(config.getConcurrency());

        log.info("🔄 병렬 크롤링 시작 - batchId={}, items={}, concurrency={}",
                batchId, items.size(), config.getConcurrency());

        for (RssItem item : items) {
            String contentId = item.articleId();
            crawlArticleAsync(item, contentId, batchId, rateLimiter);
        }
    }

    private void crawlArticleAsync(RssItem item, String contentId, String batchId, Semaphore rateLimiter) {
        CompletableFuture
                .supplyAsync(() -> crawlArticle(item, batchId, rateLimiter), articleExecutor)
                .thenAccept(article -> processArticleResult(article, contentId, batchId))
                .exceptionally(ex -> {
                    // 기사 단위 실패는 contentId를 Kafka 키로 사용
                    publishErrorEvent(contentId, "", new Exception(ex.getMessage()), "CRAWL_FAILED");
                    return null;
                });
    }

    private ArticleRecord crawlArticle(RssItem item, String batchId, Semaphore rateLimiter) {
        try {
            // 동시성 제어
            rateLimiter.acquire();

            // 요청 간격 대기
            waitBetweenRequests();

            // 기사 다운로드 (재시도 포함)
            return downloadArticleWithRetry(item, batchId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ArticleRecord.failed(item, 499, "INTERRUPTED");

        } finally {
            rateLimiter.release();
        }
    }

    private void waitBetweenRequests() {
        int delayMs = config.getInterRequestDelayMs();
        int jitterMs = ThreadLocalRandom.current().nextInt(config.getInterRequestJitterMs() + 1);
        int totalDelayMs = delayMs + jitterMs;

        try {
            Thread.sleep(totalDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ArticleRecord downloadArticleWithRetry(RssItem item, String batchId) {
        try {
            // @Retryable 메서드 호출
            String html = downloadArticleHtmlWithRetry(item.link(), item.articleId());

            // MinIO에 저장
            String objectKey = saveArticleHtmlToStorage(batchId, item.articleId(), html);

            // 성공 레코드 반환
            return ArticleRecord.success(item, 200, "text/html", "UTF-8", objectKey);

        } catch (Exception e) {
            log.warn("❌ 기사 크롤링 최종 실패 - articleId={}", item.articleId(), e);
            return ArticleRecord.failed(item, 500, e.getMessage());
        }
    }

    /**
     * `@Retryable`을 사용한 재시도 로직
     * <p>
     * - maxAttempts: 최대 3번 시도 - backoff: 1초부터 시작, 2배씩 증가 (최대 10초) - recover: 재시도 실패 시 복구 메서드
     */
    @Retryable(
            retryFor = {
                    WebClientRequestException.class,
                    WebClientResponseException.TooManyRequests.class,
                    WebClientResponseException.ServiceUnavailable.class,
                    WebClientResponseException.GatewayTimeout.class,
                    RetriableException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(
                    delay = 1000,      // 1초
                    multiplier = 2.0,  // 2배씩 증가
                    maxDelay = 10000   // 최대 10초
            ),
            listeners = "retryListener"
    )
    private String downloadArticleHtmlWithRetry(String articleUrl, String articleId) {
        log.debug("⬇️ 기사 다운로드 시도 - articleId={}, url={}", articleId, articleUrl);

        String html = webClient.get()
                .uri(articleUrl)
                .header(HttpHeaders.USER_AGENT, config.getUserAgent())
                .header(HttpHeaders.ACCEPT_LANGUAGE, config.getAcceptLanguage())
                .retrieve()
                .bodyToMono(String.class)
                .block();

        if (html == null || html.isBlank()) {
            throw new RetriableException("Empty article body");
        }

        return html;
    }

    private String saveArticleHtmlToStorage(String batchId, String articleId, String html) {
        try {
            String objectKey = buildArticleObjectKey(batchId, articleId);
            byte[] htmlBytes = html.getBytes(StandardCharsets.UTF_8);

            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(MinIOBuckets.RAW_NEWS_YNA)
                    .object(objectKey)
                    .stream(new ByteArrayInputStream(htmlBytes), htmlBytes.length, -1)
                    .contentType("text/html; charset=utf-8")
                    .userMetadata(Map.of("article-id", articleId))
                    .build());

            return buildMinioUri(objectKey);

        } catch (Exception e) {
            throw new MinioStorageException("MinIO HTML 저장 실패", e);
        }
    }

    // ============================================================
    // Step 4: 결과 처리
    // ============================================================

    private void processArticleResult(ArticleRecord article, String contentId, String batchId) {
        if (article.crawlStatus() == 200) {
            // 1. DB에 메타데이터 저장
            saveContentMetadata(article, contentId, batchId);

            // 2. Kafka 성공 이벤트 발행
            publishSuccessEvent(contentId, article.htmlObjectKey());

            log.debug("✅ 기사 처리 성공 - contentId={}", contentId);
        } else {
            // 기사 단위 실패는 contentId를 Kafka 키로 사용
            publishErrorEvent(contentId, "", new Exception(article.errorMessage()), "CRAWL_FAILED");

            log.warn("⚠️ 기사 크롤링 실패 - contentId={}, status={}, error={}",
                    contentId, article.crawlStatus(), article.errorMessage());
        }
    }

    private void saveContentMetadata(ArticleRecord record, String contentId, String batchId) {
        ContentMetadata metadata = ContentMetadata.builder()
                .source(ContentSource.NEWS_YNA.getCode())
                .externalId(contentId)
                .title(record.item().title())
                .rawUri(record.htmlObjectKey())
                .collectionId(batchId)
                .collectedAt(LocalDateTime.now())
                .build();

        contentMetadataRepository.save(metadata);
    }

    // ============================================================
    // Kafka 이벤트 발행
    // ============================================================

    private void publishSuccessEvent(String contentId, String objectKey) {
        eventPublisher.publishSuccess(
                ContentSource.NEWS_YNA.name(),
                KafkaTopics.RAW_NEWS_YNA,
                contentId,  // Kafka 키
                objectKey, // MinIO URI
                1
        );
    }

    private void publishRssFeedError(String feedUrl, String batchId, Exception e) {
        eventPublisher.publishError(
                ContentSource.NEWS_YNA.name(),
                KafkaTopics.RAW_NEWS_YNA_DLQ,
                batchId,
                feedUrl,
                "RSS_FEED_ERROR",
                e.getMessage(),
                RetryUtils.isRetriable(e)
        );
    }

    private void publishErrorEvent(String kafkaKey, String rawDataUrl, Exception e, String errorCode) {
        eventPublisher.publishError(
                ContentSource.NEWS_YNA.name(),
                KafkaTopics.RAW_NEWS_YNA_DLQ,
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

    private String buildRssObjectKey(String batchId) {
        return String.format("%s/rss.xml", buildBasePath(batchId));
    }

    private String buildArticleObjectKey(String batchId, String articleId) {
        return String.format("%s/%s.html", buildBasePath(batchId), articleId);
    }

    private String buildBasePath(String batchId) {
        String datePath = LocalDateTime.now().format(DateTimeFormats.STORAGE_PATH_DATE);
        return String.format("%s/%s", datePath, batchId);
    }

    private String buildMinioUri(String objectKey) {
        return String.format("minio://%s/%s", MinIOBuckets.RAW_NEWS_YNA, objectKey);
    }

    private Map<String, String> buildRssMetadata(String batchId, String feedUrl) {
        return Map.of(
                "batch-id", batchId,
                "feed-url", feedUrl,
                "collected-at", Instant.now().toString()
        );
    }
}