package com.realtime.collector.application.news.yna;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.news.yna.dto.ArticleRecord;
import com.realtime.collector.application.news.yna.dto.Manifest;
import com.realtime.collector.application.news.yna.dto.RssItem;
import com.realtime.collector.application.news.yna.util.YnaRssParser;
import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
import com.realtime.collector.infrastructure.config.YnaConfig;
import com.realtime.collector.infrastructure.messaging.CollectorEventPublisher;
import com.realtime.common.constants.ContentSource;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.constants.MinIOBuckets;
import com.realtime.common.util.CollectionIdGenerator;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
@Slf4j
@RequiredArgsConstructor
public class YnaCollector {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient;
    private final ContentMetadataRepository contentMetadataRepository;
    private final CollectorEventPublisher collectorEventPublisher;
    private final YnaConfig config;
    @Qualifier("ynaArticleExecutor")
    private final TaskExecutor ynaArticleExecutor;

    @Async("ynaTaskExecutor")
    public CompletableFuture<Void> collectAndProcessYnaData() {
        String collectionId = CollectionIdGenerator.generateId("YNA");
        try {
            // 1) RSS 수집 (다중 피드 지원)
            List<RssItem> rssItems = new ArrayList<>();
            for (String feedUrl : config.getFeeds()) {
                String rssXml = fetchRssXml(feedUrl);
                storeRssToMinio(collectionId, feedUrl, rssXml);
                rssItems.addAll(YnaRssParser.parse(rssXml));
            }

            // 중복 제거(articleId)
            List<RssItem> deduped = rssItems.stream()
                    .collect(Collectors.toMap(RssItem::articleId, it -> it, (a, b) -> a))
                    .values().stream().toList();

            // 2) 기사 크롤링 (동시성 + 요청 지연)
            List<ArticleRecord> articles = crawlArticlesWithConcurrency(deduped, collectionId);

            // 3) 매니페스트 저장(JSON) + HTML 저장은 crawl 단계에서 수행
            String manifestUrl = storeManifest(collectionId, config.getFeeds(), articles);

            // 4) 메타데이터 저장(DB)
            saveMetadata(articles, collectionId, manifestUrl);

            // 5) 성공 이벤트 발행
            int successCount = (int) articles.stream().filter(a -> a.crawlStatus() == 200).count();
            publishSuccess(collectionId, manifestUrl, successCount);

            log.info("YNA 수집 완료 - collectionId={}, success={} / total={}", collectionId, successCount, articles.size());
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.error("YNA 수집 실패 - collectionId={}", collectionId, e);
            publishError(collectionId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private String fetchRssXml(String feedUrl) {
        return webClient.get()
                .uri(feedUrl)
                .header(HttpHeaders.USER_AGENT, config.getUserAgent())
                .header(HttpHeaders.ACCEPT_LANGUAGE, config.getAcceptLanguage())
                .retrieve()
                .bodyToMono(String.class)
                .block();
    }

    private void storeRssToMinio(String collectionId, String feedUrl, String rssXml) throws Exception {
        byte[] bytes = rssXml.getBytes(StandardCharsets.UTF_8);
        String key = String.format("%s/rss.xml", createBasePrefix(collectionId));
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(MinIOBuckets.RAW_NEWS_YNA)
                .object(key)
                .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                .contentType("application/xml; charset=utf-8")
                .userMetadata(Map.of(
                        "collection-id", collectionId,
                        "feed-url", feedUrl,
                        "collection-time", Instant.now().toString()
                ))
                .build());
    }

    private List<ArticleRecord> crawlArticlesWithConcurrency(List<RssItem> items, String collectionId) {
        Semaphore semaphore = new Semaphore(Math.max(1, config.getConcurrency()));
        List<CompletableFuture<ArticleRecord>> futures = new ArrayList<>();
        for (RssItem item : items) {
            futures.add(CompletableFuture.supplyAsync(() -> fetchArticleWithThrottle(item, semaphore, collectionId),
                    ynaArticleExecutor));
        }
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private ArticleRecord fetchArticleWithThrottle(RssItem item, Semaphore semaphore, String collectionId) {
        try {
            semaphore.acquire();
            sleepInterRequest();
            return fetchArticle(item, collectionId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ArticleRecord.failed(item, 499, "INTERRUPTED");
        } finally {
            semaphore.release();
        }
    }

    private void sleepInterRequest() {
        int base = config.getInterRequestDelayMs();
        int jitter = ThreadLocalRandom.current().nextInt(config.getInterRequestJitterMs() + 1);
        try {
            Thread.sleep(base + jitter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ArticleRecord fetchArticle(RssItem item, String collectionId) {
        for (int attempt = 1; attempt <= config.getMaxRetries(); attempt++) {
            try {
                String body = webClient.get()
                        .uri(item.link())
                        .header(HttpHeaders.USER_AGENT, config.getUserAgent())
                        .header(HttpHeaders.ACCEPT_LANGUAGE, config.getAcceptLanguage())
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();

                String htmlKey = storeHtml(collectionId, item.articleId(), body);
                return ArticleRecord.success(item, 200, "text/html", "UTF-8", htmlKey);
            } catch (Exception e) {
                boolean retriable = isRetriable(e);
                if (!retriable || attempt == config.getMaxRetries()) {
                    log.warn("기사 크롤 실패 - {} attempt={} retriable={}", item.articleId(), attempt, retriable);
                    return ArticleRecord.failed(item, 500, e.getMessage());
                }
                backoff(attempt);
            }
        }
        return ArticleRecord.failed(item, 500, "UNKNOWN");
    }

    private boolean isRetriable(Exception e) {
        if (e instanceof WebClientResponseException ex) {
            int status = ex.getStatusCode().value();
            return status == 429 || status >= 500;
        }
        return true; // 네트워크/일시 오류
    }

    private void backoff(int attempt) {
        long backoff = (long) (config.getBaseBackoffMs() * Math.pow(2, attempt - 1));
        long jitter = ThreadLocalRandom.current().nextLong(50, 150);
        try {
            Thread.sleep(backoff + jitter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String storeHtml(String collectionId, String articleId, String html) throws Exception {
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        String key = String.format("%s/%s.html", createBasePrefix(collectionId), articleId);
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(MinIOBuckets.RAW_NEWS_YNA)
                .object(key)
                .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                .contentType("text/html; charset=utf-8")
                .userMetadata(Map.of("article-id", articleId))
                .build());
        return String.format("minio://%s/%s", MinIOBuckets.RAW_NEWS_YNA, key);
    }

    private String storeManifest(String collectionId, List<String> feeds, List<ArticleRecord> records)
            throws Exception {
        Manifest manifest = Manifest.of(collectionId, feeds, records);
        byte[] bytes = objectMapper.writeValueAsBytes(manifest);
        String key = String.format("%s/articles-manifest.json", createBasePrefix(collectionId));
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(MinIOBuckets.RAW_NEWS_YNA)
                .object(key)
                .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                .contentType("application/json; charset=utf-8")
                .userMetadata(Map.of(
                        "collection-id", collectionId,
                        "item-count", String.valueOf(records.size()),
                        "collection-time", Instant.now().toString()
                ))
                .build());
        return String.format("minio://%s/%s", MinIOBuckets.RAW_NEWS_YNA, key);
    }

    private void saveMetadata(List<ArticleRecord> records, String collectionId, String manifestUrl) {
        LocalDateTime now = LocalDateTime.now();
        List<ContentMetadata> list = records.stream()
                .filter(r -> r.crawlStatus() == 200)
                .map(r -> ContentMetadata.builder()
                        .source(ContentSource.NEWS_YNA.code())
                        .externalId(r.item().articleId())
                        .title(r.item().title())
                        .rawUri(manifestUrl)
                        .collectionId(collectionId)
                        .collectedAt(now)
                        .build())
                .toList();
        contentMetadataRepository.saveAll(list);
    }

    @Async("kafkaTaskExecutor")
    protected void publishSuccess(String collectionId, String manifestUrl, int count) {
        collectorEventPublisher.publishCollected(ContentSource.NEWS_YNA.name(), KafkaTopics.RAW_NEWS_YNA,
                collectionId, manifestUrl, count);
    }

    @Async("kafkaTaskExecutor")
    protected void publishError(String collectionId, Exception e) {
        boolean retriable = isRetriable(e);
        collectorEventPublisher.publishCollectError(ContentSource.NEWS_YNA.name(), KafkaTopics.RAW_NEWS_YNA_DLQ,
                collectionId, "", "INTERNAL_SERVER_ERROR", e.getMessage(), retriable);
    }

    private String createBasePrefix(String collectionId) {
        return String.format("%s/%s",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")),
                collectionId);
    }
}
