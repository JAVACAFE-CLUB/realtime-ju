package com.realtime.refine.application.docs.wikipedia;

import com.realtime.refine.application.docs.wikipedia.dto.WikiPage;
import com.realtime.common.constants.KafkaTopics;
import com.realtime.common.kafka.message.CollectMessage;
import com.realtime.refine.application.service.IdempotencyService;
import com.realtime.refine.domain.docs.wikipedia.RefinedDocsWikipedia;
import com.realtime.refine.domain.docs.wikipedia.RefinedDocsWikipediaRepository;
import com.realtime.refine.infrastructure.messaging.RefineEventPublisher;
import com.realtime.refine.infrastructure.parser.ndjson.NdjsonRefineService;
import com.realtime.refine.infrastructure.parser.wikitext.WikitextRefineService;
import com.realtime.refine.infrastructure.persistence.jpa.ContentMetadataService;
import com.realtime.refine.infrastructure.storage.minio.MinioLoader;
import com.realtime.refine.infrastructure.storage.minio.MinioUri;
import com.realtime.refine.support.HashUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class WikipediaRefineProcessor {

    private final IdempotencyService idempotencyService;
    private final MinioLoader minioLoader;
    private final NdjsonRefineService ndjsonRefineService;
    private final WikitextRefineService wikitextRefineService;
    private final RefinedDocsWikipediaRepository refinedDocsRepository;
    private final RefineEventPublisher publisher;
    private final ContentMetadataService contentMetadataService;

    @Qualifier("refineTaskExecutor")
    private final Executor refineTaskExecutor;

    // 배치 처리 크기 (ThreadPool 오버플로우 방지)
    private static final int BATCH_SIZE = 100;

    /**
     * 샤드 파일 전체 처리 (진입점)
     * 
     * @param msg Kafka 메시지 (샤드 파일 URL 포함)
     */
    public void processShard(CollectMessage msg) {
        // [1단계] 메시지에서 기본 정보 추출
        final String source = msg.getSource();
        final String shardCollectionId = msg.getCollectionId();
        final String rawDataUrl = msg.getRawDataUrl();

        log.info("🚀 위키피디아 샤드 정제 시작 - source={}, shardCollectionId={}", source, shardCollectionId);

        // [2단계] MinIO URI 파싱 및 처리 시간 측정 시작
        MinioUri uri = MinioUri.parse(rawDataUrl);
        log.debug("📍 MinIO URI 파싱 완료 - bucket={}, key={}", uri.bucket(), uri.objectKey());
        long shardStart = System.currentTimeMillis();

        try (InputStream in = minioLoader.loadStream(uri.bucket(), uri.objectKey())) {
            // [3단계] 원본 객체 크기 확인
            long size = minioLoader.sizeOf(uri.bucket(), uri.objectKey());
            log.debug("⬇️ MinIO 샤드 파일 로드 완료 - size={}bytes ({}KB)", size, size / 1024);

            // [4단계] NDJSON 스트리밍 파싱하여 모든 페이지 수집
            List<WikiPage> pages = new ArrayList<>();
            int totalPages = ndjsonRefineService.parseShard(in, pages::add);
            log.debug("📄 샤드 페이지 수집 완료 - totalPages={}", totalPages);

            // [5단계] 병렬 처리 및 배치 저장
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);

            processShardPagesInParallel(pages, shardCollectionId, source, rawDataUrl, successCount, failCount);

            long shardElapsed = System.currentTimeMillis() - shardStart;
            log.info("✅ 위키피디아 샤드 정제 완료 - shardCollectionId={}, totalPages={}, success={}, fail={}, totalTime={}ms",
                    shardCollectionId, totalPages, successCount.get(), failCount.get(), shardElapsed);

        } catch (Exception e) {
            // [에러 처리] 샤드 파일 로드/파싱 실패
            long elapsed = System.currentTimeMillis() - shardStart;
            log.error("❌ 위키피디아 샤드 정제 실패 - shardCollectionId={}, elapsed={}ms, errorType={}, error={}",
                    shardCollectionId, elapsed, classifyError(e), e.getMessage(), e);

            publisher.publishRefineError(
                    KafkaTopics.REFINED_DOCS_WIKIPEDIA_DLQ,
                    shardCollectionId,
                    shardCollectionId,
                    source,
                    rawDataUrl,
                    classifyError(e),
                    e.getMessage(),
                    false
            );
        }
    }

    /**
     * 샤드 내 페이지들을 배치 단위로 병렬 처리 및 저장
     * ThreadPool 오버플로우 방지를 위해 BATCH_SIZE 단위로 분할 처리
     */
    private void processShardPagesInParallel(
            List<WikiPage> pages,
            String shardCollectionId,
            String source,
            String rawDataUrl,
            AtomicInteger successCount,
            AtomicInteger failCount
    ) {
        if (pages == null || pages.isEmpty()) {
            return;
        }

        long parallelStart = System.currentTimeMillis();
        int totalPages = pages.size();
        int numBatches = (int) Math.ceil((double) totalPages / BATCH_SIZE);

        log.info("🔄 배치 병렬 처리 시작 - totalPages={}, batchSize={}, numBatches={}",
                totalPages, BATCH_SIZE, numBatches);

        List<ProcessedPage> allProcessedPages = new ArrayList<>();

        // [1단계] 페이지를 BATCH_SIZE 단위로 분할하여 순차 처리
        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            int fromIndex = batchIndex * BATCH_SIZE;
            int toIndex = Math.min(fromIndex + BATCH_SIZE, totalPages);
            List<WikiPage> batch = pages.subList(fromIndex, toIndex);

            log.debug("📦 배치 처리 시작 - batch={}/{}, size={}", batchIndex + 1, numBatches, batch.size());
            long batchStart = System.currentTimeMillis();

            // [1-1단계] 배치 내 페이지를 병렬로 처리 (Wikitext 변환, 도메인 객체 생성)
            List<CompletableFuture<ProcessedPage>> futures = batch.stream()
                    .map(page -> CompletableFuture.supplyAsync(() -> {
                        try {
                            return processPageToDocument(page, shardCollectionId, source, rawDataUrl);
                        } catch (Exception e) {
                            log.warn("⚠️ 페이지 처리 실패 - pageId={}, error={}",
                                    page.getPageId(), e.getMessage());
                            failCount.incrementAndGet();
                            return null;
                        }
                    }, refineTaskExecutor))
                    .collect(Collectors.toList());

            // [1-2단계] 배치 내 병렬 작업 완료 대기
            List<ProcessedPage> processedPages = futures.stream()
                    .map(CompletableFuture::join)
                    .filter(p -> p != null)
                    .collect(Collectors.toList());

            allProcessedPages.addAll(processedPages);

            long batchElapsed = System.currentTimeMillis() - batchStart;
            log.debug("✅ 배치 처리 완료 - batch={}/{}, processed={}, elapsed={}ms",
                    batchIndex + 1, numBatches, processedPages.size(), batchElapsed);
        }

        long parallelElapsed = System.currentTimeMillis() - parallelStart;
        log.info("✅ 전체 병렬 처리 완료 - totalProcessed={}, elapsed={}ms", allProcessedPages.size(), parallelElapsed);

        if (allProcessedPages.isEmpty()) {
            return;
        }

        // [2단계] 배치 조회: 기존 문서 일괄 조회
        long batchQueryStart = System.currentTimeMillis();
        List<String> pageIds = allProcessedPages.stream()
                .map(p -> p.pageId)
                .collect(Collectors.toList());
        Map<String, RefinedDocsWikipedia> existingDocs = refinedDocsRepository.findByPageIds(pageIds);
        long batchQueryElapsed = System.currentTimeMillis() - batchQueryStart;
        log.debug("✅ 배치 조회 완료 - pageIds={}, found={}, elapsed={}ms",
                pageIds.size(), existingDocs.size(), batchQueryElapsed);

        // [3단계] 변경 감지 및 저장 대상 분류
        List<RefinedDocsWikipedia> toSave = new ArrayList<>();
        Map<String, String> idempotencyMap = new HashMap<>();
        List<String> metadataUpdates = new ArrayList<>();

        for (ProcessedPage processed : allProcessedPages) {
            RefinedDocsWikipedia existing = existingDocs.get(processed.pageId);

            if (existing != null && existing.getChecksum() != null
                    && existing.getChecksum().equals(processed.doc.getChecksum())) {
                // 내용 동일 → 스킵
                processed.doc = processed.doc.toBuilder().id(existing.getId()).build();
                idempotencyMap.put(processed.pageId, existing.getId());
            } else if (existing != null) {
                // 내용 변경 → 기존 ID로 업데이트
                processed.doc = processed.doc.toBuilder().id(existing.getId()).build();
                toSave.add(processed.doc);
                idempotencyMap.put(processed.pageId, existing.getId());
                metadataUpdates.add(processed.pageId);
            } else {
                // 신규 문서
                toSave.add(processed.doc);
                idempotencyMap.put(processed.pageId, processed.doc.getId());
                metadataUpdates.add(processed.pageId);
            }
        }

        // [4단계] 배치 저장
        if (!toSave.isEmpty()) {
            long batchSaveStart = System.currentTimeMillis();
            boolean saved = false;
            try {
                saved = retry(() -> refinedDocsRepository.saveAll(toSave), 3, 100);
            } catch (Exception e) {
                log.error("❌ 배치 저장 재시도 실패 - count={}, error={}", toSave.size(), e.getMessage());
            }
            long batchSaveElapsed = System.currentTimeMillis() - batchSaveStart;
            log.debug("✅ 배치 저장 완료 - count={}, saved={}, elapsed={}ms", toSave.size(), saved, batchSaveElapsed);

            if (!saved) {
                log.warn("⚠️ 배치 저장 실패 - count={}", toSave.size());
            }
        }

        // [5단계] Idempotency 및 메타데이터 업데이트 (병렬)
        // 후처리도 배치 단위로 분할하여 ThreadPool 부담 감소
        int postProcessBatchSize = BATCH_SIZE;
        for (int i = 0; i < allProcessedPages.size(); i += postProcessBatchSize) {
            int toIndex = Math.min(i + postProcessBatchSize, allProcessedPages.size());
            List<ProcessedPage> postProcessBatch = allProcessedPages.subList(i, toIndex);

            List<CompletableFuture<Void>> postProcessFutures = postProcessBatch.stream()
                    .map(processed -> CompletableFuture.runAsync(() -> {
                        try {
                            String refinedId = idempotencyMap.get(processed.pageId);
                            if (refinedId != null) {
                                idempotencyService.markProcessed(source, processed.pageId, refinedId);
                                if (metadataUpdates.contains(processed.pageId)) {
                                    contentMetadataService.updateRefinedId(refinedId, source, processed.pageId);
                                }
                                // Kafka 이벤트 발행
                                publisher.publishRefined(
                                        KafkaTopics.REFINED_DOCS_WIKIPEDIA,
                                        refinedId,
                                        refinedId,
                                        processed.pageId,
                                        source
                                );
                            }
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            log.warn("⚠️ 후처리 실패 - pageId={}, error={}", processed.pageId, e.getMessage());
                        }
                    }, refineTaskExecutor))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(postProcessFutures.toArray(new CompletableFuture[0])).join();
        }

        long totalElapsed = System.currentTimeMillis() - parallelStart;
        log.info("✅ 샤드 배치 처리 완료 - totalElapsed={}ms, saved={}, skipped={}",
                totalElapsed, toSave.size(), allProcessedPages.size() - toSave.size());
    }

    /**
     * 페이지를 정제하여 도메인 객체로 변환 (저장 전 단계)
     */
    private ProcessedPage processPageToDocument(WikiPage page, String shardCollectionId, String source, String rawDataUrl) {
        final String pageId = page.getPageId();
        if (pageId == null || pageId.trim().isEmpty()) {
            throw new IllegalArgumentException("pageId is required");
        }

        // 네임스페이스 필터링: ns=0 (일반 문서)만 처리
        if (page.getNs() != null && page.getNs() != 0) {
            log.debug("⏭️ 네임스페이스 {} 스킵 - pageId={}, title={}", page.getNs(), pageId, page.getTitle());
            return null;
        }

        // Idempotency 체크
        if (idempotencyService.alreadyProcessed(source, pageId)) {
            return null; // 이미 처리됨
        }

        long pageStart = System.currentTimeMillis();

        // 리다이렉트 페이지 최적화: 본문 비움
        String plainText;
        if (page.getRedirectTitle() != null && !page.getRedirectTitle().isEmpty()) {
            plainText = ""; // 리다이렉트는 본문 없음
            log.debug("⏭️ 리다이렉트 페이지 - pageId={}, redirectTo={}", pageId, page.getRedirectTitle());
        } else {
            // Wikitext 변환
            plainText = wikitextRefineService.convertToPlainText(page.getText());
        }

        // 정제 문서 ID 및 체크섬 생성
        String refinedId = "refined_" + HashUtils.sha256Hex(source + pageId);
        String checksum = DigestUtils.sha256Hex(plainText != null ? plainText : "");

        // 도메인 객체 구성
        RefinedDocsWikipedia doc = RefinedDocsWikipedia.builder()
                .id(refinedId)
                .pageId(pageId)
                .contentId(shardCollectionId)
                .source(source)
                .rawUri(rawDataUrl)
                .title(page.getTitle())
                .content(plainText)
                .ns(page.getNs())
                .redirectTitle(page.getRedirectTitle())
                .revisionId(page.getRevisionId())
                .timestamp(page.getTimestamp())
                .contributor(page.getContributor())
                .language("ko")
                .charset("UTF-8")
                .contentLength(plainText != null ? plainText.length() : 0)
                .checksum("sha256:" + checksum)
                .refinedAt(Instant.now())
                .processingTimeMs((int) (System.currentTimeMillis() - pageStart))
                .schemaVersion("1.0")
                .processingStatus("COMPLETED")
                .build();

        return new ProcessedPage(pageId, doc);
    }

    private String classifyError(Exception e) {
        String n = e.getClass().getSimpleName();
        if (n.contains("Minio") || n.contains("S3")) {
            return "MINIO_ERROR";
        }
        if (n.contains("Mongo") || n.contains("Bson")) {
            return "MONGO_ERROR";
        }
        if (n.contains("Json") || n.contains("Jackson")) {
            return "JSON_PARSE_ERROR";
        }
        if (n.contains("Wikitext") || n.contains("Wiki")) {
            return "WIKITEXT_PARSE_ERROR";
        }
        return "REFINE_ERROR";
    }

    private static <T> T retry(Callable<T> callable, int maxAttempts, long backoffMs)
            throws Exception {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (++attempt >= maxAttempts) {
                    throw e;
                }
                try {
                    Thread.sleep(backoffMs * attempt);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 처리된 페이지 정보를 담는 내부 클래스
     */
    private static class ProcessedPage {
        final String pageId;
        RefinedDocsWikipedia doc;

        ProcessedPage(String pageId, RefinedDocsWikipedia doc) {
            this.pageId = pageId;
            this.doc = doc;
        }
    }
}

