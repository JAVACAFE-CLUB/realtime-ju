package com.realtime.index.application.processor;

import com.realtime.common.constants.ContentSource;
import com.realtime.common.kafka.message.RefineMessage;
import com.realtime.index.application.service.ElasticsearchKeywordExtractor;
import com.realtime.index.application.service.IdempotencyService;
import com.realtime.index.application.service.IndexService;
import com.realtime.index.domain.document.IndexedDocument;
import com.realtime.index.domain.metadata.IndexMetadata;
import com.realtime.index.infrastructure.persistence.mongo.RefinedDocumentReader;
import com.realtime.index.infrastructure.persistence.mongo.RefinedDocsWikipedia;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Wikipedia 문서 색인 프로세서
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class WikipediaIndexProcessor {

    private final RefinedDocumentReader documentReader;
    private final IndexService indexService;
    private final ElasticsearchKeywordExtractor keywordExtractor;
    private final IdempotencyService idempotencyService;

    /**
     * RefineMessage 배치를 처리하여 Elasticsearch에 색인
     */
    @Transactional
    public void processBatch(List<RefineMessage> messages) {
        log.info("Processing {} Wikipedia refined messages", messages.size());

        // 1. 중복 제거 (이미 색인된 것 제외)
        List<RefineMessage> uniqueMessages = messages.stream()
                .filter(msg -> !idempotencyService.isProcessed(msg.getRefinedId()))
                .collect(Collectors.toList());

        if (uniqueMessages.isEmpty()) {
            log.info("All messages already processed, skipping");
            return;
        }

        log.info("Processing {} unique Wikipedia messages", uniqueMessages.size());

        // 2. MongoDB에서 정제된 문서 조회
        List<String> refinedIds = uniqueMessages.stream()
                .map(RefineMessage::getRefinedId)
                .collect(Collectors.toList());

        List<RefinedDocsWikipedia> refinedDocuments = documentReader.findWikipediaDocuments(refinedIds);

        // 2-1. 찾지 못한 문서 추적
        if (refinedDocuments.isEmpty()) {
            log.error("❌ CRITICAL: No refined documents found in MongoDB for {} requested IDs. This means data loss!",
                    refinedIds.size());
            log.error("❌ Missing Wikipedia IDs (first 10): {}",
                    refinedIds.stream().limit(10).collect(Collectors.toList()));
            return;
        }

        // 2-2. 부분 실패 감지
        if (refinedDocuments.size() < refinedIds.size()) {
            List<String> foundIds = refinedDocuments.stream()
                    .map(RefinedDocsWikipedia::getId)
                    .collect(Collectors.toList());
            List<String> missingIds = refinedIds.stream()
                    .filter(id -> !foundIds.contains(id))
                    .collect(Collectors.toList());

            log.error("⚠️ WARNING: Partial failure - Found {}/{} Wikipedia documents in MongoDB",
                    refinedDocuments.size(), refinedIds.size());
            log.error("⚠️ Missing Wikipedia IDs ({}): {}",
                    missingIds.size(),
                    missingIds.stream().limit(10).collect(Collectors.toList()));
        }

        // 3. Elasticsearch 문서로 변환
        List<IndexedDocument> indexDocuments = refinedDocuments.stream()
                .map(this::convertToIndexDocument)
                .collect(Collectors.toList());

        // 4. Bulk indexing
        try {
            indexService.bulkIndex(indexDocuments);

            // 5. 메타데이터 저장 (idempotency)
            saveIndexMetadata(refinedDocuments);

            log.info("Successfully indexed {} Wikipedia documents", indexDocuments.size());

        } catch (Exception e) {
            log.error("Failed to index Wikipedia documents", e);
            throw new RuntimeException("Wikipedia indexing failed", e);
        }
    }

    /**
     * RefinedDocsWikipedia를 IndexedDocument로 변환
     */
    private IndexedDocument convertToIndexDocument(RefinedDocsWikipedia doc) {
        // 키워드 추출
        String fullText = (doc.getTitle() != null ? doc.getTitle() : "") + " " +
                (doc.getContent() != null ? doc.getContent() : "");
        List<String> keywords = keywordExtractor.extract(fullText);

        // 메타데이터 구성
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("pageId", doc.getPageId());
        metadata.put("ns", doc.getNs());
        metadata.put("redirectTitle", doc.getRedirectTitle());
        metadata.put("revisionId", doc.getRevisionId());
        metadata.put("contributor", doc.getContributor());
        metadata.put("language", doc.getLanguage());
        metadata.put("contentLength", doc.getContentLength());

        // 발행일시 파싱 (timestamp는 ISO 8601 형식)
        LocalDateTime publishedAt = parseTimestamp(doc.getTimestamp());

        return IndexedDocument.builder()
                .documentId(doc.getId())
                .sourceType(ContentSource.DOCS_WIKIPEDIA)
                .title(doc.getTitle())
                .content(doc.getContent())
                .keywords(keywords)
                .author(doc.getContributor())
                .category(doc.getNs() != null ? "namespace_" + doc.getNs() : null)
                .publishedAt(publishedAt)
                .indexedAt(LocalDateTime.now())
                .popularity(0L)  // Wikipedia는 조회수 정보 없음
                .metadata(metadata)
                .build();
    }

    /**
     * 색인 메타데이터 저장
     */
    private void saveIndexMetadata(List<RefinedDocsWikipedia> documents) {
        documents.forEach(doc -> {
            IndexMetadata metadata = IndexMetadata.builder()
                    .refinedId(doc.getId())
                    .documentId(doc.getId())
                    .sourceType(ContentSource.DOCS_WIKIPEDIA)
                    .indexedAt(LocalDateTime.now())
                    .isSuccess(true)
                    .build();

            idempotencyService.markAsProcessed(metadata);
        });
    }

    /**
     * ISO 8601 타임스탬프 파싱
     */
    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return null;
        }

        try {
            // Wikipedia timestamp format: 2024-12-02T10:30:00Z
            return LocalDateTime.parse(timestamp.replace("Z", ""));
        } catch (Exception e) {
            log.warn("Failed to parse timestamp: {}", timestamp);
            return null;
        }
    }
}
