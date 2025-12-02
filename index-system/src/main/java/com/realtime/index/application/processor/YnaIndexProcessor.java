package com.realtime.index.application.processor;

import com.realtime.common.constants.ContentSource;
import com.realtime.common.kafka.message.RefineMessage;
import com.realtime.index.application.service.ElasticsearchKeywordExtractor;
import com.realtime.index.application.service.IdempotencyService;
import com.realtime.index.application.service.IndexService;
import com.realtime.index.domain.document.IndexedDocument;
import com.realtime.index.domain.metadata.IndexMetadata;
import com.realtime.index.infrastructure.persistence.mongo.RefinedDocumentReader;
import com.realtime.index.infrastructure.persistence.mongo.RefinedNewsYna;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 연합뉴스 문서 색인 프로세서
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class YnaIndexProcessor {

    private final RefinedDocumentReader documentReader;
    private final IndexService indexService;
    private final ElasticsearchKeywordExtractor keywordExtractor;
    private final IdempotencyService idempotencyService;

    @Transactional
    public void processBatch(List<RefineMessage> messages) {
        log.info("Processing {} YNA refined messages", messages.size());

        // 1. 중복 제거
        List<RefineMessage> uniqueMessages = messages.stream()
                .filter(msg -> !idempotencyService.isProcessed(msg.getRefinedId()))
                .collect(Collectors.toList());

        if (uniqueMessages.isEmpty()) {
            log.info("All messages already processed, skipping");
            return;
        }

        log.info("Processing {} unique YNA messages", uniqueMessages.size());

        // 2. MongoDB에서 정제된 문서 조회
        List<String> refinedIds = uniqueMessages.stream()
                .map(RefineMessage::getRefinedId)
                .collect(Collectors.toList());

        List<RefinedNewsYna> refinedDocuments = documentReader.findYnaDocuments(refinedIds);

        // 2-1. 찾지 못한 문서 추적
        if (refinedDocuments.isEmpty()) {
            log.error("❌ CRITICAL: No refined documents found in MongoDB for {} requested IDs. This means data loss!",
                    refinedIds.size());
            log.error("❌ Missing YNA IDs (first 10): {}",
                    refinedIds.stream().limit(10).collect(Collectors.toList()));
            return;
        }

        // 2-2. 부분 실패 감지
        if (refinedDocuments.size() < refinedIds.size()) {
            List<String> foundIds = refinedDocuments.stream()
                    .map(RefinedNewsYna::getId)
                    .collect(Collectors.toList());
            List<String> missingIds = refinedIds.stream()
                    .filter(id -> !foundIds.contains(id))
                    .collect(Collectors.toList());

            log.error("⚠️ WARNING: Partial failure - Found {}/{} YNA documents in MongoDB",
                    refinedDocuments.size(), refinedIds.size());
            log.error("⚠️ WARNING: Missing YNA IDs ({}): {}",
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
            saveIndexMetadata(refinedDocuments);
            log.info("Successfully indexed {} YNA documents", indexDocuments.size());

        } catch (Exception e) {
            log.error("Failed to index YNA documents", e);
            throw new RuntimeException("YNA indexing failed", e);
        }
    }

    private IndexedDocument convertToIndexDocument(RefinedNewsYna doc) {
        // 키워드 추출
        String fullText = (doc.getTitle() != null ? doc.getTitle() : "") + " " +
                (doc.getContent() != null ? doc.getContent() : "");
        List<String> keywords = keywordExtractor.extract(fullText);

        // 메타데이터 구성
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("contentId", doc.getContentId());
        metadata.put("rawUri", doc.getRawUri());
        metadata.put("language", doc.getLanguage());
        metadata.put("charset", doc.getCharset());
        metadata.put("contentLength", doc.getContentLength());
        metadata.put("checksum", doc.getChecksum());

        return IndexedDocument.builder()
                .documentId(doc.getId())
                .sourceType(ContentSource.NEWS_YNA)
                .title(doc.getTitle())
                .content(doc.getContent())
                .keywords(keywords)
                .author("연합뉴스")
                .category("news")
                .publishedAt(doc.getRefinedAt() != null ?
                        LocalDateTime.ofInstant(doc.getRefinedAt(), java.time.ZoneId.systemDefault()) : null)
                .indexedAt(LocalDateTime.now())
                .popularity(0L)
                .metadata(metadata)
                .build();
    }

    private void saveIndexMetadata(List<RefinedNewsYna> documents) {
        documents.forEach(doc -> {
            IndexMetadata metadata = IndexMetadata.builder()
                    .refinedId(doc.getId())
                    .documentId(doc.getId())
                    .sourceType(ContentSource.NEWS_YNA)
                    .indexedAt(LocalDateTime.now())
                    .isSuccess(true)
                    .build();

            idempotencyService.markAsProcessed(metadata);
        });
    }
}
