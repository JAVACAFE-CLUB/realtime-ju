package com.realtime.index.application.processor;

import com.realtime.common.constants.ContentSource;
import com.realtime.common.kafka.message.RefineMessage;
import com.realtime.index.application.service.ElasticsearchKeywordExtractor;
import com.realtime.index.application.service.IdempotencyService;
import com.realtime.index.application.service.IndexService;
import com.realtime.index.domain.document.IndexedDocument;
import com.realtime.index.domain.metadata.IndexMetadata;
import com.realtime.index.infrastructure.persistence.mongo.RefinedDocumentReader;
import com.realtime.index.infrastructure.persistence.mongo.RefinedSnsYouTube;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * YouTube 문서 색인 프로세서
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class YouTubeIndexProcessor {

    private final RefinedDocumentReader documentReader;
    private final IndexService indexService;
    private final ElasticsearchKeywordExtractor keywordExtractor;
    private final IdempotencyService idempotencyService;

    @Transactional
    public void processBatch(List<RefineMessage> messages) {
        log.info("Processing {} YouTube refined messages", messages.size());

        // 1. 중복 제거
        List<RefineMessage> uniqueMessages = messages.stream()
                .filter(msg -> !idempotencyService.isProcessed(msg.getRefinedId()))
                .collect(Collectors.toList());

        if (uniqueMessages.isEmpty()) {
            log.info("All messages already processed, skipping");
            return;
        }

        log.info("Processing {} unique YouTube messages", uniqueMessages.size());

        // 2. MongoDB에서 정제된 문서 조회
        List<String> refinedIds = uniqueMessages.stream()
                .map(RefineMessage::getRefinedId)
                .collect(Collectors.toList());

        List<RefinedSnsYouTube> refinedDocuments = documentReader.findYouTubeDocuments(refinedIds);

        // 2-1. 찾지 못한 문서 추적
        if (refinedDocuments.isEmpty()) {
            log.error("❌ CRITICAL: No refined documents found in MongoDB for {} requested IDs. This means data loss!",
                    refinedIds.size());
            log.error("❌ Missing YouTube IDs (first 10): {}",
                    refinedIds.stream().limit(10).collect(Collectors.toList()));
            return;
        }

        // 2-2. 부분 실패 감지
        if (refinedDocuments.size() < refinedIds.size()) {
            List<String> foundIds = refinedDocuments.stream()
                    .map(RefinedSnsYouTube::getId)
                    .collect(Collectors.toList());
            List<String> missingIds = refinedIds.stream()
                    .filter(id -> !foundIds.contains(id))
                    .collect(Collectors.toList());

            log.error("⚠️ WARNING: Partial failure - Found {}/{} YouTube documents in MongoDB",
                    refinedDocuments.size(), refinedIds.size());
            log.error("⚠️ WARNING: Missing YouTube IDs ({}): {}",
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
            log.info("Successfully indexed {} YouTube documents", indexDocuments.size());

        } catch (Exception e) {
            log.error("Failed to index YouTube documents", e);
            throw new RuntimeException("YouTube indexing failed", e);
        }
    }

    private IndexedDocument convertToIndexDocument(RefinedSnsYouTube doc) {
        // 키워드 추출 (title + description + tags)
        String fullText = (doc.getTitle() != null ? doc.getTitle() : "") + " " +
                (doc.getDescription() != null ? doc.getDescription() : "") + " " +
                (doc.getTags() != null ? doc.getTags() : "");

        List<String> keywords = keywordExtractor.extract(fullText);

        // tags를 keywords에 추가
        if (doc.getTags() != null && !doc.getTags().isBlank()) {
            List<String> tagList = Arrays.stream(doc.getTags().split(","))
                    .map(String::trim)
                    .filter(tag -> !tag.isEmpty())
                    .collect(Collectors.toList());
            keywords.addAll(tagList);
        }

        // 메타데이터 구성
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("videoId", doc.getContentId());
        metadata.put("channelId", doc.getChannelId());
        metadata.put("channelTitle", doc.getChannelTitle());
        metadata.put("categoryId", doc.getCategoryId());
        metadata.put("thumbnailUrl", doc.getThumbnailUrl());
        metadata.put("rawUri", doc.getRawUri());
        metadata.put("language", doc.getLanguage());

        // publishedAt 파싱 (ISO 8601)
        LocalDateTime publishedAt = parsePublishedAt(doc.getPublishedAt());

        return IndexedDocument.builder()
                .documentId(doc.getId())
                .sourceType(ContentSource.SNS_YOUTUBE)
                .title(doc.getTitle())
                .content(doc.getDescription())
                .keywords(keywords)
                .author(doc.getChannelTitle())
                .category(doc.getCategoryId())
                .publishedAt(publishedAt)
                .indexedAt(LocalDateTime.now())
                .popularity(0L)  // 향후 조회수/좋아요 정보 추가 가능
                .metadata(metadata)
                .build();
    }

    private void saveIndexMetadata(List<RefinedSnsYouTube> documents) {
        documents.forEach(doc -> {
            IndexMetadata metadata = IndexMetadata.builder()
                    .refinedId(doc.getId())
                    .documentId(doc.getId())
                    .sourceType(ContentSource.SNS_YOUTUBE)
                    .indexedAt(LocalDateTime.now())
                    .isSuccess(true)
                    .build();

            idempotencyService.markAsProcessed(metadata);
        });
    }

    private LocalDateTime parsePublishedAt(String publishedAt) {
        if (publishedAt == null || publishedAt.isBlank()) {
            return null;
        }

        try {
            // ISO 8601: 2024-12-02T10:30:00Z
            return LocalDateTime.parse(publishedAt.replace("Z", ""));
        } catch (Exception e) {
            log.warn("Failed to parse publishedAt: {}", publishedAt);
            return null;
        }
    }
}
