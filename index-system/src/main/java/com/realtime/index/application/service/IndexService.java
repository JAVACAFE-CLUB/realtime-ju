package com.realtime.index.application.service;

import com.realtime.index.domain.document.IndexedDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Elasticsearch 색인 서비스
 * Bulk API를 활용하여 배치 색인 수행
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IndexService {

    private final ElasticsearchOperations elasticsearchOperations;

    private static final int BULK_SIZE = 500;
    private static final String INDEX_NAME = "realtime-contents";

    /**
     * 문서 목록을 Elasticsearch에 bulk indexing
     */
    public void bulkIndex(List<IndexedDocument> documents) {
        if (documents == null || documents.isEmpty()) {
            log.warn("No documents to index");
            return;
        }

        try {
            // Batch 단위로 나누기
            List<List<IndexedDocument>> batches = partitionList(documents, BULK_SIZE);

            for (int i = 0; i < batches.size(); i++) {
                List<IndexedDocument> batch = batches.get(i);
                bulkIndexBatch(batch);
                log.info("Bulk indexed batch {}/{}: {} documents",
                        i + 1, batches.size(), batch.size());
            }

            log.info("Successfully indexed {} documents in {} batches",
                    documents.size(), batches.size());

        } catch (Exception e) {
            log.error("Failed to bulk index documents", e);
            throw new RuntimeException("Bulk indexing failed", e);
        }
    }

    /**
     * 단일 배치 색인
     */
    private void bulkIndexBatch(List<IndexedDocument> batch) {
        List<IndexQuery> queries = batch.stream()
                .map(doc -> new IndexQueryBuilder()
                        .withId(doc.getDocumentId())
                        .withObject(doc)
                        .build())
                .collect(Collectors.toList());

        elasticsearchOperations.bulkIndex(
                queries,
                IndexCoordinates.of(INDEX_NAME)
        );
    }

    /**
     * 리스트를 지정된 크기로 분할
     */
    private <T> List<List<T>> partitionList(List<T> list, int size) {
        List<List<T>> partitions = new java.util.ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    /**
     * 단일 문서 색인
     */
    public void indexDocument(IndexedDocument document) {
        try {
            elasticsearchOperations.save(document);
            log.info("Indexed document: {}", document.getDocumentId());
        } catch (Exception e) {
            log.error("Failed to index document: {}", document.getDocumentId(), e);
            throw new RuntimeException("Document indexing failed", e);
        }
    }
}
