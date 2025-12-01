package com.realtime.refine.infrastructure.persistence.mongo;

import com.realtime.refine.domain.docs.wikipedia.RefinedDocsWikipedia;
import com.realtime.refine.domain.docs.wikipedia.RefinedDocsWikipediaRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class RefinedDocsWikipediaRepositoryImpl implements RefinedDocsWikipediaRepository {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Override
    public RefinedDocsWikipedia findById(String id) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            return mongoTemplate.findById(id, RefinedDocsWikipedia.class);
        } catch (Exception e) {
            log.warn("Failed to find document from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public RefinedDocsWikipedia findByPageId(String pageId) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            org.springframework.data.mongodb.core.query.Query query =
                    new org.springframework.data.mongodb.core.query.Query(
                            org.springframework.data.mongodb.core.query.Criteria.where("pageId").is(pageId)
                    );
            return mongoTemplate.findOne(query, RefinedDocsWikipedia.class);
        } catch (Exception e) {
            log.warn("Failed to find document by pageId from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean save(RefinedDocsWikipedia document) {
        if (mongoTemplate == null) {
            log.debug("MongoTemplate not available, skipping save");
            return false;
        }
        try {
            mongoTemplate.save(document);
            log.debug("Saved document to MongoDB: {}", document.getId());
            return true;
        } catch (Exception e) {
            log.warn("Failed to save document to MongoDB: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public Map<String, RefinedDocsWikipedia> findByPageIds(List<String> pageIds) {
        if (mongoTemplate == null || pageIds == null || pageIds.isEmpty()) {
            return new HashMap<>();
        }
        try {
            org.springframework.data.mongodb.core.query.Query query =
                    new org.springframework.data.mongodb.core.query.Query(
                            org.springframework.data.mongodb.core.query.Criteria.where("pageId").in(pageIds)
                    );
            List<RefinedDocsWikipedia> documents = mongoTemplate.find(query, RefinedDocsWikipedia.class);
            return documents.stream()
                    .collect(Collectors.toMap(RefinedDocsWikipedia::getPageId, doc -> doc, (a, b) -> a));
        } catch (Exception e) {
            log.warn("Failed to find documents by pageIds from MongoDB: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    @Override
    public boolean saveAll(List<RefinedDocsWikipedia> documents) {
        if (mongoTemplate == null || documents == null || documents.isEmpty()) {
            log.debug("MongoTemplate not available or empty list, skipping save");
            return false;
        }
        try {
            // BulkOperations로 upsert 수행 (INSERT + UPDATE 모두 가능)
            org.springframework.data.mongodb.core.BulkOperations bulkOps =
                mongoTemplate.bulkOps(
                    org.springframework.data.mongodb.core.BulkOperations.BulkMode.UNORDERED,
                    RefinedDocsWikipedia.class
                );

            for (RefinedDocsWikipedia doc : documents) {
                org.springframework.data.mongodb.core.query.Query query =
                    new org.springframework.data.mongodb.core.query.Query(
                        org.springframework.data.mongodb.core.query.Criteria.where("_id").is(doc.getId())
                    );

                // Update 객체 생성 (모든 필드 설정)
                org.springframework.data.mongodb.core.query.Update update =
                    new org.springframework.data.mongodb.core.query.Update()
                        .set("pageId", doc.getPageId())
                        .set("contentId", doc.getContentId())
                        .set("source", doc.getSource())
                        .set("rawUri", doc.getRawUri())
                        .set("title", doc.getTitle())
                        .set("content", doc.getContent())
                        .set("ns", doc.getNs())
                        .set("redirectTitle", doc.getRedirectTitle())
                        .set("revisionId", doc.getRevisionId())
                        .set("timestamp", doc.getTimestamp())
                        .set("contributor", doc.getContributor())
                        .set("language", doc.getLanguage())
                        .set("charset", doc.getCharset())
                        .set("contentLength", doc.getContentLength())
                        .set("checksum", doc.getChecksum())
                        .set("refinedAt", doc.getRefinedAt())
                        .set("processingTimeMs", doc.getProcessingTimeMs())
                        .set("schemaVersion", doc.getSchemaVersion())
                        .set("processingStatus", doc.getProcessingStatus())
                        .set("indexedAt", doc.getIndexedAt());

                bulkOps.upsert(query, update);
            }

            com.mongodb.bulk.BulkWriteResult result = bulkOps.execute();
            log.debug("Saved {} documents to MongoDB in batch - inserted={}, modified={}",
                documents.size(), result.getInsertedCount(), result.getModifiedCount());
            return true;
        } catch (Exception e) {
            log.warn("Failed to save documents to MongoDB in batch: {}", e.getMessage());
            return false;
        }
    }
}

