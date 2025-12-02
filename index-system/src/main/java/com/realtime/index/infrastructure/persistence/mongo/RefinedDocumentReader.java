package com.realtime.index.infrastructure.persistence.mongo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * MongoDB에서 정제된 문서를 조회하는 Reader
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RefinedDocumentReader {

    private final MongoTemplate mongoTemplate;

    /**
     * Wikipedia 문서 조회 (ID 목록)
     */
    public List<RefinedDocsWikipedia> findWikipediaDocuments(List<String> ids) {
        Query query = new Query(Criteria.where("_id").in(ids));
        List<RefinedDocsWikipedia> documents = mongoTemplate.find(query, RefinedDocsWikipedia.class);
        log.info("Found {} Wikipedia documents out of {} requested", documents.size(), ids.size());
        return documents;
    }

    /**
     * 연합뉴스 문서 조회 (ID 목록)
     */
    public List<RefinedNewsYna> findYnaDocuments(List<String> ids) {
        Query query = new Query(Criteria.where("_id").in(ids));
        List<RefinedNewsYna> documents = mongoTemplate.find(query, RefinedNewsYna.class);
        log.info("Found {} YNA documents out of {} requested", documents.size(), ids.size());
        return documents;
    }

    /**
     * YouTube 문서 조회 (ID 목록)
     */
    public List<RefinedSnsYouTube> findYouTubeDocuments(List<String> ids) {
        Query query = new Query(Criteria.where("_id").in(ids));
        List<RefinedSnsYouTube> documents = mongoTemplate.find(query, RefinedSnsYouTube.class);
        log.info("Found {} YouTube documents out of {} requested", documents.size(), ids.size());
        return documents;
    }

    /**
     * Wikipedia 문서 단건 조회
     */
    public RefinedDocsWikipedia findWikipediaDocument(String id) {
        return mongoTemplate.findById(id, RefinedDocsWikipedia.class);
    }

    /**
     * 연합뉴스 문서 단건 조회
     */
    public RefinedNewsYna findYnaDocument(String id) {
        return mongoTemplate.findById(id, RefinedNewsYna.class);
    }

    /**
     * YouTube 문서 단건 조회
     */
    public RefinedSnsYouTube findYouTubeDocument(String id) {
        return mongoTemplate.findById(id, RefinedSnsYouTube.class);
    }
}
