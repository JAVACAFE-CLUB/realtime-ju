package com.realtime.refine.infrastructure.persistence.mongo;

import com.realtime.refine.domain.news.yna.RefinedNewsYna;
import com.realtime.refine.domain.news.yna.RefinedNewsYnaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class RefinedNewsYnaRepositoryImpl implements RefinedNewsYnaRepository {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Override
    public RefinedNewsYna findById(String id) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            return mongoTemplate.findById(id, RefinedNewsYna.class);
        } catch (Exception e) {
            log.warn("Failed to find document from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public RefinedNewsYna findByContentId(String contentId) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            org.springframework.data.mongodb.core.query.Query query =
                    new org.springframework.data.mongodb.core.query.Query(
                            org.springframework.data.mongodb.core.query.Criteria.where("contentId").is(contentId)
                    );
            return mongoTemplate.findOne(query, RefinedNewsYna.class);
        } catch (Exception e) {
            log.warn("Failed to find document by contentId from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean save(RefinedNewsYna document) {
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
}



