package com.realtime.refine.infrastructure.persistence.mongo;

import com.realtime.refine.domain.sns.youtube.RefinedSnsYouTube;
import com.realtime.refine.domain.sns.youtube.RefinedSnsYouTubeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class RefinedSnsYouTubeRepositoryImpl implements RefinedSnsYouTubeRepository {

    @Autowired(required = false)
    private MongoTemplate mongoTemplate;

    @Override
    public RefinedSnsYouTube findById(String id) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            return mongoTemplate.findById(id, RefinedSnsYouTube.class);
        } catch (Exception e) {
            log.warn("Failed to find document from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public RefinedSnsYouTube findByContentId(String contentId) {
        if (mongoTemplate == null) {
            return null;
        }
        try {
            org.springframework.data.mongodb.core.query.Query query =
                    new org.springframework.data.mongodb.core.query.Query(
                            org.springframework.data.mongodb.core.query.Criteria.where("contentId").is(contentId)
                    );
            return mongoTemplate.findOne(query, RefinedSnsYouTube.class);
        } catch (Exception e) {
            log.warn("Failed to find document by contentId from MongoDB: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean save(RefinedSnsYouTube document) {
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
