package com.realtime.common.infrastructure.persistence.jpa;

import com.realtime.common.domain.content.ContentMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ContentMetadataJpaRepository extends JpaRepository<ContentMetadata, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update ContentMetadata c set c.refinedId = :refinedId where c.source = :source and c.externalId = :externalId")
    int updateRefinedIdBySourceAndExternalId(@Param("refinedId") String refinedId,
                                             @Param("source") String source,
                                             @Param("externalId") String externalId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update ContentMetadata c set c.refinedId = :refinedId where c.source = :source and c.collectionId = :collectionId")
    int updateRefinedIdBySourceAndCollectionId(@Param("refinedId") String refinedId,
                                               @Param("source") String source,
                                               @Param("collectionId") String collectionId);
}

