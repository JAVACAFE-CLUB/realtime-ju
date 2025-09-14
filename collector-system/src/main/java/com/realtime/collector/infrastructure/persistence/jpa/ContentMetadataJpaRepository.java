package com.realtime.collector.infrastructure.persistence.jpa;

import com.realtime.collector.domain.content.ContentMetadata;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContentMetadataJpaRepository extends CrudRepository<ContentMetadata, Long> {
}


