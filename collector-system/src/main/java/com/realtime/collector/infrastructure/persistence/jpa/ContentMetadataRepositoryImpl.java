package com.realtime.collector.infrastructure.persistence.jpa;

import com.realtime.common.domain.content.ContentMetadata;
import com.realtime.common.domain.content.ContentMetadataRepository;
import com.realtime.common.infrastructure.persistence.jpa.ContentMetadataJpaRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ContentMetadataRepositoryImpl implements ContentMetadataRepository {

    private final ContentMetadataJpaRepository jpaRepository;

    @Override
    public void saveAll(List<ContentMetadata> entities) {
        jpaRepository.saveAll(entities);
    }

    @Override
    public void save(ContentMetadata entity) {
        jpaRepository.save(entity);
    }
}


