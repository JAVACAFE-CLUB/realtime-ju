package com.realtime.collector.infrastructure.persistence.jpa;

import com.realtime.collector.domain.content.ContentMetadata;
import com.realtime.collector.domain.content.ContentMetadataRepository;
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
}


