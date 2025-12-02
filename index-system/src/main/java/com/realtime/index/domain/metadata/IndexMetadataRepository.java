package com.realtime.index.domain.metadata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IndexMetadataRepository extends JpaRepository<IndexMetadata, Long> {

    Optional<IndexMetadata> findByRefinedId(String refinedId);

    boolean existsByRefinedId(String refinedId);
}
