package com.realtime.index.domain.document;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface IndexedDocumentRepository extends ElasticsearchRepository<IndexedDocument, String> {

    Optional<IndexedDocument> findByDocumentId(String documentId);

    List<IndexedDocument> findByDocumentIdIn(List<String> documentIds);

    boolean existsByDocumentId(String documentId);
}
