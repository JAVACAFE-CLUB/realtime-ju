package com.realtime.index.application.service;

import com.realtime.index.domain.metadata.IndexMetadata;
import com.realtime.index.domain.metadata.IndexMetadataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 중복 색인 방지 서비스
 * RefineMessage의 refinedId를 기반으로 이미 색인되었는지 확인
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final IndexMetadataRepository indexMetadataRepository;

    /**
     * 이미 처리된 refinedId인지 확인
     */
    public boolean isProcessed(String refinedId) {
        boolean exists = indexMetadataRepository.existsByRefinedId(refinedId);
        if (exists) {
            log.debug("Skipping already indexed refinedId: {}", refinedId);
        }
        return exists;
    }

    /**
     * 처리 완료 표시 (색인 메타데이터 저장)
     */
    @Transactional
    public void markAsProcessed(IndexMetadata metadata) {
        indexMetadataRepository.save(metadata);
        log.debug("Marked refinedId as indexed: {}", metadata.getRefinedId());
    }
}
