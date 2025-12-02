package com.realtime.refine.infrastructure.persistence.jpa;

import com.realtime.common.infrastructure.persistence.jpa.ContentMetadataJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * MySQL ContentMetadata 트랜잭션 처리 서비스
 * - @Modifying 쿼리는 반드시 트랜잭션 내에서 실행되어야 함
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ContentMetadataService {

    private final ContentMetadataJpaRepository contentMetadataRepository;

    /**
     * refinedId 업데이트 (트랜잭션 처리)
     * @param refinedId 정제 문서 ID
     * @param source 소스 (NEWS_YNA 등)
     * @param externalId 외부 ID (contentId)
     * @return 업데이트된 행 수
     */
    @Transactional
    public int updateRefinedId(String refinedId, String source, String externalId) {
        try {
            int updated = contentMetadataRepository.updateRefinedIdBySourceAndExternalId(
                    refinedId, source, externalId);
            log.debug("✅ MySQL 메타데이터 업데이트 완료 - source={}, externalId={}, refinedId={}, updatedRows={}", 
                    source, externalId, refinedId, updated);
            return updated;
        } catch (Exception e) {
            log.warn("⚠️ MySQL 메타데이터 업데이트 실패 - source={}, externalId={}, error={}", 
                    source, externalId, e.getMessage());
            // 메타데이터 업데이트 실패는 전체 흐름을 중단하지 않음
            return 0;
        }
    }
}

