package com.realtime.serving.application.service;

import com.realtime.serving.application.dto.KeywordRankingResponse;
import com.realtime.serving.domain.ranking.RankedKeyword;
import com.realtime.serving.infrastructure.cache.KeywordRankingCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 키워드 랭킹 조회 서비스
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KeywordRankingService {

    private final KeywordRankingCache rankingCache;

    /**
     * 상위 N개의 트렌딩 키워드 조회
     */
    public KeywordRankingResponse getTopKeywords(int limit) {
        List<RankedKeyword> keywords = rankingCache.getTopKeywords(limit);
        Long lastUpdatedAt = rankingCache.getLastUpdateTime();

        log.debug("Retrieved top {} keywords", limit);
        return KeywordRankingResponse.of(keywords, lastUpdatedAt);
    }

    /**
     * 기본 상위 10개 트렌딩 키워드 조회
     */
    public KeywordRankingResponse getTopKeywords() {
        return getTopKeywords(10);
    }
}
