package com.realtime.serving.infrastructure.cache;

import com.realtime.serving.domain.ranking.RankedKeyword;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Redis를 사용한 키워드 랭킹 캐시
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KeywordRankingCache {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String RANKING_KEY = "trending:keywords";
    private static final String TIMESTAMP_KEY = "trending:updated_at";

    /**
     * 랭킹 데이터를 Redis에 저장
     * Sorted Set을 사용하여 score 기준으로 자동 정렬
     */
    public void saveRanking(List<RankedKeyword> rankedKeywords) {
        ZSetOperations<String, Object> zSetOps = redisTemplate.opsForZSet();

        // 기존 데이터 삭제
        redisTemplate.delete(RANKING_KEY);

        // 새로운 데이터 저장 (score가 높을수록 상위)
        for (RankedKeyword keyword : rankedKeywords) {
            zSetOps.add(RANKING_KEY, keyword.getKeyword(), keyword.getScore());
        }

        // 업데이트 시간 저장
        redisTemplate.opsForValue().set(TIMESTAMP_KEY, System.currentTimeMillis());

        log.info("Saved {} keywords to Redis ranking cache", rankedKeywords.size());
    }

    /**
     * 상위 N개 키워드 조회
     */
    public List<RankedKeyword> getTopKeywords(int limit) {
        ZSetOperations<String, Object> zSetOps = redisTemplate.opsForZSet();

        // score 역순으로 조회 (높은 점수가 먼저)
        Set<ZSetOperations.TypedTuple<Object>> results =
                zSetOps.reverseRangeWithScores(RANKING_KEY, 0, limit - 1);

        if (results == null || results.isEmpty()) {
            log.warn("No ranking data found in Redis cache");
            return List.of();
        }

        List<RankedKeyword> rankedKeywords = new ArrayList<>();
        int rank = 1;
        for (ZSetOperations.TypedTuple<Object> result : results) {
            rankedKeywords.add(RankedKeyword.builder()
                    .keyword((String) result.getValue())
                    .score(result.getScore() != null ? result.getScore() : 0.0)
                    .rank(rank++)
                    .build());
        }

        log.debug("Retrieved {} keywords from Redis cache", rankedKeywords.size());
        return rankedKeywords;
    }

    /**
     * 마지막 업데이트 시간 조회
     */
    public Long getLastUpdateTime() {
        Object timestamp = redisTemplate.opsForValue().get(TIMESTAMP_KEY);
        return timestamp != null ? (Long) timestamp : null;
    }

    /**
     * 캐시 초기화
     */
    public void clear() {
        redisTemplate.delete(RANKING_KEY);
        redisTemplate.delete(TIMESTAMP_KEY);
        log.info("Cleared ranking cache");
    }
}
