package com.realtime.serving.infrastructure.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.serving.domain.ranking.RankedKeyword;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis를 사용한 키워드 랭킹 캐시
 * RankedKeyword 전체를 JSON으로 저장하여 sources 정보 보존
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KeywordRankingCache {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String RANKING_KEY = "trending:keywords:full";
    private static final String TIMESTAMP_KEY = "trending:updated_at";
    private static final long CACHE_TTL_HOURS = 24;

    /**
     * 랭킹 데이터를 Redis에 저장 (JSON 직렬화)
     */
    public void saveRanking(List<RankedKeyword> rankedKeywords) {
        try {
            String json = objectMapper.writeValueAsString(rankedKeywords);
            redisTemplate.opsForValue().set(RANKING_KEY, json, CACHE_TTL_HOURS, TimeUnit.HOURS);
            redisTemplate.opsForValue().set(TIMESTAMP_KEY, System.currentTimeMillis());
            log.info("Saved {} keywords to Redis ranking cache", rankedKeywords.size());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize ranking data", e);
        }
    }

    /**
     * 상위 N개 키워드 조회
     */
    public List<RankedKeyword> getTopKeywords(int limit) {
        try {
            Object cached = redisTemplate.opsForValue().get(RANKING_KEY);
            if (cached == null) {
                log.warn("No ranking data found in Redis cache");
                return List.of();
            }

            List<RankedKeyword> allKeywords = objectMapper.readValue(
                    cached.toString(),
                    new TypeReference<List<RankedKeyword>>() {}
            );

            // limit 적용
            List<RankedKeyword> result = allKeywords.stream()
                    .limit(limit)
                    .toList();

            log.debug("Retrieved {} keywords from Redis cache", result.size());
            return result;

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize ranking data", e);
            return List.of();
        }
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
