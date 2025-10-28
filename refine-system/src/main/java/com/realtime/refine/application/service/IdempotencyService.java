package com.realtime.refine.application.service;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final StringRedisTemplate redisTemplate;

    private static final Duration TTL = Duration.ofDays(7);

    public boolean alreadyProcessed(String source, String contentId) {
        String key = buildKey(source, contentId);
        Boolean exists = redisTemplate.hasKey(key);
        boolean processed = exists != null && exists;
        
        if (processed) {
            log.debug("🔍 Redis 중복 체크 - key={}, result=이미 처리됨", key);
        } else {
            log.debug("🔍 Redis 중복 체크 - key={}, result=미처리", key);
        }
        
        return processed;
    }

    public void markProcessed(String source, String contentId, String refinedId) {
        String key = buildKey(source, contentId);
        redisTemplate.opsForValue().set(key, refinedId, TTL);
        log.debug("💾 Redis 처리 마커 저장 - key={}, refinedId={}, ttl={}일", key, refinedId, TTL.toDays());
    }

    private String buildKey(String source, String contentId) {
        return "processed:refine:" + source + ":" + contentId;
    }
}



