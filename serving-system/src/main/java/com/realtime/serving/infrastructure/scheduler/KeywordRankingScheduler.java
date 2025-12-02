package com.realtime.serving.infrastructure.scheduler;

import com.realtime.common.constants.ContentSource;
import com.realtime.serving.application.service.KeywordAggregationService;
import com.realtime.serving.application.service.KeywordScoringService;
import com.realtime.serving.domain.ranking.RankedKeyword;
import com.realtime.serving.domain.ranking.RawKeyword;
import com.realtime.serving.infrastructure.cache.KeywordRankingCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 키워드 랭킹 업데이트 스케줄러
 * - 주기적으로 Elasticsearch에서 키워드 집계
 * - 가중치를 적용하여 스코어 계산
 * - Redis에 랭킹 저장
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KeywordRankingScheduler {

    private final KeywordAggregationService aggregationService;
    private final KeywordScoringService scoringService;
    private final KeywordRankingCache rankingCache;

    /**
     * 애플리케이션 시작 시 10초 후 초기 랭킹 업데이트
     */
    @PostConstruct
    public void init() {
        CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS)
                .execute(this::updateKeywordRanking);
    }

    /**
     * 매일 자정에 키워드 랭킹 업데이트 (하루 기준)
     */
    @Scheduled(cron = "0 0 0 * * *", zone = "Asia/Seoul")
    public void updateKeywordRanking() {
        log.info("🔄 Starting keyword ranking update...");

        try {
            // 1. 최근 24시간의 키워드 집계
            LocalDateTime since = LocalDateTime.now().minusHours(24);
            Map<ContentSource, List<RawKeyword>> keywordsBySource = aggregationService.aggregateAllKeywords(since);

            // 2. 소스별 키워드 개수 로깅
            logKeywordCounts(keywordsBySource);

            // 3. 스코어링 및 랭킹 계산
            Map<String, List<RawKeyword>> keywordsBySourceString = new HashMap<>();
            keywordsBySource.forEach((source, keywords) ->
                    keywordsBySourceString.put(source.name(), keywords));

            List<RankedKeyword> rankedKeywords = scoringService.calculateRanking(keywordsBySourceString);

            // 4. Redis에 저장
            rankingCache.saveRanking(rankedKeywords);

            // 5. 상위 10개 로깅
            logTopKeywords(rankedKeywords);

            log.info("✅ Keyword ranking update completed successfully");

        } catch (Exception e) {
            log.error("❌ Failed to update keyword ranking", e);
        }
    }

    private void logKeywordCounts(Map<ContentSource, List<RawKeyword>> keywordsBySource) {
        keywordsBySource.forEach((source, keywords) ->
                log.info("  📊 {}: {} keywords", source, keywords.size()));
    }

    private void logTopKeywords(List<RankedKeyword> rankedKeywords) {
        log.info("🏆 Top 10 keywords:");
        rankedKeywords.stream()
                .limit(10)
                .forEach(keyword -> log.info("  {}. {} (score: {})",
                        keyword.getRank(), keyword.getKeyword(), String.format("%.2f", keyword.getScore())));
    }
}
