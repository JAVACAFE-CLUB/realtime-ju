package com.realtime.serving.application.service;

import com.realtime.serving.domain.ranking.RankedKeyword;
import com.realtime.serving.domain.ranking.RawKeyword;
import com.realtime.serving.domain.ranking.ScoreWeights;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 키워드 스코어링 서비스
 * - 소스별 가중치 적용
 * - 시간별 가중치 적용
 *
 * Note: 불용어 필터링은 index-system에서 이미 처리됨
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KeywordScoringService {

    private final ScoreWeights scoreWeights;

    /**
     * 원시 키워드에 가중치를 적용하여 스코어 계산
     */
    public double calculateScore(RawKeyword rawKeyword) {
        double sourceWeight = scoreWeights.getSourceWeight(rawKeyword.getSource());
        double timeWeight = scoreWeights.getTimeWeight(rawKeyword.getIndexedAt());

        return rawKeyword.getDocCount() * sourceWeight * timeWeight;
    }

    /**
     * 여러 소스의 키워드를 통합하고 스코어를 합산하여 랭킹 계산
     */
    public List<RankedKeyword> calculateRanking(Map<String, List<RawKeyword>> keywordsBySource) {
        // 1. 모든 소스의 키워드를 합침
        List<RawKeyword> allKeywords = keywordsBySource.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // 2. 키워드별로 스코어 합산 (불용어는 index-system에서 이미 제거됨)
        Map<String, Double> keywordScores = new HashMap<>();
        for (RawKeyword rawKeyword : allKeywords) {
            double score = calculateScore(rawKeyword);
            keywordScores.merge(rawKeyword.getKeyword(), score, Double::sum);
        }

        // 3. 스코어 순으로 정렬하여 랭킹 부여
        List<RankedKeyword> rankedKeywords = keywordScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .map(entry -> RankedKeyword.builder()
                        .keyword(entry.getKey())
                        .score(entry.getValue())
                        .rank(0)  // 임시 rank, 아래에서 설정
                        .build())
                .collect(Collectors.toList());

        // 4. Rank 설정
        for (int i = 0; i < rankedKeywords.size(); i++) {
            RankedKeyword keyword = rankedKeywords.get(i);
            rankedKeywords.set(i, RankedKeyword.builder()
                    .keyword(keyword.getKeyword())
                    .score(keyword.getScore())
                    .rank(i + 1)
                    .build());
        }

        log.info("Calculated ranking for {} keywords", rankedKeywords.size());

        return rankedKeywords;
    }
}
