package com.realtime.serving.application.service;

import com.realtime.common.constants.ContentSource;
import com.realtime.serving.domain.ranking.RankedKeyword;
import com.realtime.serving.domain.ranking.RawKeyword;
import com.realtime.serving.domain.ranking.ScoreWeights;
import com.realtime.serving.domain.ranking.SourceLink;
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

        // 2. 키워드별로 스코어 합산 및 소스 링크 수집
        Map<String, Double> keywordScores = new HashMap<>();
        Map<String, Map<ContentSource, RawKeyword>> keywordSources = new HashMap<>();

        for (RawKeyword rawKeyword : allKeywords) {
            String keyword = rawKeyword.getKeyword();
            double score = calculateScore(rawKeyword);
            keywordScores.merge(keyword, score, Double::sum);

            // 소스별 대표 문서 정보 저장 (가장 높은 docCount 기준)
            keywordSources.computeIfAbsent(keyword, k -> new HashMap<>())
                    .merge(rawKeyword.getSource(), rawKeyword, (existing, newOne) ->
                            existing.getDocCount() >= newOne.getDocCount() ? existing : newOne);
        }

        // 3. 스코어 순으로 정렬하여 랭킹 부여
        List<RankedKeyword> rankedKeywords = keywordScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .map(entry -> {
                    String keyword = entry.getKey();
                    List<SourceLink> sources = buildSourceLinks(keywordSources.get(keyword));
                    return RankedKeyword.builder()
                            .keyword(keyword)
                            .score(entry.getValue())
                            .rank(0)  // 임시 rank, 아래에서 설정
                            .sources(sources)
                            .build();
                })
                .collect(Collectors.toList());

        // 4. Rank 설정
        for (int i = 0; i < rankedKeywords.size(); i++) {
            RankedKeyword keyword = rankedKeywords.get(i);
            rankedKeywords.set(i, RankedKeyword.builder()
                    .keyword(keyword.getKeyword())
                    .score(keyword.getScore())
                    .rank(i + 1)
                    .sources(keyword.getSources())
                    .build());
        }

        log.info("Calculated ranking for {} keywords", rankedKeywords.size());

        return rankedKeywords;
    }

    /**
     * 소스별 RawKeyword 정보를 SourceLink 리스트로 변환
     */
    private List<SourceLink> buildSourceLinks(Map<ContentSource, RawKeyword> sourceMap) {
        if (sourceMap == null || sourceMap.isEmpty()) {
            return List.of();
        }

        return sourceMap.entrySet().stream()
                .filter(entry -> entry.getValue().getDocumentUrl() != null)
                .map(entry -> SourceLink.builder()
                        .source(mapSourceName(entry.getKey()))
                        .url(entry.getValue().getDocumentUrl())
                        .title(entry.getValue().getDocumentTitle())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * ContentSource를 API 응답용 소스 이름으로 변환
     */
    private String mapSourceName(ContentSource source) {
        switch (source) {
            case NEWS_YNA:
                return "yna";
            case DOCS_WIKIPEDIA:
                return "wikipedia";
            case SNS_YOUTUBE:
                return "youtube";
            default:
                return source.name().toLowerCase();
        }
    }
}
