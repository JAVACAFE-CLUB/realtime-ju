package com.realtime.index.application.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.AnalyzeRequest;
import co.elastic.clients.elasticsearch.indices.AnalyzeResponse;
import co.elastic.clients.elasticsearch.indices.analyze.AnalyzeToken;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Elasticsearch의 _analyze API를 활용한 키워드 추출 서비스
 * nori tokenizer를 사용하여 한국어 형태소 분석 및 불용어 제거
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchKeywordExtractor {

    private final ElasticsearchClient elasticsearchClient;

    private static final int MAX_KEYWORDS = 10;
    private static final int MIN_TOKEN_LENGTH = 2;
    private static final int MAX_TEXT_LENGTH = 50000;  // 최대 50,000자 (토큰 수 제한 회피)

    // 불용어 목록
    private static final Set<String> STOPWORDS = Set.of(
            // 한국어 조사/어미
            "있다", "있는", "하다", "되다", "이다", "것", "수", "등", "그", "및", "또는",
            "이", "가", "을", "를", "의", "에", "와", "과", "도", "만",
            // Wikipedia 메타데이터
            "탄생", "사망", "기년", "사건", "분류", "연호", "왕조", "출생", "년", "월", "일",
            "template"
    );

    /**
     * 키워드가 불용어인지 확인
     */
    private boolean isStopword(String keyword) {
        if (keyword == null || keyword.isBlank()) {
            return true;
        }
        // 순수 숫자도 제거
        if (keyword.matches("\\d+")) {
            return true;
        }
        return STOPWORDS.contains(keyword);
    }

    /**
     * Elasticsearch의 korean_analyzer를 사용하여 키워드 추출
     *
     * @param text 분석할 텍스트
     * @return 추출된 키워드 목록 (빈도 높은 순)
     */
    public List<String> extract(String text) {
        if (text == null || text.isBlank()) {
            return Collections.emptyList();
        }

        // 토큰 수 제한을 피하기 위해 텍스트 길이 제한
        final String analyzableText = text.length() > MAX_TEXT_LENGTH
                ? text.substring(0, MAX_TEXT_LENGTH)
                : text;

        if (text.length() > MAX_TEXT_LENGTH) {
            log.debug("Text truncated to {} characters to avoid token limit", MAX_TEXT_LENGTH);
        }

        try {
            // Elasticsearch _analyze API 호출
            AnalyzeRequest request = AnalyzeRequest.of(a -> a
                    .analyzer("nori")  // nori analyzer 사용 (한국어 형태소 분석)
                    .text(analyzableText)
            );

            AnalyzeResponse response = elasticsearchClient.indices().analyze(request);

            // 토큰 추출 및 빈도 계산 (불용어 제거)
            Map<String, Long> tokenFrequency = response.tokens().stream()
                    .map(AnalyzeToken::token)
                    .filter(token -> token.length() >= MIN_TOKEN_LENGTH)
                    .filter(token -> !isStopword(token))  // 불용어 및 숫자 제거
                    .collect(Collectors.groupingBy(
                            Function.identity(),
                            Collectors.counting()
                    ));

            // 빈도 높은 상위 N개 반환
            List<String> keywords = tokenFrequency.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(MAX_KEYWORDS)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            log.debug("Extracted {} keywords from text (length: {})", keywords.size(), text.length());
            return keywords;

        } catch (Exception e) {
            log.error("Failed to extract keywords using Elasticsearch _analyze API", e);
            // Fallback: 빈 리스트 반환
            return Collections.emptyList();
        }
    }

    /**
     * 특정 analyzer를 지정하여 키워드 추출
     *
     * @param text 분석할 텍스트
     * @param analyzer analyzer 이름
     * @return 추출된 키워드 목록
     */
    public List<String> extractWithAnalyzer(String text, String analyzer) {
        if (text == null || text.isBlank()) {
            return Collections.emptyList();
        }

        // 토큰 수 제한을 피하기 위해 텍스트 길이 제한
        final String analyzableText = text.length() > MAX_TEXT_LENGTH
                ? text.substring(0, MAX_TEXT_LENGTH)
                : text;

        if (text.length() > MAX_TEXT_LENGTH) {
            log.debug("Text truncated to {} characters to avoid token limit", MAX_TEXT_LENGTH);
        }

        try {
            AnalyzeRequest request = AnalyzeRequest.of(a -> a
                    .analyzer(analyzer)
                    .text(analyzableText)
            );

            AnalyzeResponse response = elasticsearchClient.indices().analyze(request);

            Map<String, Long> tokenFrequency = response.tokens().stream()
                    .map(AnalyzeToken::token)
                    .filter(token -> token.length() >= MIN_TOKEN_LENGTH)
                    .filter(token -> !isStopword(token))  // 불용어 및 숫자 제거
                    .collect(Collectors.groupingBy(
                            Function.identity(),
                            Collectors.counting()
                    ));

            return tokenFrequency.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(MAX_KEYWORDS)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to extract keywords with analyzer: {}", analyzer, e);
            return Collections.emptyList();
        }
    }
}
