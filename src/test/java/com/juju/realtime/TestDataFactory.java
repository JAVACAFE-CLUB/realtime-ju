package com.juju.realtime;

import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.entity.TrendStatus;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordCreateRequest;
import com.juju.realtime.presentation.rest.keyword.dto.KeywordResponse;

import java.time.LocalDateTime;
import java.util.List;

public class TestDataFactory {

    // 기본 테스트 데이터 상수
    public static final String KIMCHI_JJIGAE = "김치찌개";
    public static final String DOENJANG_JJIGAE = "된장찌개";
    public static final String SUNDUBU_JJIGAE = "순두부찌개";
    public static final String BUDAE_JJIGAE = "부대찌개";
    public static final String DAKBOKKEUMTANG = "닭볶음탕";
    public static final String NEW_FOOD = "새로운 음식";

    // Keyword 엔티티 생성
    public static Keyword createKeyword(String keyword, Integer ranking, TrendStatus trendStatus) {
        return createKeyword(1L, keyword, ranking, trendStatus, 1000L);
    }

    public static Keyword createKeyword(Long id, String keyword, Integer ranking, TrendStatus trendStatus, Long searchCount) {
        return Keyword.builder()
                .id(id)
                .keyword(keyword)
                .ranking(ranking)
                .trendStatus(trendStatus)
                .searchCount(searchCount)
                .build();
    }

    // KeywordCreateRequest 생성
    public static KeywordCreateRequest createKeywordRequest(String keyword, Integer ranking, TrendStatus trendStatus) {
        return new KeywordCreateRequest(keyword, ranking, trendStatus);
    }

    // KeywordResponse 생성
    public static KeywordResponse createKeywordResponse(Long id, String keyword, Integer ranking, TrendStatus trendStatus) {
        return KeywordResponse.builder()
                .id(id)
                .keyword(keyword)
                .ranking(ranking)
                .trendStatus(trendStatus)
                .searchCount(1000L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    // 기본 TOP 3 키워드 리스트 생성
    public static List<Keyword> createTop3Keywords() {
        return List.of(
                createKeyword(1L, KIMCHI_JJIGAE, 1, TrendStatus.UP, 1000L),
                createKeyword(2L, DOENJANG_JJIGAE, 2, TrendStatus.MAINTAIN, 800L),
                createKeyword(3L, SUNDUBU_JJIGAE, 3, TrendStatus.DOWN, 700L)
        );
    }

    // 기본 TOP 5 키워드 리스트 생성
    public static List<Keyword> createTop5Keywords() {
        return List.of(
                createKeyword(1L, KIMCHI_JJIGAE, 1, TrendStatus.UP, 1000L),
                createKeyword(2L, DOENJANG_JJIGAE, 2, TrendStatus.MAINTAIN, 800L),
                createKeyword(3L, SUNDUBU_JJIGAE, 3, TrendStatus.DOWN, 700L),
                createKeyword(4L, BUDAE_JJIGAE, 4, TrendStatus.UP, 600L),
                createKeyword(5L, DAKBOKKEUMTANG, 5, TrendStatus.NEW, 500L)
        );
    }

    // 기본 TOP 3 키워드 응답 리스트 생성
    public static List<KeywordResponse> createTop3KeywordResponses() {
        return List.of(
                createKeywordResponse(1L, KIMCHI_JJIGAE, 1, TrendStatus.UP),
                createKeywordResponse(2L, DOENJANG_JJIGAE, 2, TrendStatus.MAINTAIN),
                createKeywordResponse(3L, SUNDUBU_JJIGAE, 3, TrendStatus.DOWN)
        );
    }

    // 기본 샘플 키워드 생성
    public static Keyword createSampleKeyword() {
        return createKeyword(1L, KIMCHI_JJIGAE, 1, TrendStatus.UP, 1000L);
    }

    // 기본 샘플 요청 생성
    public static KeywordCreateRequest createSampleRequest() {
        return createKeywordRequest(NEW_FOOD, 5, TrendStatus.NEW);
    }

    // 기본 샘플 응답 생성
    public static KeywordResponse createSampleResponse() {
        return createKeywordResponse(1L, KIMCHI_JJIGAE, 1, TrendStatus.UP);
    }
} 