package com.realtime.serving;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.serving.domain.keyword.entity.TrendStatus;
import com.realtime.serving.presentation.rest.keyword.dto.KeywordResponse;
import java.util.List;

public class TestAssertions {

    // KeywordResponse 검증 헬퍼
    public static void assertKeywordResponse(KeywordResponse response,
                                             String expectedKeyword,
                                             Integer expectedRanking,
                                             TrendStatus expectedTrendStatus) {
        assertThat(response).isNotNull();
        assertThat(response.keyword()).isEqualTo(expectedKeyword);
        assertThat(response.ranking()).isEqualTo(expectedRanking);
        assertThat(response.trendStatus()).isEqualTo(expectedTrendStatus);
    }

    // KeywordResponse 리스트 검증 헬퍼
    public static void assertKeywordResponseList(List<KeywordResponse> responses, int expectedSize) {
        assertThat(responses).hasSize(expectedSize);
    }

    // 첫 번째 키워드 검증 헬퍼
    public static void assertFirstKeyword(List<KeywordResponse> responses,
                                          String expectedKeyword,
                                          Integer expectedRanking,
                                          TrendStatus expectedTrendStatus) {
        assertThat(responses).isNotEmpty();
        assertKeywordResponse(responses.get(0), expectedKeyword, expectedRanking, expectedTrendStatus);
    }

    // 키워드 순서 검증 헬퍼
    public static void assertKeywordOrder(List<KeywordResponse> responses,
                                          String... expectedKeywords) {
        assertThat(responses).hasSize(expectedKeywords.length);
        for (int i = 0; i < expectedKeywords.length; i++) {
            assertThat(responses.get(i).keyword()).isEqualTo(expectedKeywords[i]);
        }
    }
} 