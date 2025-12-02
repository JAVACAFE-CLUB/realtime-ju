package com.realtime.serving.application.dto;

import com.realtime.serving.domain.ranking.RankedKeyword;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * 키워드 랭킹 응답 DTO
 */
@Getter
@Builder
public class KeywordRankingResponse {
    private List<RankedKeyword> keywords;
    private int totalCount;
    private Long lastUpdatedAt;

    public static KeywordRankingResponse of(List<RankedKeyword> keywords, Long lastUpdatedAt) {
        return KeywordRankingResponse.builder()
                .keywords(keywords)
                .totalCount(keywords.size())
                .lastUpdatedAt(lastUpdatedAt)
                .build();
    }
}
