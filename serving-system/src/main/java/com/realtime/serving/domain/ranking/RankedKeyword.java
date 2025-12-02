package com.realtime.serving.domain.ranking;

import lombok.Builder;
import lombok.Getter;

/**
 * 가중치가 적용된 최종 랭킹 키워드
 */
@Getter
@Builder
public class RankedKeyword {
    private String keyword;
    private double score;
    private int rank;

    public static RankedKeyword of(String keyword, double score, int rank) {
        return RankedKeyword.builder()
                .keyword(keyword)
                .score(score)
                .rank(rank)
                .build();
    }
}
