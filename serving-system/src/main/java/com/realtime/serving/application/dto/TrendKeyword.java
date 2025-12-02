package com.realtime.serving.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrendKeyword {

    private String keyword;
    private Long count;  // 등장 빈도
    private Double score;  // 가중치 점수
    private Integer ranking;  // 순위

    public static TrendKeyword of(String keyword, Long count, Double score, Integer ranking) {
        return TrendKeyword.builder()
                .keyword(keyword)
                .count(count)
                .score(score)
                .ranking(ranking)
                .build();
    }
}
