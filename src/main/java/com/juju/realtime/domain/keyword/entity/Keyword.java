package com.juju.realtime.domain.keyword.entity;

import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Keyword {

    @Builder.Default
    private Long id = null;
    private String keyword;
    private Integer ranking;
    private TrendStatus trendStatus;
    private Long searchCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static Keyword create(String keyword, Integer ranking, TrendStatus trendStatus) {
        LocalDateTime now = LocalDateTime.now();
        return Keyword.builder()
                .keyword(keyword)
                .ranking(ranking)
                .trendStatus(trendStatus)
                .searchCount(0L)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
