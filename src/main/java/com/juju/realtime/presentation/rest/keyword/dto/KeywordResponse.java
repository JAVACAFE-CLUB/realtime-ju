package com.juju.realtime.presentation.rest.keyword.dto;

import com.juju.realtime.domain.keyword.entity.Keyword;
import com.juju.realtime.domain.keyword.entity.TrendStatus;
import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record KeywordResponse(
        Long id,
        String keyword,
        Integer ranking,
        TrendStatus trendStatus,
        Long searchCount,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {

    public static KeywordResponse from(Keyword keyword) {
        return KeywordResponse.builder()
                .id(keyword.getId())
                .keyword(keyword.getKeyword())
                .ranking(keyword.getRanking())
                .trendStatus(keyword.getTrendStatus())
                .searchCount(keyword.getSearchCount())
                .createdAt(keyword.getCreatedAt())
                .updatedAt(keyword.getUpdatedAt())
                .build();
    }
}
