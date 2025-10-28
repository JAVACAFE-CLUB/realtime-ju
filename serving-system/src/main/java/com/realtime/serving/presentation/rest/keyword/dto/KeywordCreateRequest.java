package com.realtime.serving.presentation.rest.keyword.dto;

import com.realtime.serving.domain.keyword.entity.TrendStatus;

public record KeywordCreateRequest(
        String keyword,
        Integer ranking,
        TrendStatus trendStatus
) {
}
