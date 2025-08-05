package com.juju.realtime.presentation.rest.keyword.dto;

import com.juju.realtime.domain.keyword.entity.TrendStatus;

public record KeywordCreateRequest(
        String keyword,
        Integer ranking,
        TrendStatus trendStatus
) {
}
