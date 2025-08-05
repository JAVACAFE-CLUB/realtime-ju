package com.juju.realtime.domain.keyword.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum TrendStatus {
    UP("상승", "▲"),
    DOWN("하락", "▼"),
    NEW("신규", "+"),
    MAINTAIN("유지", "-");

    private final String description;
    private final String symbol;
}
