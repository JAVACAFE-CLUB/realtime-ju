package com.realtime.common.constants;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ContentSource {
    DOCS_WIKIPEDIA("DOCS_WIKIPEDIA"),
    NEWS_YNA("NEWS_YNA"),
    SNS_YOUTUBE("SNS_YOUTUBE");

    private final String code;
}