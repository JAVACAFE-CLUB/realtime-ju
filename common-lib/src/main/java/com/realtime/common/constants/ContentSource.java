package com.realtime.common.constants;

public enum ContentSource {
    DOCS_WIKIPEDIA("DOCS_WIKIPEDIA"),
    NEWS_YNA("NEWS_YNA"),
    SNS_YOUTUBE("SNS_YOUTUBE");

    private final String code;

    ContentSource(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}