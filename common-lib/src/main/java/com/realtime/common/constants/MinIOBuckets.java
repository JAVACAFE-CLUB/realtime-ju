package com.realtime.common.constants;

public final class MinIOBuckets {

    // ========================================
    //  원본 데이터 저장소
    // ========================================
    public static final String RAW_DOCS_WIKIPEDIA = "raw-docs-wikipedia";
    public static final String RAW_NEWS_YNA = "raw-news-yna";
    public static final String RAW_SNS_YOUTUBE = "raw-sns-youtube";


    private MinIOBuckets() {
        // 유틸리티 클래스 - 인스턴스화 방지
    }

    public static String[] getAllBuckets() {
        return new String[]{
                RAW_DOCS_WIKIPEDIA, RAW_NEWS_YNA, RAW_SNS_YOUTUBE,
        };
    }
}
