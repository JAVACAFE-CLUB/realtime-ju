package com.realtime.common.constants;

public final class MinIOBuckets {

    // ========================================
    // 🔥 원본 데이터 저장소 (Bronze Layer)
    // ========================================
    public static final String RAW_DOCS_WIKIPEDIA = "raw-docs-wikipedia";
    public static final String RAW_NEWS_NAVER = "raw-news-naver";
    public static final String RAW_NEWS_YNA = "raw-news-yna";
    public static final String RAW_SNS_YOUTUBE = "raw-sns-youtube";

    // ========================================
    // 🔥 임시 저장소
    // ========================================
    public static final String TEMP_PROCESSING = "temp-processing";
    public static final String TEMP_UPLOADS = "temp-uploads";

    // ========================================
    // 🔥 백업 저장소
    // ========================================
    public static final String BACKUP_DATA = "backup-data";
    public static final String ARCHIVE_DATA = "archive-data";

    private MinIOBuckets() {
    }

    public static String[] getAllBuckets() {
        return new String[]{
                RAW_DOCS_WIKIPEDIA, RAW_NEWS_NAVER, RAW_NEWS_YNA, RAW_SNS_YOUTUBE,
                TEMP_PROCESSING, TEMP_UPLOADS,
                BACKUP_DATA, ARCHIVE_DATA
        };
    }
}