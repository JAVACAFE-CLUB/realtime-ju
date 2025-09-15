package com.realtime.common.constants;

public final class KafkaTopics {

    // ========================================
    // 🔥 Raw Data Topics (원본 데이터)
    // ========================================
    public static final String RAW_DOCS_WIKIPEDIA = "raw.docs.wikipedia";
    public static final String RAW_NEWS_NAVER = "raw.news.naver";
    public static final String RAW_NEWS_YNA = "raw.news.yna";   // 연합뉴스
    public static final String RAW_SNS_YOUTUBE = "raw.sns.youtube";

    // ========================================
    // 🔥 Refined Data Topics (정제된 데이터)
    // ========================================
    public static final String REFINED_DOCS_WIKIPEDIA = "refined.docs.wikipedia";
    public static final String REFINED_NEWS_NAVER = "refined.news.naver";
    public static final String REFINED_NEWS_YNA = "refined.news.yna";
    public static final String REFINED_SNS_YOUTUBE = "refined.sns.youtube";

    // ========================================
    // 🔥 Error Handling Topics
    // ========================================
    private static final String RETRY = ".retry";
    private static final String DLQ = ".dlq";

    public static final String RAW_DOCS_WIKIPEDIA_RETRY = RAW_DOCS_WIKIPEDIA + RETRY;
    public static final String RAW_DOCS_WIKIPEDIA_DLQ = RAW_DOCS_WIKIPEDIA + DLQ;
    public static final String RAW_NEWS_NAVER_RETRY = RAW_NEWS_NAVER + RETRY;
    public static final String RAW_NEWS_NAVER_DLQ = RAW_NEWS_NAVER + DLQ;
    public static final String RAW_NEWS_YNA_RETRY = RAW_NEWS_YNA + RETRY;
    public static final String RAW_NEWS_YNA_DLQ = RAW_NEWS_YNA + DLQ;
    public static final String RAW_SNS_YOUTUBE_RETRY = RAW_SNS_YOUTUBE + RETRY;
    public static final String RAW_SNS_YOUTUBE_DLQ = RAW_SNS_YOUTUBE + DLQ;

    private KafkaTopics() {
    }

    public static String[] getAllTopics() {
        return new String[]{
                RAW_DOCS_WIKIPEDIA, RAW_NEWS_NAVER, RAW_NEWS_YNA, RAW_SNS_YOUTUBE,
                REFINED_DOCS_WIKIPEDIA, REFINED_NEWS_NAVER, REFINED_NEWS_YNA, REFINED_SNS_YOUTUBE,
                RAW_DOCS_WIKIPEDIA_RETRY, RAW_DOCS_WIKIPEDIA_DLQ,
                RAW_NEWS_NAVER_RETRY, RAW_NEWS_NAVER_DLQ,
                RAW_NEWS_YNA_RETRY, RAW_NEWS_YNA_DLQ,
                RAW_SNS_YOUTUBE_RETRY, RAW_SNS_YOUTUBE_DLQ,
        };
    }
}