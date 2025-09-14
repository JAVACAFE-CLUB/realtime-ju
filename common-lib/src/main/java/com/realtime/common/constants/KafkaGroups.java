package com.realtime.common.constants;

public final class KafkaGroups {

    // ========================================
    // 🔥 System Consumer Groups
    // ========================================
    public static final String COLLECTOR_GROUP = "collector-system-group";
    public static final String REFINE_GROUP = "refine-system-group";
    public static final String INDEX_GROUP = "index-system-group";
    public static final String SERVING_GROUP = "serving-system-group";

    // ========================================
    // 🔥 Functional Consumer Groups
    // ========================================
    public static final String RAW_DATA_PROCESSOR_GROUP = "raw-data-processor";
    public static final String REFINED_DATA_PROCESSOR_GROUP = "refined-data-processor";
    public static final String KEYWORD_ANALYZER_GROUP = "keyword-analyzer";
    public static final String TREND_ANALYZER_GROUP = "trend-analyzer";
    public static final String SEARCH_INDEXER_GROUP = "search-indexer";

    // ========================================
    // 🔥 Error Handling Groups
    // ========================================
    public static final String RETRY_PROCESSOR_GROUP = "retry-processor";
    public static final String DLQ_PROCESSOR_GROUP = "dlq-processor";

    // ========================================
    // 🔥 Real-time Groups
    // ========================================
    public static final String REALTIME_TRENDING_GROUP = "realtime-trending";
    public static final String REALTIME_SEARCH_GROUP = "realtime-search";

    private KafkaGroups() {
        // 유틸리티 클래스 - 인스턴스화 방지
    }
}