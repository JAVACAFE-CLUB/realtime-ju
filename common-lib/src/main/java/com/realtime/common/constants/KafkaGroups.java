package com.realtime.common.constants;

public final class KafkaGroups {

    // ========================================
    // System Consumer Groups
    // ========================================
    public static final String COLLECTOR_GROUP = "collector-system-group";
    public static final String REFINE_GROUP = "refine-system-group";
    public static final String INDEX_GROUP = "index-system-group";

    
    private KafkaGroups() {
        // 유틸리티 클래스 - 인스턴스화 방지
    }
}
