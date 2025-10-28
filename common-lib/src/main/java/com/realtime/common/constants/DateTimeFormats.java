package com.realtime.common.constants;

import java.time.format.DateTimeFormatter;

/**
 * 시스템 전역에서 사용되는 날짜/시간 포맷 상수
 */
public final class DateTimeFormats {

    // ========================================
    // 날짜/시간 포맷
    // ========================================
    
    /**
     * 덤프/배치 파일명에 사용되는 날짜 형식 (예: 20250101)
     */
    public static final DateTimeFormatter DUMP_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 저장소 경로(MinIO 등)에 사용되는 날짜 형식 (예: 2025/01/01)
     */
    public static final DateTimeFormatter STORAGE_PATH_DATE = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    /**
     * ISO 8601 확장 형식 (예: 2025-01-01T12:00:00)
     */
    public static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;


    private DateTimeFormats() {
        // 유틸리티 클래스 - 인스턴스화 방지
    }
}

