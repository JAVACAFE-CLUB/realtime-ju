package com.realtime.serving.domain.ranking;

import com.realtime.common.constants.ContentSource;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

/**
 * Elasticsearch에서 집계된 원시 키워드 데이터
 */
@Getter
@Builder
public class RawKeyword {
    private String keyword;
    private long docCount;
    private ContentSource source;
    private LocalDateTime indexedAt;
    private String documentUrl;     // 대표 문서 링크
    private String documentTitle;   // 대표 문서 제목
}
