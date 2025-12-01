package com.realtime.refine.domain.docs.wikipedia;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class RefinedDocsWikipedia {

    private String id; // refinedId (문서 ID)

    private String pageId; // Wikipedia 페이지 ID
    private String contentId; // collectionId (샤드 식별자)

    private String source;
    private String rawUri;

    private String title;
    private String content; // Wikitext를 변환한 일반 텍스트

    // Wikipedia 메타데이터
    private Integer ns; // 네임스페이스 번호 (문서=0)
    private String redirectTitle; // 리다이렉트 대상 제목
    private String revisionId; // 최신 리비전 ID
    private String timestamp; // 최신 리비전 타임스탬프
    private String contributor; // 기여자

    private String language;
    private String charset;
    private Integer contentLength;
    private String checksum;

    private Instant refinedAt;
    private Integer processingTimeMs;
    private String schemaVersion;

    private String processingStatus; // PENDING/PROCESSING/COMPLETED/FAILED
    private Instant indexedAt;
}

