package com.realtime.refine.domain.sns.youtube;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class RefinedSnsYouTube {

    private String id; // refinedId (문서 ID)

    private String contentId; // videoId

    private String source;
    private String rawUri;

    // 컨텐츠
    private String title;
    private String description;
    private String tags; // 쉼표 구분 문자열

    // 메타데이터
    private String channelId;
    private String channelTitle;
    private String categoryId;
    private String publishedAt; // YouTube API의 publishedAt (ISO 8601)

    // 썸네일
    private String thumbnailUrl; // 대표 썸네일 URL

    // 처리 정보
    private String language;
    private String charset;
    private Integer contentLength; // title + description 합산
    private String checksum;

    private Instant refinedAt;
    private Integer processingTimeMs;
    private String schemaVersion;

    private String processingStatus; // PENDING/PROCESSING/COMPLETED/FAILED
    private Instant indexedAt;
}
