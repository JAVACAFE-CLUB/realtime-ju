package com.realtime.refine.domain.news.yna;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class RefinedNewsYna {

    private String id; // refinedId (문서 ID)

    private String contentId;

    private String source;
    private String rawUri;

    private String title;
    private String content;

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



