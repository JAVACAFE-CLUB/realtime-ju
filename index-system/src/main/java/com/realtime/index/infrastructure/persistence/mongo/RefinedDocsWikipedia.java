package com.realtime.index.infrastructure.persistence.mongo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document(collection = "refinedDocsWikipedia")
public class RefinedDocsWikipedia {

    @Id
    private String id;  // refinedId

    private String pageId;
    private String contentId;
    private String source;
    private String rawUri;

    private String title;
    private String content;

    // Wikipedia 메타데이터
    private Integer ns;
    private String redirectTitle;
    private String revisionId;
    private String timestamp;
    private String contributor;

    private String language;
    private String charset;
    private Integer contentLength;
    private String checksum;

    private Instant refinedAt;
    private Integer processingTimeMs;
    private String schemaVersion;

    private String processingStatus;
    private Instant indexedAt;
}
