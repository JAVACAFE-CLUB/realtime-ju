package com.realtime.index.infrastructure.persistence.mongo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document(collection = "refinedSnsYouTube")
public class RefinedSnsYouTube {

    @Id
    private String id;  // refinedId

    private String contentId;  // videoId
    private String source;
    private String rawUri;

    private String title;
    private String description;
    private String tags;

    private String channelId;
    private String channelTitle;
    private String categoryId;
    private String publishedAt;

    private String thumbnailUrl;

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
