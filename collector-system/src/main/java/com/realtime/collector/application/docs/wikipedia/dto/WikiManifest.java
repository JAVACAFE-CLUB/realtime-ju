package com.realtime.collector.application.docs.wikipedia.dto;

import java.util.List;
import lombok.Data;

@Data
public class WikiManifest {
    private String collectionId;
    private String lang;
    private String dumpDate;
    private int pagesTotal;
    private int shardsTotal;
    private long bytesTotal;
    private List<String> shards;
    private String schemaVersion;
    private String createdAt;
}
