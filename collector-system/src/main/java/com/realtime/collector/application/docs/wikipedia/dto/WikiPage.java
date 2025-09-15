package com.realtime.collector.application.docs.wikipedia.dto;

import lombok.Data;

@Data
public class WikiPage {
    private String pageId;
    private String title;
    private Integer ns;
    private String redirectTitle;
    private String revisionId;
    private String timestamp;
    private String contributor;
    private String text;
}
