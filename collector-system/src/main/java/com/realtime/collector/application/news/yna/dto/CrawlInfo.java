package com.realtime.collector.application.news.yna.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CrawlInfo {

    private int status;
    private String contentType;
    private String charset;
    private String crawledAt;
    private String htmlObjectKey;
}
