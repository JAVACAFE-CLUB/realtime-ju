package com.realtime.collector.application.news.yna.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ManifestItem {

    private String articleId;
    private String guid;
    private String link;
    private String finalUrl;
    private String title;
    private String author;
    private String pubDate;
    private String description;
    private List<String> images;
    private CrawlInfo crawl;
}
