package com.realtime.collector.application.news.yna.dto;

import java.util.List;

public record RssItem(
        String articleId,
        String guid,
        String link,
        String title,
        String author,
        String pubDate,
        String description,
        List<String> images
) {
}
