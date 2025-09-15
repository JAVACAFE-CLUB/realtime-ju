package com.realtime.collector.application.news.yna.dto;

public record ArticleRecord(RssItem item, int crawlStatus, String contentType, String charset, String htmlObjectKey,
                            String errorMessage) {

    public static ArticleRecord success(RssItem item, int status, String contentType, String charset, String htmlKey) {
        return new ArticleRecord(item, status, contentType, charset, htmlKey, null);
    }

    public static ArticleRecord failed(RssItem item, int status, String error) {
        return new ArticleRecord(item, status, null, null, null, error);
    }
}
