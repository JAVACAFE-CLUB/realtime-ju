package com.realtime.collector.application.sns.youtube.dto;

import java.util.List;
import lombok.Builder;

@Builder
public record YouTubeVideoListResponse(
        String kind,
        String etag,
        List<YouTubeVideo> items
) {
    @Builder
    public record YouTubeVideo(
            String kind,
            String etag,
            String id,
            Snippet snippet
    ) {
    }

    @Builder
    public record Snippet(
            String publishedAt,
            String channelId,
            String title,
            String description,
            Thumbnails thumbnails,
            String channelTitle,
            List<String> tags,
            String categoryId,
            String liveBroadcastContent,
            String defaultLanguage,
            Localized localized,
            String defaultAudioLanguage
    ) {
    }

    @Builder
    public record Thumbnails(
            Thumbnail defaultThumbnail,
            Thumbnail medium,
            Thumbnail high,
            Thumbnail standard,
            Thumbnail maxres
    ) {
    }

    @Builder
    public record Thumbnail(
            String url,
            Integer width,
            Integer height
    ) {
    }

    @Builder
    public record Localized(
            String title,
            String description
    ) {
    }
}
