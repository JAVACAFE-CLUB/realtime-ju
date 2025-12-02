package com.realtime.collector.application.sns.youtube.dto;

import com.realtime.collector.application.sns.youtube.dto.YouTubeVideoListResponse.YouTubeVideo;
import lombok.Builder;

@Builder
public record VideoRecord(
        YouTubeVideo video,
        int status,
        String errorMessage,
        String jsonObjectKey
) {
    public static VideoRecord success(YouTubeVideo video, String objectKey) {
        return new VideoRecord(video, 200, null, objectKey);
    }

    public static VideoRecord failed(YouTubeVideo video, int status, String errorMessage) {
        return new VideoRecord(video, status, errorMessage, null);
    }
}
