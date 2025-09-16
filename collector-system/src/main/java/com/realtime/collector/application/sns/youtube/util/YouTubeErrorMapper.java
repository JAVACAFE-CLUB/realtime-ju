package com.realtime.collector.application.sns.youtube.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.realtime.collector.application.sns.youtube.dto.YouTubeApiErrorResponse;
import com.realtime.collector.exception.YouTubeApiException;

public final class YouTubeErrorMapper {

    private YouTubeErrorMapper() {
    }

    public static YouTubeApiException toException(ObjectMapper objectMapper, int statusCode, String statusText,
                                                  String body) {
        try {
            YouTubeApiErrorResponse error = objectMapper.readValue(body, YouTubeApiErrorResponse.class);
            String message = error.getError() != null ? error.getError().getMessage() : body;
            String reason =
                    (error.getError() != null && error.getError().getErrors() != null && !error.getError().getErrors()
                            .isEmpty())
                            ? error.getError().getErrors().getFirst().getReason()
                            : null;
            return new YouTubeApiException(statusCode, statusText, message, reason);
        } catch (Exception parseEx) {
            return new YouTubeApiException(
                    String.format("YouTube API 호출 실패 - HTTP %d: %s", statusCode, body),
                    parseEx);
        }
    }
}


