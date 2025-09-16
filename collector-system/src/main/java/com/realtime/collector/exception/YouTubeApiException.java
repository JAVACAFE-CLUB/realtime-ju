package com.realtime.collector.exception;

public class YouTubeApiException extends RuntimeException {

    private final Integer statusCode;
    private final String reason;

    public YouTubeApiException(String message) {
        super(message);
        this.statusCode = null;
        this.reason = null;
    }

    public YouTubeApiException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = null;
        this.reason = null;
    }

    public YouTubeApiException(int statusCode, String statusText, String errorMessage, String reason) {
        super(buildMessage(statusCode, statusText, errorMessage, reason));
        this.statusCode = statusCode;
        this.reason = reason;
    }

    private static String buildMessage(int statusCode, String statusText, String errorMessage, String reason) {
        StringBuilder sb = new StringBuilder();
        sb.append("YouTube API 호출 실패 - HTTP ")
                .append(statusCode)
                .append(" ")
                .append(statusText == null ? "" : statusText)
                .append(": ");
        if (errorMessage != null && !errorMessage.isBlank()) {
            sb.append(errorMessage);
        }
        if (reason != null && !reason.isBlank()) {
            sb.append(" (reason=").append(reason).append(")");
        }
        return sb.toString();
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getReason() {
        return reason;
    }
}


