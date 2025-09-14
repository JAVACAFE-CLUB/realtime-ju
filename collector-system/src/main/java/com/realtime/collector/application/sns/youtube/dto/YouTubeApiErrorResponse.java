package com.realtime.collector.application.sns.youtube.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class YouTubeApiErrorResponse {

    private Error error;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Error {
        private int code;
        private String message;
        private List<ErrorDetail> errors;
        private String status;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDetail {
        private String message;
        private String domain;
        private String reason;
    }
}
