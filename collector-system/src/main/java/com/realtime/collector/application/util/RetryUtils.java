package com.realtime.collector.application.util;

import com.realtime.collector.exception.YouTubeApiException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

public final class RetryUtils {

    private static final Pattern HTTP_STATUS_PATTERN = Pattern.compile("HTTP\\s+(\\d{3})\\s+");

    private RetryUtils() {
    }

    public static boolean isRetriable(Throwable throwable) {
        if (throwable instanceof YouTubeApiException ytEx && ytEx.getStatusCode() != null) {
            int statusCode = ytEx.getStatusCode();
            return isRetriableStatus(statusCode);
        }
        if (throwable instanceof WebClientResponseException responseException) {
            int statusCode = responseException.getStatusCode().value();
            return isRetriableStatus(statusCode);
        }
        if (throwable instanceof WebClientRequestException) {
            return true;
        }
        Throwable cause = throwable.getCause();
        if (cause instanceof WebClientResponseException responseException) {
            int statusCode = responseException.getStatusCode().value();
            return isRetriableStatus(statusCode);
        }
        String message = throwable.getMessage();
        if (message != null) {
            Matcher matcher = HTTP_STATUS_PATTERN.matcher(message);
            if (matcher.find()) {
                int statusCode = Integer.parseInt(matcher.group(1));
                return isRetriableStatus(statusCode);
            }
        }
        return true;
    }

    public static boolean isRetriableStatus(int statusCode) {
        return statusCode == 429 || statusCode >= 500;
    }

    public static void sleepWithBackoff(long baseBackoffMs, int attempt) {
        try {
            long backoff = (long) (baseBackoffMs * Math.pow(2, attempt - 1));
            long jitter = ThreadLocalRandom.current().nextLong(50, 150);
            Thread.sleep(backoff + jitter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("백오프 대기 중 인터럽트 발생", e);
        }
    }
}
