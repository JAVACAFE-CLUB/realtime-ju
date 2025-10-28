package com.realtime.collector.exception;

/**
 * 재시도 가능한 예외를 나타내는 마커 예외
 */
public class RetriableException extends RuntimeException {

    public RetriableException(String message) {
        super(message);
    }

    public RetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}