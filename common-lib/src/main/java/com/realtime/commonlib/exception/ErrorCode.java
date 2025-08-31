package com.realtime.commonlib.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    // 검색어 관련 에러
    KEYWORD_NOT_FOUND("KW001", "검색어를 찾을 수 없습니다", HttpStatus.NOT_FOUND),
    DUPLICATE_RANKING("KW002", "이미 존재하는 랭킹입니다", HttpStatus.CONFLICT),
    INVALID_RANKING("KW003", "유효하지 않은 랭킹입니다 (1-100 범위)", HttpStatus.BAD_REQUEST),
    KEYWORD_ALREADY_EXISTS("KW004", "이미 존재하는 검색어입니다", HttpStatus.CONFLICT),

    // 검증 관련 에러
    VALIDATION_FAILED("VA001", "입력값 검증에 실패했습니다", HttpStatus.BAD_REQUEST),
    INVALID_REQUEST("VA002", "잘못된 요청입니다", HttpStatus.BAD_REQUEST),

    // 시스템 에러
    INTERNAL_SERVER_ERROR("SY001", "서버 내부 오류가 발생했습니다", HttpStatus.INTERNAL_SERVER_ERROR);

    private final String code;
    private final String message;
    private final HttpStatus httpStatus;
}
