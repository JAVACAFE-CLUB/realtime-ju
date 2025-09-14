package com.realtime.common.exception;

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
    INTERNAL_SERVER_ERROR("SY001", "서버 내부 오류가 발생했습니다", HttpStatus.INTERNAL_SERVER_ERROR),

    // 외부 연동/메시징 에러
    EXTERNAL_API_ERROR("EX001", "외부 API 호출 실패", HttpStatus.BAD_GATEWAY),
    KAFKA_ERROR("KF001", "카프카 처리 실패", HttpStatus.SERVICE_UNAVAILABLE),

    // 저장소/데이터베이스 에러 (공통)
    STORAGE_ERROR("ST001", "스토리지 작업에 실패했습니다", HttpStatus.INTERNAL_SERVER_ERROR),
    DATABASE_ERROR("DB001", "데이터베이스 작업에 실패했습니다", HttpStatus.INTERNAL_SERVER_ERROR);

    private final String code;
    private final String message;
    private final HttpStatus httpStatus;
}
