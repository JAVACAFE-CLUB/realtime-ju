package com.realtime.common.exception;

import com.realtime.common.dto.api.ApiResponse;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

@Slf4j
@RestControllerAdvice(basePackages = {"com.realtime.serving.presentation.rest"})
public class GlobalExceptionHandler {

    /**
     * 비즈니스 예외 처리
     */
    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ApiResponse<Void>> handleBusinessException(BusinessException e, WebRequest request) {
        log.warn("Business Exception: {} - {}", e.getErrorCode().name(), e.getMessage());

        ApiResponse<Void> response = ApiResponse.error(e.getErrorCode());

        return ResponseEntity.status(e.getErrorCode().getHttpStatus()).body(response);
    }

    /**
     * 요청 바디 유효성 검증 실패
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleMethodArgumentNotValidException(
            MethodArgumentNotValidException e, WebRequest request) {
        log.warn("Validation Exception: {}", e.getMessage());

        ApiResponse<Void> response = ApiResponse.error(ErrorCode.VALIDATION_FAILED);

        return ResponseEntity.status(ErrorCode.VALIDATION_FAILED.getHttpStatus()).body(response);
    }

    /**
     * 바인딩 예외 처리
     */
    @ExceptionHandler(BindException.class)
    public ResponseEntity<ApiResponse<Void>> handleBindException(BindException e, WebRequest request) {
        log.warn("Bind Exception: {}", e.getMessage());

        ApiResponse<Void> response = ApiResponse.error(ErrorCode.VALIDATION_FAILED);

        return ResponseEntity.status(ErrorCode.VALIDATION_FAILED.getHttpStatus()).body(response);
    }

    /**
     * 제약 조건 위반 예외 처리
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiResponse<Void>> handleConstraintViolationException(
            ConstraintViolationException e, WebRequest request) {
        log.warn("Constraint Violation Exception: {}", e.getMessage());

        ApiResponse<Void> response = ApiResponse.error(ErrorCode.INVALID_REQUEST);

        return ResponseEntity.status(ErrorCode.INVALID_REQUEST.getHttpStatus()).body(response);
    }

    /**
     * 일반 예외 처리
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleException(Exception e, WebRequest request) {
        log.error("Unexpected Exception: ", e);

        ApiResponse<Void> response = ApiResponse.error(ErrorCode.INTERNAL_SERVER_ERROR);

        return ResponseEntity.status(ErrorCode.INTERNAL_SERVER_ERROR.getHttpStatus()).body(response);
    }
}
