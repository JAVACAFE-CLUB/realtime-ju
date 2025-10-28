package com.realtime.common.kafka.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class CollectErrorMessage extends ProcessingBaseMessage {

    private String rawDataUrl;        // 원본 URI (없을 수 있음)
    private String errorCode;         // YT_API_ERROR / MINIO_ERROR / DB_ERROR / KAFKA_ERROR / VALIDATION_ERROR
    private String errorMessage;      // 요약 메시지
    private boolean retriable;        // 재시도 가능 여부
}
