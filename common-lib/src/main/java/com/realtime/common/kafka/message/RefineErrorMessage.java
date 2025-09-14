package com.realtime.common.kafka.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class RefineErrorMessage extends ProcessingBaseMessage {

    private String inputRawUri;      // 입력 원본 URI(옵션)
    private String errorCode;
    private String errorMessage;
    private boolean retriable;
}
