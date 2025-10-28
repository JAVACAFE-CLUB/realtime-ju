package com.realtime.common.kafka.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class CollectMessage extends ProcessingBaseMessage {

    private String rawDataUrl;        // 원본 저장소 URI (예: minio://bucket/path)
    private Integer recordCount;      // 성공 저장 건수
}
