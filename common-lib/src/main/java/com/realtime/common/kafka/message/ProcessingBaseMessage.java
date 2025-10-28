package com.realtime.common.kafka.message;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * 단계별 메시지들의 공통 베이스
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public abstract class ProcessingBaseMessage {

    private String schemaVersion;
    private String collectionId;
    private String source;
    private Instant occurredAt;
}
