package com.realtime.common.kafka.message;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class RefineMessage extends ProcessingBaseMessage {

    private String refinedId;         // 정제 산출물 ID (예: Mongo ObjectId)
    private Integer producedCount;    // 생성된 레코드 수 (옵션)
    private String targetCollection;  // Mongo 등 타겟 컬렉션명 (옵션)
    private Map<String, String> metadata; // 옵션 메타데이터
}
