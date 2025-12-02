package com.realtime.common.kafka.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class RefineMessage extends ProcessingBaseMessage {

    private String refinedId;         // 정제 산출물 ID
}
