package com.realtime.refine.infrastructure.messaging;

import com.realtime.common.kafka.message.RefineErrorMessage;
import com.realtime.common.kafka.message.RefineMessage;
import com.realtime.common.kafka.producer.KafkaMessageProducer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RefineEventPublisher {

    private final KafkaMessageProducer producer;

    public CompletableFuture<SendResult<String, Object>> publishRefined(
            String topic,
            String key,
            String refinedId,
            String collectionId,
            String source
    ) {
        // [메시지 구성] 표준 정제 완료 메시지 생성
        RefineMessage message = RefineMessage.builder()
                .schemaVersion("1")
                .occurredAt(Instant.now())
                .collectionId(collectionId)
                .source(source)
                .refinedId(refinedId)
                .build();

        // [발송] 키 포함 비동기 발송 및 로깅
        log.debug("🟢 Publish refined: topic={}, key={}, refinedId={}", topic, key, refinedId);
        return producer.sendMessage(topic, key, message);
    }

    public CompletableFuture<SendResult<String, Object>> publishRefineError(
            String topic,
            String key,
            String collectionId,
            String source,
            String inputRawUri,
            String errorCode,
            String errorMessage,
            boolean retriable
    ) {
        // [메시지 구성] 표준 에러 메시지 생성
        RefineErrorMessage message = RefineErrorMessage.builder()
                .schemaVersion("1")
                .occurredAt(Instant.now())
                .collectionId(collectionId)
                .source(source)
                .inputRawUri(inputRawUri)
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .retriable(retriable)
                .build();

        // [발송] 에러 메시지 비동기 발송 및 경고 로깅
        log.warn("🟠 Publish refine error: topic={}, key={}, code={}", topic, key, errorCode);
        return producer.sendMessage(topic, key, message);
    }
}



