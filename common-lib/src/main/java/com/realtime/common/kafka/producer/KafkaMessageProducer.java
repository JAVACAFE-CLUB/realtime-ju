package com.realtime.common.kafka.producer;

import com.realtime.common.kafka.message.ProcessingBaseMessage;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * Kafka 메시지 프로듀서
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 키 지정 메시지 발송 (비동기)
     */
    public CompletableFuture<SendResult<String, Object>> sendMessage(String topic, String key, Object message) {
        log.debug("🚀 Sending message to topic: {}, key: {}", topic, key);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, message);

        // 성공/실패 콜백 등록
        future.whenComplete((result, throwable) -> {
            if (throwable == null) {
                handleSendSuccess(topic, key, message, result);
            } else {
                handleSendFailure(topic, key, message, throwable);
            }
        });

        return future;
    }

    /**
     * 헤더 포함 메시지 발송 - 커스텀 헤더를 병합하고, 도메인 표준 헤더를 추가합니다. - ProcessingBaseMessage.occuredAt 존재 시 KafkaHeaders.TIMESTAMP로
     * 매핑합니다.
     */
    public CompletableFuture<SendResult<String, Object>> sendMessageWithHeaders(
            String topic, String key, Object payload, java.util.Map<String, Object> headers) {

        log.debug("🚀 Sending message with headers to topic: {}, key: {}, headers: {}", topic, key, headers.size());

        MessageBuilder<Object> messageBuilder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key);

        // 커스텀 헤더 추가
        headers.forEach(messageBuilder::setHeader);

        // 표준 도메인 헤더 추가 (ProcessingBaseMessage 기반)
        if (payload instanceof ProcessingBaseMessage base) {
            if (base.getSchemaVersion() != null) {
                messageBuilder.setHeader("schema-version", base.getSchemaVersion());
            }
            if (base.getCollectionId() != null) {
                messageBuilder.setHeader("collection-id", base.getCollectionId());
            }
            if (base.getSource() != null) {
                messageBuilder.setHeader("source", base.getSource());
            }
            if (base.getOccurredAt() != null) {
                messageBuilder.setHeader("occurred-at", base.getOccurredAt().toString());
                messageBuilder.setHeader(KafkaHeaders.TIMESTAMP, base.getOccurredAt().toEpochMilli());
            }
        }

        Message<Object> message = messageBuilder.build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(message);

        future.whenComplete((result, throwable) -> {
            if (throwable == null) {
                handleSendSuccess(topic, key, payload, result);
            } else {
                handleSendFailure(topic, key, payload, throwable);
            }
        });

        return future;
    }


    /**
     * 전송 성공 처리
     */
    private void handleSendSuccess(String topic, String key, Object message, SendResult<String, Object> result) {
        long offset = result.getRecordMetadata().offset();
        int partition = result.getRecordMetadata().partition();

        log.debug("✅ Message sent successfully - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                topic, partition, offset, key);
    }

    /**
     * 전송 실패 처리
     */
    private void handleSendFailure(String topic, String key, Object message, Throwable throwable) {
        log.error("❌ Failed to send message - Topic: {}, Key: {}", topic, key, throwable);
    }

    /**
     * DLQ 메시지 발송
     */
    public CompletableFuture<SendResult<String, Object>> sendDlqMessage(String dlqTopic, ProcessingBaseMessage message,
                                                                        String reason) {
        log.error("☠️ Sending message to DLQ - Topic: {}, Key: {}, Reason: {}",
                dlqTopic, message.getCollectionId(), reason);

        return sendMessageWithHeaders(dlqTopic, message.getCollectionId(), message,
                Map.of("dlq.reason", reason));
    }

    /**
     * 플러시 (모든 대기 중인 메시지 전송)
     */
    public void flush() {
        log.debug("🔄 Flushing all pending messages");
        kafkaTemplate.flush();
    }
}
