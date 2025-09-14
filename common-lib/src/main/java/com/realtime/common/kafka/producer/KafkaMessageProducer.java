package com.realtime.common.kafka.producer;

import com.realtime.common.kafka.message.ProcessingBaseMessage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
     * 기본 메시지 발송 (비동기)
     */
    public CompletableFuture<SendResult<String, Object>> sendMessage(String topic, ProcessingBaseMessage message) {
        String key = message.getCollectionId();
        return sendMessageWithHeaders(topic, key, message, Map.of());
    }

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
     * 헤더 포함 메시지 발송
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
     * 동기 메시지 발송 (타임아웃 포함)
     */
    public SendResult<String, Object> sendMessageSync(String topic, String key, Object message, long timeoutSeconds) {
        try {
            log.debug("🚀 Sending message synchronously to topic: {}, key: {}", topic, key);

            CompletableFuture<SendResult<String, Object>> future = sendMessage(topic, key, message);
            SendResult<String, Object> result = future.get(timeoutSeconds, TimeUnit.SECONDS);

            log.debug("✅ Message sent synchronously - Topic: {}, Key: {}, Offset: {}",
                    topic, key, result.getRecordMetadata().offset());

            return result;

        } catch (Exception e) {
            log.error("❌ Failed to send message synchronously - Topic: {}, Key: {}, Error: {}",
                    topic, key, e.getMessage());
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }

    /**
     * 배치 메시지 발송
     */
    public CompletableFuture<Void> sendBatchMessages(String topic, List<? extends ProcessingBaseMessage> messages) {
        log.info("🚀 Sending batch of {} messages to topic: {}", messages.size(), topic);

        List<CompletableFuture<SendResult<String, Object>>> futures = messages.stream()
                .map(message -> sendMessage(topic, message))
                .toList();

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        return allFutures.whenComplete((result, throwable) -> {
            if (throwable == null) {
                log.info("✅ Batch messages sent successfully to topic: {} ({} messages)", topic, messages.size());
            } else {
                log.error("❌ Failed to send batch messages to topic: {} - Error: {}", topic, throwable.getMessage());
            }
        });
    }

    /**
     * 파티션 지정 메시지 발송
     */
    public CompletableFuture<SendResult<String, Object>> sendToPartition(
            String topic, int partition, String key, Object message) {

        log.debug("🚀 Sending message to topic: {}, partition: {}, key: {}", topic, partition, key);

        return kafkaTemplate.send(topic, partition, key, message)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        log.debug("✅ Message sent to partition {} - Offset: {}",
                                partition, result.getRecordMetadata().offset());
                    } else {
                        log.error("❌ Failed to send message to partition {} - Error: {}",
                                partition, throwable.getMessage());
                    }
                });
    }

    /**
     * 전송 성공 처리
     */
    private void handleSendSuccess(String topic, String key, Object message, SendResult<String, Object> result) {
        long offset = result.getRecordMetadata().offset();
        int partition = result.getRecordMetadata().partition();

        log.debug("✅ Message sent successfully - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                topic, partition, offset, key);

        // 메시지 상태 업데이트는 도메인 레벨에서 처리
    }

    /**
     * 전송 실패 처리
     */
    private void handleSendFailure(String topic, String key, Object message, Throwable throwable) {
        log.error("❌ Failed to send message - Topic: {}, Key: {}, Error: {}",
                topic, key, throwable.getMessage());

        // 메시지 상태 업데이트는 도메인 레벨에서 처리
    }

    /**
     * 재시도 메시지 발송
     */
    public CompletableFuture<SendResult<String, Object>> sendRetryMessage(String retryTopic,
                                                                          ProcessingBaseMessage message) {
        log.warn("🔄 Sending retry message - Topic: {}, CollectionId: {}",
                retryTopic, message.getCollectionId());

        return sendMessageWithHeaders(retryTopic, message.getCollectionId(), message, Map.of("retry", true));
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