package com.realtime.collector.infrastructure.messaging;

import com.realtime.common.kafka.message.CollectErrorMessage;
import com.realtime.common.kafka.message.CollectMessage;
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
public class CollectorEventPublisher {

    private final KafkaMessageProducer producer;

    public CompletableFuture<SendResult<String, Object>> publishCollected(String source, String topic,
                                                                          String collectionId, String minioUrl,
                                                                          int recordCount) {
        CollectMessage event = CollectMessage.builder()
                .collectionId(collectionId)
                .source(source)
                .rawDataUrl(minioUrl)
                .recordCount(recordCount)
                .occurredAt(Instant.now())
                .schemaVersion("1")
                .build();

        return producer.sendMessage(topic, collectionId, event);
    }

    public CompletableFuture<SendResult<String, Object>> publishCollectError(String source, String topic,
                                                                             String collectionId, String rawDataUrl,
                                                                             String errorCode, String message,
                                                                             boolean retriable) {
        CollectErrorMessage error = CollectErrorMessage.builder()
                .collectionId(collectionId)
                .source(source)
                .rawDataUrl(rawDataUrl)
                .errorCode(errorCode)
                .errorMessage(message)
                .retriable(retriable)
                .occurredAt(Instant.now())
                .schemaVersion("1")
                .build();

        return producer.sendMessage(topic, collectionId, error);
    }
}


